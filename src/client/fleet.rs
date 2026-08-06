use std::collections::HashMap;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;

use super::{ClientError, ClientLoopEvent};
use crate::api::client::{ApiClient, ConnectionTarget};
use crate::api::schema::{
    AgentStatus, AgentTarget, EmptyParams, Method, Request, ResponseResult, SessionSnapshot,
    WorkspaceCreateParams, WorkspaceRenameParams, WorkspaceTarget,
};
use crate::ipc::LocalStream;
use crate::protocol::{
    self, AppSurface, CellData, ClientInputEvent, ClientMessage, ClientMouseButton,
    ClientMouseKind, FrameData, RenderEncoding, ServerMessage, MAX_FRAME_SIZE,
};

const LOCAL_INSTANCE_ID: &str = "local";
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(750);

pub(super) fn is_enabled() -> bool {
    crate::fleet::load().enabled_instances().next().is_some()
}

struct Instance {
    id: String,
    name: String,
    writer: Option<LocalStream>,
    api_target: Option<ConnectionTarget>,
    snapshot: Option<SessionSnapshot>,
    frame: Option<FrameData>,
    error: Option<String>,
}

enum FleetEvent {
    ServerMessage {
        instance_id: String,
        message: ServerMessage,
    },
    Disconnected {
        instance_id: String,
    },
    Snapshot {
        instance_id: String,
        result: Result<SessionSnapshot, String>,
    },
}

#[derive(Clone)]
enum SidebarHit {
    Instance { index: usize },
    Workspace { index: usize, workspace_id: String },
    Agent { index: usize, target: String },
}

struct FleetState {
    instances: Vec<Instance>,
    active: usize,
    sidebar: crate::app::AppState,
    workspace_routes: HashMap<usize, SidebarHit>,
    agent_routes: HashMap<(usize, crate::layout::PaneId), SidebarHit>,
    blit_encoder: crate::protocol::render_ansi::BlitEncoder,
    repaint_pending: bool,
    quit_requested: bool,
    sound_config: crate::config::SoundConfig,
    visual: FleetVisualConfig,
}

struct FleetVisualConfig {
    sidebar_width: u16,
    palette: crate::app::state::Palette,
    theme_name: String,
    theme_runtime: crate::app::state::ThemeRuntimeConfig,
    status_indicators: crate::config::StatusIndicatorStyle,
    agent_panel_sort: crate::app::state::AgentPanelSort,
    sidebar_agents: crate::config::AgentsSidebarConfig,
    sidebar_spaces: crate::config::SpacesSidebarConfig,
    sidebar_min_width: u16,
    sidebar_max_width: u16,
    sidebar_start_collapsed: bool,
    sidebar_collapsed_mode: crate::config::SidebarCollapsedModeConfig,
    confirm_close: bool,
    show_agent_labels_on_pane_borders: bool,
    sound: crate::config::SoundConfig,
    toast: crate::config::ToastConfig,
    keybinds: crate::config::Keybinds,
    prefix_code: crossterm::event::KeyCode,
    prefix_mods: crossterm::event::KeyModifiers,
    mouse_capture: bool,
}

fn fleet_visual_config(config: &crate::config::Config) -> FleetVisualConfig {
    let theme_runtime = crate::app::theme_runtime_config(config, true);
    let (palette, theme_name) = crate::app::resolve_effective_theme(&theme_runtime, None);
    let (prefix_code, prefix_mods) = config.prefix_key();
    let (sidebar_min_width, sidebar_max_width) = crate::config::validated_sidebar_bounds(
        config.ui.sidebar_min_width,
        config.ui.sidebar_max_width,
    )
    .unwrap_or((18, 36));
    FleetVisualConfig {
        sidebar_width: config
            .ui
            .sidebar_width
            .clamp(sidebar_min_width, sidebar_max_width),
        palette,
        theme_name,
        theme_runtime,
        status_indicators: config.ui.status_indicators,
        agent_panel_sort: crate::app::agent_panel_sort_from_config(config.ui.agent_panel_sort),
        sidebar_agents: config.ui.sidebar.agents.clone(),
        sidebar_spaces: config.ui.sidebar.spaces.clone(),
        sidebar_min_width,
        sidebar_max_width,
        sidebar_start_collapsed: config.ui.sidebar_start_collapsed,
        sidebar_collapsed_mode: config.ui.sidebar_collapsed_mode,
        confirm_close: config.ui.confirm_close,
        show_agent_labels_on_pane_borders: config.ui.show_agent_labels_on_pane_borders,
        sound: config.ui.sound.clone(),
        toast: config.ui.toast.clone(),
        keybinds: config.keybinds(),
        prefix_code,
        prefix_mods,
        mouse_capture: config.ui.mouse_capture,
    }
}

pub(super) fn run() -> io::Result<()> {
    super::init_logging();
    let loaded = crate::config::Config::load();
    crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout())?;
    let visual = fleet_visual_config(&loaded.config);
    let mouse_capture = visual.mouse_capture;
    let sidebar_width = visual.sidebar_width;
    let (cols, rows, _, _) = super::initial_terminal_geometry(false);
    let initial_sidebar_width = if visual.sidebar_start_collapsed {
        match visual.sidebar_collapsed_mode {
            crate::config::SidebarCollapsedModeConfig::Compact => 4,
            crate::config::SidebarCollapsedModeConfig::Hidden => 0,
        }
    } else {
        sidebar_width
    }
    .min(cols.saturating_sub(2));
    let content_cols = cols.saturating_sub(initial_sidebar_width).max(2);
    let registry = crate::fleet::load();

    let mut bridges = Vec::new();
    let mut instances = Vec::new();
    instances.push(Instance {
        id: LOCAL_INSTANCE_ID.to_string(),
        name: "local".to_string(),
        writer: None,
        api_target: Some(ConnectionTarget::LocalSession(crate::session::active_name())),
        snapshot: None,
        frame: None,
        error: None,
    });

    for definition in registry.enabled_instances() {
        let mut instance = Instance {
            id: definition.id.as_str().to_string(),
            name: definition.name.clone(),
            writer: None,
            api_target: None,
            snapshot: None,
            frame: None,
            error: None,
        };
        match crate::remote::start_fleet_remote_bridge(
            &definition.target,
            definition.session.as_deref(),
            definition.id.as_str(),
        ) {
            Ok(bridge) => {
                let client_socket = bridge.client_socket().to_path_buf();
                let api_socket = bridge.api_socket().to_path_buf();
                instance.api_target = Some(ConnectionTarget::SocketPath(api_socket));
                instance.writer = connect_app(&client_socket, content_cols, rows).ok();
                if instance.writer.is_none() {
                    instance.error = Some("app connection failed".to_string());
                }
                bridges.push(bridge);
            }
            Err(err) => instance.error = Some(err.to_string()),
        }
        instances.push(instance);
    }

    let local_socket = crate::server::socket_paths::client_socket_path();
    match connect_app(&local_socket, content_cols, rows) {
        Ok(stream) => instances[0].writer = Some(stream),
        Err(err) => return Err(io::Error::other(err.to_string())),
    }

    let terminal_guard = super::setup_terminal(mouse_capture)?;
    let should_quit = Arc::new(AtomicBool::new(false));
    let quit_flag = should_quit.clone();
    let _ = ctrlc::set_handler(move || quit_flag.store(true, Ordering::Release));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    let result = runtime.block_on(run_loop(
        instances,
        cols,
        rows,
        sidebar_width,
        mouse_capture,
        should_quit,
        visual.sound.clone(),
        visual,
    ));

    drop(terminal_guard);
    drop(bridges);
    runtime.shutdown_timeout(Duration::from_millis(100));
    crate::logging::shutdown("client");
    result.map_err(|err| io::Error::other(err.to_string()))
}

fn connect_app(path: &PathBuf, cols: u16, rows: u16) -> Result<LocalStream, ClientError> {
    let mut stream =
        crate::ipc::connect_local_stream(path).map_err(ClientError::ConnectionFailed)?;
    super::do_handshake(
        &mut stream,
        cols,
        rows,
        0,
        0,
        RenderEncoding::SemanticFrame,
        false,
    )?;
    super::write_to_server(
        &mut stream,
        &ClientMessage::SetAppSurface {
            surface: AppSurface::Content,
        },
    )
    .map_err(ClientError::ConnectionFailed)?;
    Ok(stream)
}

async fn run_loop(
    instances: Vec<Instance>,
    cols: u16,
    rows: u16,
    sidebar_width: u16,
    mouse_capture: bool,
    should_quit: Arc<AtomicBool>,
    sound_config: crate::config::SoundConfig,
    visual: FleetVisualConfig,
) -> Result<(), ClientError> {
    let (input_tx, mut input_rx) = tokio::sync::mpsc::channel::<ClientLoopEvent>(256);
    let (fleet_tx, mut fleet_rx) = tokio::sync::mpsc::channel::<FleetEvent>(256);
    let host_mouse_capture = Arc::new(AtomicBool::new(mouse_capture));

    let stdin_quit = should_quit.clone();
    let stdin_capture = host_mouse_capture.clone();
    let stdin_tx = input_tx.clone();
    std::thread::spawn(move || {
        super::input::stdin_reader_loop(stdin_tx, &stdin_quit, false, false, stdin_capture);
    });

    let resize_quit = should_quit.clone();
    let resize_tx = input_tx.clone();
    let reported_cell_size = Arc::new(std::sync::atomic::AtomicU64::new(0));
    std::thread::spawn(move || {
        super::resize_poll_loop(
            resize_tx,
            cols,
            rows,
            0,
            0,
            false,
            &reported_cell_size,
            &resize_quit,
        );
    });

    for instance in &instances {
        if let Some(writer) = &instance.writer {
            let read_stream = writer.try_clone().map_err(ClientError::ConnectionFailed)?;
            spawn_server_reader(
                instance.id.clone(),
                read_stream,
                fleet_tx.clone(),
                should_quit.clone(),
            );
        }
        if let Some(target) = instance.api_target.clone() {
            spawn_snapshot_poller(
                instance.id.clone(),
                target,
                fleet_tx.clone(),
                should_quit.clone(),
            );
        }
    }

    let mut sidebar = crate::app::AppState::empty_for_client_rendering();
    configure_sidebar_state(&mut sidebar, &visual, sidebar_width, cols, rows);
    let mut state = FleetState {
        instances,
        active: 0,
        sidebar,
        workspace_routes: HashMap::new(),
        agent_routes: HashMap::new(),
        blit_encoder: crate::protocol::render_ansi::BlitEncoder::new(),
        repaint_pending: true,
        quit_requested: false,
        sound_config,
        visual,
    };
    let mut current_cols = cols;
    let mut current_rows = rows;
    sync_sidebar_model(&mut state, current_cols, current_rows);
    render(&mut state, current_cols, current_rows);

    while !should_quit.load(Ordering::Acquire) && !state.quit_requested {
        tokio::select! {
            input = input_rx.recv() => {
                if let Some(input) = input {
                    if let ClientLoopEvent::Resize(cols, rows, _, _) = &input {
                        current_cols = *cols;
                        current_rows = *rows;
                    }
                    handle_input(&mut state, input, current_cols, current_rows)?;
                }
            }
            event = fleet_rx.recv() => {
                if let Some(event) = event {
                    if handle_fleet_event(&mut state, event) {
                        sync_sidebar_model(&mut state, current_cols, current_rows);
                    }
                    render(&mut state, current_cols, current_rows);
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    for instance in &mut state.instances {
        if let Some(writer) = &mut instance.writer {
            let _ = super::write_to_server(writer, &ClientMessage::Detach);
        }
    }
    Ok(())
}

fn spawn_server_reader(
    instance_id: String,
    mut stream: LocalStream,
    event_tx: tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let _ = stream.set_nonblocking(false);
        while !should_quit.load(Ordering::Acquire) {
            match protocol::read_message(&mut stream, MAX_FRAME_SIZE) {
                Ok(message) => {
                    if event_tx
                        .blocking_send(FleetEvent::ServerMessage {
                            instance_id: instance_id.clone(),
                            message,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => {
                    let _ = event_tx.blocking_send(FleetEvent::Disconnected { instance_id });
                    return;
                }
            }
        }
    });
}

fn spawn_snapshot_poller(
    instance_id: String,
    target: ConnectionTarget,
    event_tx: tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let client = ApiClient::for_target(target);
        while !should_quit.load(Ordering::Acquire) {
            let result = client
                .request(Request {
                    id: format!("fleet-snapshot:{instance_id}"),
                    method: Method::SessionSnapshot(EmptyParams::default()),
                })
                .and_then(|response| match response.result {
                    ResponseResult::SessionSnapshot { snapshot } => Ok(*snapshot),
                    result => Err(crate::api::client::ApiClientError::UnexpectedResult(
                        format!("{result:?}"),
                    )),
                })
                .map_err(|err| err.to_string());
            if event_tx
                .blocking_send(FleetEvent::Snapshot {
                    instance_id: instance_id.clone(),
                    result,
                })
                .is_err()
            {
                return;
            }
            std::thread::sleep(SNAPSHOT_INTERVAL);
        }
    });
}

fn handle_fleet_event(state: &mut FleetState, event: FleetEvent) -> bool {
    let instance_id = match &event {
        FleetEvent::ServerMessage { instance_id, .. }
        | FleetEvent::Disconnected { instance_id }
        | FleetEvent::Snapshot { instance_id, .. } => instance_id,
    };
    let Some(index) = state
        .instances
        .iter()
        .position(|instance| &instance.id == instance_id)
    else {
        return false;
    };

    match event {
        FleetEvent::ServerMessage { message, .. } => match message {
            ServerMessage::Frame(frame) => {
                state.instances[index].frame = Some(frame);
                false
            }
            ServerMessage::Notify {
                kind,
                message,
                body,
            } => {
                super::handle_notify(kind, &message, body.as_deref(), &state.sound_config);
                false
            }
            ServerMessage::Clipboard { data } if index == state.active => {
                super::forward_clipboard(&data);
                false
            }
            ServerMessage::WindowTitle { title } if index == state.active => {
                super::write_window_title(title.as_deref());
                false
            }
            ServerMessage::ServerShutdown { reason } => {
                state.instances[index].error =
                    Some(reason.unwrap_or_else(|| "server shut down".to_string()));
                state.instances[index].writer = None;
                state.instances[index].frame = None;
                true
            }
            _ => false,
        },
        FleetEvent::Disconnected { .. } => {
            state.instances[index].error = Some("disconnected".to_string());
            state.instances[index].writer = None;
            state.instances[index].frame = None;
            true
        }
        FleetEvent::Snapshot { result, .. } => match result {
            Ok(snapshot) => {
                let changed = state.instances[index].snapshot.as_ref() != Some(&snapshot);
                state.instances[index].snapshot = Some(snapshot);
                state.instances[index].error = None;
                changed
            }
            Err(error) => {
                let changed = state.instances[index].error.as_deref() != Some(error.as_str());
                state.instances[index].error = Some(error);
                changed
            }
        },
    }
}

fn handle_input(
    state: &mut FleetState,
    input: ClientLoopEvent,
    current_cols: u16,
    current_rows: u16,
) -> Result<(), ClientError> {
    match input {
        ClientLoopEvent::StdinInput(data) => {
            route_raw_input(state, data, current_cols, current_rows)
        }
        ClientLoopEvent::Resize(cols, rows, _, _) => {
            update_sidebar_geometry(state, cols, rows);
            resize_content_streams(state, cols, rows)?;
            state.repaint_pending = true;
            render(state, cols, rows);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn route_raw_input(
    state: &mut FleetState,
    data: Vec<u8>,
    current_cols: u16,
    current_rows: u16,
) -> Result<(), ClientError> {
    let events = crate::raw_input::parse_raw_input_bytes_with_ranges(&data);
    if events.is_empty() {
        return write_active(state, ClientMessage::Input { data });
    }

    let previous_width = current_sidebar_width(state, current_cols);
    let mut forward = Vec::new();
    let mut offset = 0usize;
    let mut shell_changed = false;

    for ranged in events {
        if ranged.start > offset {
            forward.extend_from_slice(&data[offset..ranged.start]);
        }
        let end = ranged.start.saturating_add(ranged.len).min(data.len());
        let raw = &data[ranged.start.min(data.len())..end];
        offset = offset.max(end);

        match ranged.event {
            crate::raw_input::RawInputEvent::Mouse(mouse) => {
                flush_active_input(state, &mut forward)?;
                shell_changed |= handle_mouse_input(state, mouse, current_cols, current_rows)?
                    == MouseInputRoute::Sidebar;
            }
            crate::raw_input::RawInputEvent::Key(key) => {
                match crate::app::handle_client_sidebar_key(&mut state.sidebar, key) {
                    crate::app::ClientSidebarInput::Forward => forward.extend_from_slice(raw),
                    crate::app::ClientSidebarInput::CancelServerPrefix(action) => {
                        flush_active_input(state, &mut forward)?;
                        cancel_active_server_prefix(state)?;
                        if let Some(action) = action {
                            dispatch_sidebar_action(state, action);
                        }
                        shell_changed = true;
                    }
                    crate::app::ClientSidebarInput::Consumed(action) => {
                        if let Some(action) = action {
                            dispatch_sidebar_action(state, action);
                        }
                        shell_changed = true;
                    }
                }
            }
            crate::raw_input::RawInputEvent::Text(text) => {
                if crate::app::handle_client_sidebar_text(&mut state.sidebar, text.as_str()) {
                    shell_changed = true;
                } else {
                    forward.extend_from_slice(raw);
                }
            }
            crate::raw_input::RawInputEvent::Paste(text) => {
                if crate::app::handle_client_sidebar_text(&mut state.sidebar, &text) {
                    shell_changed = true;
                } else {
                    forward.extend_from_slice(raw);
                }
            }
            _ => forward.extend_from_slice(raw),
        }
    }

    if offset < data.len() {
        forward.extend_from_slice(&data[offset..]);
    }
    flush_active_input(state, &mut forward)?;

    if shell_changed {
        update_sidebar_geometry(state, current_cols, current_rows);
        if current_sidebar_width(state, current_cols) != previous_width {
            resize_content_streams(state, current_cols, current_rows)?;
        }
        state.repaint_pending = true;
        render(state, current_cols, current_rows);
    }
    Ok(())
}

fn flush_active_input(state: &mut FleetState, data: &mut Vec<u8>) -> Result<(), ClientError> {
    if data.is_empty() {
        return Ok(());
    }
    write_active(
        state,
        ClientMessage::Input {
            data: std::mem::take(data),
        },
    )
}

fn cancel_active_server_prefix(state: &mut FleetState) -> Result<(), ClientError> {
    write_active(state, ClientMessage::Input { data: vec![0x1b] })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MouseInputRoute {
    Sidebar,
    Content,
}

fn handle_mouse_input(
    state: &mut FleetState,
    mouse: MouseEvent,
    current_cols: u16,
    current_rows: u16,
) -> Result<MouseInputRoute, ClientError> {
    let sidebar_width = current_sidebar_width(state, current_cols);
    if state.sidebar.mode == crate::app::Mode::Prefix {
        cancel_active_server_prefix(state)?;
        state.sidebar.mode = crate::app::Mode::Terminal;
    }
    let sidebar_related = state.sidebar.mode != crate::app::Mode::Terminal
        || mouse.column < sidebar_width
        || state.sidebar.drag.is_some();
    if sidebar_related {
        if let Some(action) = crate::app::handle_client_sidebar_mouse(&mut state.sidebar, mouse) {
            dispatch_sidebar_action(state, action);
        }
        update_sidebar_geometry(state, current_cols, current_rows);
        if current_sidebar_width(state, current_cols) != sidebar_width {
            resize_content_streams(state, current_cols, current_rows)?;
        }
        return Ok(MouseInputRoute::Sidebar);
    }

    if let Some(kind) = mouse_kind(mouse.kind) {
        let event = ClientInputEvent::Mouse {
            kind,
            column: mouse.column.saturating_sub(sidebar_width),
            row: mouse.row,
            modifiers: mouse.modifiers.bits(),
        };
        write_active(
            state,
            ClientMessage::InputEvents {
                events: vec![event],
            },
        )?;
    }
    Ok(MouseInputRoute::Content)
}

fn resize_content_streams(state: &mut FleetState, cols: u16, rows: u16) -> Result<(), ClientError> {
    let content_cols = cols
        .saturating_sub(current_sidebar_width(state, cols))
        .max(2);
    for instance in &mut state.instances {
        instance.frame = None;
        if let Some(writer) = &mut instance.writer {
            super::write_to_server(
                writer,
                &ClientMessage::Resize {
                    cols: content_cols,
                    rows,
                    cell_width_px: 0,
                    cell_height_px: 0,
                },
            )
            .map_err(ClientError::ConnectionLost)?;
        }
    }
    Ok(())
}

fn write_active(state: &mut FleetState, message: ClientMessage) -> Result<(), ClientError> {
    let Some(writer) = state.instances[state.active].writer.as_mut() else {
        return Ok(());
    };
    super::write_to_server(writer, &message).map_err(ClientError::ConnectionLost)
}

fn mouse_kind(kind: MouseEventKind) -> Option<ClientMouseKind> {
    Some(match kind {
        MouseEventKind::Down(button) => ClientMouseKind::Down(mouse_button(button)),
        MouseEventKind::Up(button) => ClientMouseKind::Up(mouse_button(button)),
        MouseEventKind::Drag(button) => ClientMouseKind::Drag(mouse_button(button)),
        MouseEventKind::Moved => ClientMouseKind::Moved,
        MouseEventKind::ScrollUp => ClientMouseKind::ScrollUp,
        MouseEventKind::ScrollDown => ClientMouseKind::ScrollDown,
        MouseEventKind::ScrollLeft => ClientMouseKind::ScrollLeft,
        MouseEventKind::ScrollRight => ClientMouseKind::ScrollRight,
    })
}

fn mouse_button(button: MouseButton) -> ClientMouseButton {
    match button {
        MouseButton::Left => ClientMouseButton::Left,
        MouseButton::Right => ClientMouseButton::Right,
        MouseButton::Middle => ClientMouseButton::Middle,
    }
}

fn update_fleet_config_file<F>(state: &mut FleetState, error_context: &str, update: F) -> bool
where
    F: FnOnce(&str) -> String,
{
    let path = crate::config::config_path();
    if let Some(parent) = path.parent() {
        if let Err(err) = std::fs::create_dir_all(parent) {
            tracing::warn!(path = %path.display(), error_context, err = %err, "fleet config write failed");
            state.sidebar.config_diagnostic =
                Some(format!("failed to save {error_context}: {err}"));
            return false;
        }
    }

    let content = std::fs::read_to_string(&path).unwrap_or_default();
    if let Err(err) = std::fs::write(&path, update(&content)) {
        tracing::warn!(path = %path.display(), error_context, err = %err, "fleet config write failed");
        state.sidebar.config_diagnostic = Some(format!("failed to save {error_context}: {err}"));
        return false;
    }

    reload_fleet_visual_config(state);
    true
}

fn install_recommended_fleet_integrations(state: &mut FleetState) {
    let targets = state
        .sidebar
        .integration_recommendations
        .iter()
        .filter(|recommendation| recommendation.needs_install())
        .map(|recommendation| recommendation.target)
        .collect::<Vec<_>>();

    state.sidebar.integration_install_messages.clear();
    if targets.is_empty() {
        state
            .sidebar
            .integration_install_messages
            .push("all detected integrations are current".to_string());
        return;
    }

    for target in targets {
        let label = crate::integration::integration_target_label(target);
        match crate::integration::install_target(target) {
            Ok(messages) => {
                state
                    .sidebar
                    .integration_install_messages
                    .push(format!("installed {label}"));
                state
                    .sidebar
                    .integration_install_messages
                    .extend(messages.into_iter().filter(|message| {
                        message.starts_with(crate::integration::INSTALL_WARNING_PREFIX)
                    }));
            }
            Err(err) => state
                .sidebar
                .integration_install_messages
                .push(format!("{label}: {err}")),
        }
    }
    state.sidebar.integration_recommendations = crate::integration::integration_recommendations();
}

fn dispatch_sidebar_action(state: &mut FleetState, action: crate::app::ClientSidebarAction) {
    match action {
        crate::app::ClientSidebarAction::Redraw => {}
        crate::app::ClientSidebarAction::NewWorkspace => request_for_active(
            state,
            "fleet-workspace-create",
            Method::WorkspaceCreate(WorkspaceCreateParams {
                cwd: None,
                focus: true,
                label: None,
                env: Default::default(),
            }),
        ),
        crate::app::ClientSidebarAction::FocusWorkspace { ws_idx } => {
            focus_sidebar_route(state, ws_idx, None)
        }
        crate::app::ClientSidebarAction::FocusPane { ws_idx, pane_id } => {
            focus_sidebar_route(state, ws_idx, Some(pane_id))
        }
        crate::app::ClientSidebarAction::RenameStarted { ws_idx } => {
            if let Some(SidebarHit::Workspace {
                index,
                workspace_id,
            }) = state.workspace_routes.get(&ws_idx)
            {
                if let Some(label) = state.instances[*index]
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| {
                        snapshot
                            .workspaces
                            .iter()
                            .find(|workspace| workspace.workspace_id == *workspace_id)
                    })
                    .map(|workspace| workspace.label.clone())
                {
                    state.sidebar.name_input = label;
                }
            }
        }
        crate::app::ClientSidebarAction::RenameWorkspace { ws_idx, label } => {
            if let Some(SidebarHit::Workspace {
                index,
                workspace_id,
            }) = state.workspace_routes.get(&ws_idx).cloned()
            {
                request_for_instance(
                    state,
                    index,
                    "fleet-workspace-rename",
                    Method::WorkspaceRename(WorkspaceRenameParams {
                        workspace_id,
                        label,
                    }),
                );
            }
        }
        crate::app::ClientSidebarAction::CloseWorkspace { ws_idx } => {
            if let Some(SidebarHit::Workspace {
                index,
                workspace_id,
            }) = state.workspace_routes.get(&ws_idx).cloned()
            {
                request_for_instance(
                    state,
                    index,
                    "fleet-workspace-close",
                    Method::WorkspaceClose(WorkspaceTarget { workspace_id }),
                );
            }
        }
        crate::app::ClientSidebarAction::SaveTheme { name } => {
            update_fleet_config_file(state, "theme", |content| {
                let content = crate::config::upsert_section_value(
                    content,
                    "theme",
                    "name",
                    &format!("\"{name}\""),
                );
                crate::config::upsert_section_bool(&content, "theme", "auto_switch", false)
            });
        }
        crate::app::ClientSidebarAction::SaveStatusIndicators { style } => {
            update_fleet_config_file(state, "status indicators", |content| {
                crate::config::upsert_section_value(
                    content,
                    "ui",
                    "status_indicators",
                    &format!("\"{}\"", style.as_str()),
                )
            });
        }
        crate::app::ClientSidebarAction::SaveSound { enabled } => {
            update_fleet_config_file(state, "sound setting", |content| {
                crate::config::upsert_section_bool(content, "ui.sound", "enabled", enabled)
            });
        }
        crate::app::ClientSidebarAction::SaveToastDelivery { delivery } => {
            let value = match delivery {
                crate::config::ToastDelivery::Off => "\"off\"",
                crate::config::ToastDelivery::Herdr => "\"herdr\"",
                crate::config::ToastDelivery::Terminal => "\"terminal\"",
                crate::config::ToastDelivery::System => "\"system\"",
            };
            update_fleet_config_file(state, "toast setting", |content| {
                let content =
                    crate::config::upsert_section_value(content, "ui.toast", "delivery", value);
                crate::config::remove_section_key(&content, "ui.toast", "enabled")
            });
        }
        crate::app::ClientSidebarAction::SaveAgentBorderLabels { enabled } => {
            update_fleet_config_file(state, "agent border labels", |content| {
                crate::config::upsert_section_bool(
                    content,
                    "ui",
                    "show_agent_labels_on_pane_borders",
                    enabled,
                )
            });
        }
        crate::app::ClientSidebarAction::InstallRecommendedIntegrations => {
            install_recommended_fleet_integrations(state);
        }
        crate::app::ClientSidebarAction::SaveAgentPanelSort { sort } => {
            let value = match sort {
                crate::app::state::AgentPanelSort::Spaces => {
                    crate::config::AgentPanelSortConfig::Spaces.as_str()
                }
                crate::app::state::AgentPanelSort::Priority => {
                    crate::config::AgentPanelSortConfig::Priority.as_str()
                }
            };
            update_fleet_config_file(state, "agent panel sort", |content| {
                crate::config::upsert_section_value(
                    content,
                    "ui",
                    "agent_panel_sort",
                    &format!("\"{value}\""),
                )
            });
        }
        crate::app::ClientSidebarAction::ReloadConfig => {
            reload_fleet_visual_config(state);
            for index in 0..state.instances.len() {
                request_for_instance(
                    state,
                    index,
                    "fleet-reload-config",
                    Method::ServerReloadConfig(EmptyParams::default()),
                );
            }
        }
        crate::app::ClientSidebarAction::Detach => state.quit_requested = true,
    }
}

fn focus_sidebar_route(
    state: &mut FleetState,
    ws_idx: usize,
    pane_id: Option<crate::layout::PaneId>,
) {
    let route = pane_id
        .and_then(|pane_id| state.agent_routes.get(&(ws_idx, pane_id)))
        .or_else(|| state.workspace_routes.get(&ws_idx))
        .cloned();
    let Some(route) = route else {
        return;
    };
    let (index, method) = match route {
        SidebarHit::Instance { index } => (index, None),
        SidebarHit::Workspace {
            index,
            workspace_id,
        } => (
            index,
            Some(Method::WorkspaceFocus(WorkspaceTarget { workspace_id })),
        ),
        SidebarHit::Agent { index, target } => {
            (index, Some(Method::AgentFocus(AgentTarget { target })))
        }
    };
    state.active = index;
    state.sidebar.active = Some(ws_idx);
    state.sidebar.selected = ws_idx;
    if let Some(method) = method {
        request_for_instance(state, index, "fleet-focus", method);
    }
}

fn request_for_active(state: &FleetState, id: &'static str, method: Method) {
    request_for_instance(state, state.active, id, method);
}

fn request_for_instance(state: &FleetState, index: usize, id: &'static str, method: Method) {
    let Some(target) = state
        .instances
        .get(index)
        .and_then(|instance| instance.api_target.clone())
    else {
        return;
    };
    std::thread::spawn(move || {
        let _ = ApiClient::for_target(target).request(Request {
            id: id.to_string(),
            method,
        });
    });
}

fn render(state: &mut FleetState, cols: u16, rows: u16) {
    if cols == 0 || rows == 0 {
        return;
    }
    update_sidebar_geometry(state, cols, rows);
    let sidebar_width = current_sidebar_width(state, cols);
    let base = compose_frame(
        state.instances[state.active].frame.as_ref(),
        cols,
        rows,
        sidebar_width,
    );
    let Some(base_buffer) = base.to_ratatui_buffer() else {
        return;
    };
    let backend = TestBackend::new(cols, rows);
    let mut terminal = ratatui::Terminal::new(backend)
        .expect("fleet shell TestBackend construction should not fail");
    terminal
        .draw(|frame| {
            frame.buffer_mut().clone_from(&base_buffer);
            crate::ui::render_client_shell(&state.sidebar, frame);
        })
        .expect("fleet shell render should not fail");
    let buffer = terminal.backend().buffer().clone();
    let cursor = matches!(
        state.sidebar.mode,
        crate::app::Mode::Terminal | crate::app::Mode::Prefix
    )
    .then(|| base.cursor.clone())
    .flatten();
    let mut frame = FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, cursor, &[]);
    for (rendered, original) in frame.cells.iter_mut().zip(&base.cells) {
        if rendered.symbol == original.symbol
            && rendered.fg == original.fg
            && rendered.bg == original.bg
            && rendered.modifier == original.modifier
        {
            rendered.hyperlink = original.hyperlink;
        }
    }
    frame.hyperlinks = base.hyperlinks;
    frame.graphics = base.graphics;
    let encoded = state.blit_encoder.encode(&frame, state.repaint_pending);
    let mut stdout = io::stdout();
    let _ = stdout.write_all(&encoded.bytes);
    let _ = stdout.flush();
    state.blit_encoder.commit(frame, encoded);
    state.repaint_pending = false;
}

fn configure_sidebar_state(
    app: &mut crate::app::AppState,
    visual: &FleetVisualConfig,
    width: u16,
    cols: u16,
    rows: u16,
) {
    app.sidebar_width = width;
    app.default_sidebar_width = width;
    app.sidebar_min_width = visual.sidebar_min_width;
    app.sidebar_max_width = visual.sidebar_max_width;
    app.sidebar_section_split = 0.5;
    app.sidebar_collapsed = visual.sidebar_start_collapsed;
    app.sidebar_collapsed_mode = visual.sidebar_collapsed_mode;
    app.status_indicators = visual.status_indicators;
    app.agent_panel_sort = visual.agent_panel_sort;
    app.sidebar_agents = visual.sidebar_agents.clone();
    app.sidebar_spaces = visual.sidebar_spaces.clone();
    app.mouse_capture = visual.mouse_capture;
    app.confirm_close = visual.confirm_close;
    app.show_agent_labels_on_pane_borders = visual.show_agent_labels_on_pane_borders;
    app.sound = visual.sound.clone();
    app.toast_config = visual.toast.clone();
    app.keybinds = visual.keybinds.clone();
    app.prefix_code = visual.prefix_code;
    app.prefix_mods = visual.prefix_mods;
    app.palette = visual.palette.clone();
    app.theme_name = visual.theme_name.clone();
    app.theme_runtime = visual.theme_runtime.clone();
    app.mode = crate::app::Mode::Terminal;
    app.view.layout = crate::app::state::ViewLayout::Desktop;
    let sidebar_width = current_app_sidebar_width(app, cols);
    app.view.sidebar_rect = Rect::new(0, 0, sidebar_width, rows);
    app.view.terminal_area = Rect::new(sidebar_width, 0, cols.saturating_sub(sidebar_width), rows);
}

fn apply_fleet_visual_config(state: &mut FleetState, visual: FleetVisualConfig) {
    state.sound_config = visual.sound.clone();
    let app = &mut state.sidebar;
    app.default_sidebar_width = visual.sidebar_width;
    app.sidebar_min_width = visual.sidebar_min_width;
    app.sidebar_max_width = visual.sidebar_max_width;
    app.sidebar_width = app
        .sidebar_width
        .clamp(app.sidebar_min_width, app.sidebar_max_width);
    app.sidebar_collapsed_mode = visual.sidebar_collapsed_mode;
    app.status_indicators = visual.status_indicators;
    app.agent_panel_sort = visual.agent_panel_sort;
    app.sidebar_agents = visual.sidebar_agents.clone();
    app.sidebar_spaces = visual.sidebar_spaces.clone();
    app.mouse_capture = visual.mouse_capture;
    app.confirm_close = visual.confirm_close;
    app.show_agent_labels_on_pane_borders = visual.show_agent_labels_on_pane_borders;
    app.sound = visual.sound.clone();
    app.toast_config = visual.toast.clone();
    app.keybinds = visual.keybinds.clone();
    app.prefix_code = visual.prefix_code;
    app.prefix_mods = visual.prefix_mods;
    app.palette = visual.palette.clone();
    app.theme_name = visual.theme_name.clone();
    app.theme_runtime = visual.theme_runtime.clone();
    state.visual = visual;
}

fn reload_fleet_visual_config(state: &mut FleetState) {
    let loaded = crate::config::Config::load();
    state.sidebar.config_diagnostic = crate::config::config_diagnostic_summary(&loaded.diagnostics);
    apply_fleet_visual_config(state, fleet_visual_config(&loaded.config));
}

fn sync_sidebar_model(state: &mut FleetState, cols: u16, rows: u16) {
    let (mut model, workspace_routes, agent_routes) = build_sidebar_state(state, cols, rows);
    state.sidebar.terminals = std::mem::take(&mut model.terminals);
    state.sidebar.workspaces = std::mem::take(&mut model.workspaces);
    state.sidebar.active = model.active;
    if !matches!(
        state.sidebar.mode,
        crate::app::Mode::RenameWorkspace
            | crate::app::Mode::ContextMenu
            | crate::app::Mode::ConfirmClose
    ) {
        state.sidebar.selected = model.selected;
    }
    state.workspace_routes = workspace_routes;
    state.agent_routes = agent_routes;
    update_sidebar_geometry(state, cols, rows);
}

fn current_app_sidebar_width(app: &crate::app::AppState, cols: u16) -> u16 {
    let desired = if app.sidebar_collapsed {
        match app.sidebar_collapsed_mode {
            crate::config::SidebarCollapsedModeConfig::Compact => 4,
            crate::config::SidebarCollapsedModeConfig::Hidden => 0,
        }
    } else {
        app.sidebar_width
            .clamp(app.sidebar_min_width, app.sidebar_max_width)
    };
    desired.min(cols.saturating_sub(2))
}

fn current_sidebar_width(state: &FleetState, cols: u16) -> u16 {
    current_app_sidebar_width(&state.sidebar, cols)
}

fn update_sidebar_geometry(state: &mut FleetState, cols: u16, rows: u16) {
    let width = current_sidebar_width(state, cols);
    state.sidebar.view.layout = crate::app::state::ViewLayout::Desktop;
    state.sidebar.view.sidebar_rect = Rect::new(0, 0, width, rows);
    state.sidebar.view.terminal_area = Rect::new(width, 0, cols.saturating_sub(width), rows);
    if state.sidebar.sidebar_collapsed {
        state.sidebar.workspace_scroll = state
            .sidebar
            .workspace_scroll
            .min(state.sidebar.workspaces.len().saturating_sub(1));
        state.sidebar.agent_panel_scroll = 0;
        state.sidebar.view.workspace_card_areas.clear();
    } else {
        state.sidebar.workspace_scroll = crate::ui::normalized_workspace_scroll(
            &state.sidebar,
            state.sidebar.view.sidebar_rect,
            state.sidebar.workspace_scroll,
        );
        let (_, detail_area) = crate::ui::expanded_sidebar_sections(
            state.sidebar.view.sidebar_rect,
            state.sidebar.sidebar_section_split,
        );
        let max_agent_scroll = crate::ui::agent_panel_scroll_metrics(&state.sidebar, detail_area)
            .max_offset_from_bottom;
        state.sidebar.agent_panel_scroll = state.sidebar.agent_panel_scroll.min(max_agent_scroll);
        state.sidebar.view.workspace_card_areas = crate::ui::compute_workspace_card_areas(
            &state.sidebar,
            state.sidebar.view.sidebar_rect,
        );
    }
}

fn build_sidebar_state(
    state: &FleetState,
    cols: u16,
    rows: u16,
) -> (
    crate::app::AppState,
    HashMap<usize, SidebarHit>,
    HashMap<(usize, crate::layout::PaneId), SidebarHit>,
) {
    let mut app = crate::app::AppState::empty_for_client_rendering();
    configure_sidebar_state(
        &mut app,
        &state.visual,
        state.sidebar.sidebar_width,
        cols,
        rows,
    );

    let mut workspace_routes = HashMap::new();
    let mut agent_routes = HashMap::new();
    let mut active_workspace = None;

    for (instance_idx, instance) in state.instances.iter().enumerate() {
        let snapshot_workspaces = instance
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.workspaces.as_slice())
            .unwrap_or(&[]);

        if snapshot_workspaces.is_empty() {
            let status = if let Some(error) = &instance.error {
                error.as_str()
            } else if instance.writer.is_some() {
                "no spaces"
            } else {
                "connecting"
            };
            let terminal_id = crate::terminal::TerminalId::alloc();
            let (workspace, _) = crate::workspace::Workspace::sidebar_placeholder(
                format!("fleet:{}:empty", instance.id),
                format!("{} · {status}", instance.name),
                None,
                vec![(terminal_id.clone(), true)],
                None,
            );
            let ws_idx = app.workspaces.len();
            app.terminals.insert(
                terminal_id.clone(),
                crate::terminal::TerminalState::new(terminal_id, "/".into()),
            );
            app.workspaces.push(workspace);
            workspace_routes.insert(
                ws_idx,
                SidebarHit::Instance {
                    index: instance_idx,
                },
            );
            if instance_idx == state.active && active_workspace.is_none() {
                active_workspace = Some(ws_idx);
            }
            continue;
        }

        let snapshot = instance.snapshot.as_ref().expect("snapshot checked above");
        for workspace_info in snapshot_workspaces {
            let agents = snapshot
                .agents
                .iter()
                .filter(|agent| agent.workspace_id == workspace_info.workspace_id)
                .collect::<Vec<_>>();
            let mut terminals = Vec::new();
            let mut pane_terminals = Vec::new();
            let mut focused_pane_idx = None;

            for agent in &agents {
                let terminal_id = crate::terminal::TerminalId::alloc();
                let (agent_state, seen) = native_agent_state(agent.agent_status);
                let mut terminal = crate::terminal::TerminalState::new(
                    terminal_id.clone(),
                    agent
                        .foreground_cwd
                        .as_deref()
                        .or(agent.cwd.as_deref())
                        .unwrap_or("/")
                        .into(),
                );
                terminal.state = agent_state;
                terminal.last_agent_state_change_seq = Some(agent.state_change_seq);
                terminal.detected_agent = agent
                    .agent
                    .as_deref()
                    .or(agent.display_agent.as_deref())
                    .and_then(crate::detect::parse_agent_label);
                terminal.set_agent_name(agent_display_name(agent));
                terminal.set_terminal_title(agent.terminal_title.clone());
                patch_metadata_tokens(&mut terminal.metadata_tokens, &agent.tokens);
                if agent.focused {
                    focused_pane_idx = Some(pane_terminals.len());
                }
                pane_terminals.push((terminal_id.clone(), seen));
                terminals.push((terminal_id, terminal));
            }

            if pane_terminals.is_empty() {
                let terminal_id = crate::terminal::TerminalId::alloc();
                let (workspace_state, seen) = native_agent_state(workspace_info.agent_status);
                let mut terminal =
                    crate::terminal::TerminalState::new(terminal_id.clone(), "/".into());
                terminal.state = workspace_state;
                pane_terminals.push((terminal_id.clone(), seen));
                terminals.push((terminal_id, terminal));
            }

            let label = format!("{} · {}", instance.name, workspace_info.label);
            let (mut workspace, pane_ids) = crate::workspace::Workspace::sidebar_placeholder(
                format!("fleet:{}:{}", instance.id, workspace_info.workspace_id),
                label,
                workspace_info.tokens.get("branch").cloned(),
                pane_terminals,
                focused_pane_idx,
            );
            let mut workspace_tokens = workspace_info.tokens.clone();
            workspace_tokens.insert("host".to_string(), instance.name.clone());
            patch_metadata_tokens(&mut workspace.metadata_tokens, &workspace_tokens);

            let ws_idx = app.workspaces.len();
            for (terminal_id, terminal) in terminals {
                app.terminals.insert(terminal_id, terminal);
            }
            app.workspaces.push(workspace);
            workspace_routes.insert(
                ws_idx,
                SidebarHit::Workspace {
                    index: instance_idx,
                    workspace_id: workspace_info.workspace_id.clone(),
                },
            );
            for (pane_id, agent) in pane_ids.iter().zip(agents) {
                agent_routes.insert(
                    (ws_idx, *pane_id),
                    SidebarHit::Agent {
                        index: instance_idx,
                        target: agent.pane_id.clone(),
                    },
                );
            }

            if instance_idx == state.active
                && (workspace_info.focused
                    || snapshot.focused_workspace_id.as_deref()
                        == Some(workspace_info.workspace_id.as_str()))
            {
                active_workspace = Some(ws_idx);
            } else if instance_idx == state.active && active_workspace.is_none() {
                active_workspace = Some(ws_idx);
            }
        }
    }

    app.active = active_workspace.or_else(|| (!app.workspaces.is_empty()).then_some(0));
    app.selected = app.active.unwrap_or(0);
    app.workspace_scroll = crate::ui::normalized_workspace_scroll(&app, app.view.sidebar_rect, 0);
    app.view.workspace_card_areas = if app.sidebar_collapsed {
        Vec::new()
    } else {
        crate::ui::compute_workspace_card_areas(&app, app.view.sidebar_rect)
    };

    (app, workspace_routes, agent_routes)
}

fn native_agent_state(status: AgentStatus) -> (crate::detect::AgentState, bool) {
    match status {
        AgentStatus::Working => (crate::detect::AgentState::Working, true),
        AgentStatus::Blocked => (crate::detect::AgentState::Blocked, true),
        AgentStatus::Done => (crate::detect::AgentState::Idle, false),
        AgentStatus::Idle => (crate::detect::AgentState::Idle, true),
        AgentStatus::Unknown => (crate::detect::AgentState::Unknown, true),
    }
}

fn agent_display_name(agent: &crate::api::schema::AgentInfo) -> String {
    agent
        .name
        .as_deref()
        .or(agent.display_agent.as_deref())
        .or(agent.agent.as_deref())
        .or(agent.title.as_deref())
        .unwrap_or(&agent.pane_id)
        .to_string()
}

fn patch_metadata_tokens(
    target: &mut crate::metadata_tokens::MetadataTokens,
    values: &HashMap<String, String>,
) {
    target.patch(
        values
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect(),
        None,
        Instant::now(),
    );
}

fn compose_frame(
    content: Option<&FrameData>,
    width: u16,
    height: u16,
    sidebar_width: u16,
) -> FrameData {
    let blank = content
        .and_then(|frame| frame.cells.first())
        .cloned()
        .map(|mut cell| {
            cell.symbol = " ".to_string();
            cell.skip = false;
            cell.hyperlink = None;
            cell
        })
        .unwrap_or(CellData {
            symbol: " ".to_string(),
            fg: 0,
            bg: 0,
            modifier: 0,
            skip: false,
            hyperlink: None,
        });
    let mut cells = vec![blank; width as usize * height as usize];

    let mut cursor = None;
    let mut hyperlinks = Vec::new();
    if let Some(content) = content {
        let available = width.saturating_sub(sidebar_width);
        for row in 0..height.min(content.height) {
            for col in 0..available.min(content.width) {
                cells[row as usize * width as usize + sidebar_width as usize + col as usize] =
                    content.cells[row as usize * content.width as usize + col as usize].clone();
            }
        }
        cursor = content.cursor.clone().map(|mut cursor| {
            cursor.x = cursor.x.saturating_add(sidebar_width);
            cursor
        });
        hyperlinks = content.hyperlinks.clone();
    }

    FrameData {
        cells,
        width,
        height,
        cursor,
        hyperlinks,
        graphics: content
            .map(|frame| shift_graphics_columns(&frame.graphics, sidebar_width))
            .unwrap_or_default(),
    }
}

/// Kitty image placements use absolute cursor coordinates. Content-surface
/// servers encode those coordinates relative to column zero, so shift each
/// cursor-position command past the aggregate sidebar before blitting it.
fn shift_graphics_columns(graphics: &[u8], offset: u16) -> Vec<u8> {
    if graphics.is_empty() || offset == 0 {
        return graphics.to_vec();
    }
    let mut shifted = Vec::with_capacity(graphics.len());
    let mut cursor = 0usize;
    while cursor < graphics.len() {
        if graphics.get(cursor..cursor.saturating_add(2)) == Some(b"\x1b[") {
            let sequence_start = cursor;
            let mut index = cursor + 2;
            let row_start = index;
            while graphics.get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
            if index > row_start && graphics.get(index) == Some(&b';') {
                index += 1;
                let column_start = index;
                while graphics.get(index).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                }
                if index > column_start && graphics.get(index) == Some(&b'H') {
                    shifted.extend_from_slice(&graphics[sequence_start..column_start]);
                    let column = std::str::from_utf8(&graphics[column_start..index])
                        .ok()
                        .and_then(|value| value.parse::<u16>().ok())
                        .unwrap_or(1);
                    shifted.extend_from_slice(column.saturating_add(offset).to_string().as_bytes());
                    shifted.push(b'H');
                    cursor = index + 1;
                    continue;
                }
            }
        }
        shifted.push(graphics[cursor]);
        cursor += 1;
    }
    shifted
}
