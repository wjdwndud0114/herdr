use std::collections::HashMap;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossterm::event::{MouseButton, MouseEventKind};
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use ratatui::backend::TestBackend;
use ratatui::layout::Rect;

use super::{ClientError, ClientLoopEvent};
use crate::api::client::{ApiClient, ConnectionTarget};
use crate::api::schema::{
    AgentStatus, AgentTarget, EmptyParams, Method, Request, ResponseResult, SessionSnapshot,
    WorkspaceTarget,
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
    sidebar_width: u16,
    hits: Vec<Option<SidebarHit>>,
    blit_encoder: crate::protocol::render_ansi::BlitEncoder,
    repaint_pending: bool,
    sound_config: crate::config::SoundConfig,
    visual: FleetVisualConfig,
}

struct FleetVisualConfig {
    palette: crate::app::state::Palette,
    theme_name: String,
    theme_runtime: crate::app::state::ThemeRuntimeConfig,
    status_indicators: crate::config::StatusIndicatorStyle,
    agent_panel_sort: crate::app::state::AgentPanelSort,
    sidebar_agents: crate::config::AgentsSidebarConfig,
    sidebar_spaces: crate::config::SpacesSidebarConfig,
    mouse_capture: bool,
}

pub(super) fn run() -> io::Result<()> {
    super::init_logging();
    let loaded = crate::config::Config::load();
    crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout())?;
    let mouse_capture = loaded.config.ui.mouse_capture;
    let sidebar_width = loaded.config.ui.sidebar_width.clamp(
        loaded.config.ui.sidebar_min_width,
        loaded.config.ui.sidebar_max_width,
    );
    let theme_runtime = crate::app::theme_runtime_config(&loaded.config, true);
    let (palette, theme_name) = crate::app::resolve_effective_theme(&theme_runtime, None);
    let visual = FleetVisualConfig {
        palette,
        theme_name,
        theme_runtime,
        status_indicators: loaded.config.ui.status_indicators,
        agent_panel_sort: crate::app::agent_panel_sort_from_config(
            loaded.config.ui.agent_panel_sort,
        ),
        sidebar_agents: loaded.config.ui.sidebar.agents.clone(),
        sidebar_spaces: loaded.config.ui.sidebar.spaces.clone(),
        mouse_capture,
    };
    let (cols, rows, _, _) = super::initial_terminal_geometry(false);
    let content_cols = cols.saturating_sub(sidebar_width).max(2);
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
        loaded.config.ui.sound,
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

    let mut state = FleetState {
        instances,
        active: 0,
        sidebar_width,
        hits: Vec::new(),
        blit_encoder: crate::protocol::render_ansi::BlitEncoder::new(),
        repaint_pending: true,
        sound_config,
        visual,
    };
    let mut current_cols = cols;
    let mut current_rows = rows;
    render(&mut state, current_cols, current_rows);

    while !should_quit.load(Ordering::Acquire) {
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
                    handle_fleet_event(&mut state, event);
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

fn handle_fleet_event(state: &mut FleetState, event: FleetEvent) {
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
        return;
    };

    match event {
        FleetEvent::ServerMessage { message, .. } => match message {
            ServerMessage::Frame(frame) => state.instances[index].frame = Some(frame),
            ServerMessage::Notify {
                kind,
                message,
                body,
            } => {
                super::handle_notify(kind, &message, body.as_deref(), &state.sound_config);
            }
            ServerMessage::Clipboard { data } if index == state.active => {
                super::forward_clipboard(&data);
            }
            ServerMessage::WindowTitle { title } if index == state.active => {
                super::write_window_title(title.as_deref());
            }
            ServerMessage::ServerShutdown { reason } => {
                state.instances[index].error =
                    Some(reason.unwrap_or_else(|| "server shut down".to_string()));
                state.instances[index].writer = None;
                state.instances[index].frame = None;
            }
            _ => {}
        },
        FleetEvent::Disconnected { .. } => {
            state.instances[index].error = Some("disconnected".to_string());
            state.instances[index].writer = None;
            state.instances[index].frame = None;
        }
        FleetEvent::Snapshot { result, .. } => match result {
            Ok(snapshot) => {
                state.instances[index].snapshot = Some(snapshot);
                state.instances[index].error = None;
            }
            Err(error) => state.instances[index].error = Some(error),
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
            let events = crate::raw_input::parse_raw_input_bytes_sync(&data);
            if let [crate::raw_input::RawInputEvent::Mouse(mouse)] = events.as_slice() {
                if mouse.column < state.sidebar_width {
                    if matches!(mouse.kind, MouseEventKind::Down(MouseButton::Left)) {
                        activate_sidebar_row(state, mouse.row as usize);
                        state.repaint_pending = true;
                        render(state, current_cols, current_rows);
                    }
                    return Ok(());
                }
                if let Some(kind) = mouse_kind(mouse.kind) {
                    let event = ClientInputEvent::Mouse {
                        kind,
                        column: mouse.column.saturating_sub(state.sidebar_width),
                        row: mouse.row,
                        modifiers: mouse.modifiers.bits(),
                    };
                    return write_active(
                        state,
                        ClientMessage::InputEvents {
                            events: vec![event],
                        },
                    );
                }
            }
            write_active(state, ClientMessage::Input { data })
        }
        ClientLoopEvent::Resize(cols, rows, _, _) => {
            let content_cols = cols.saturating_sub(state.sidebar_width).max(2);
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
            state.repaint_pending = true;
            render(state, cols, rows);
            Ok(())
        }
        _ => Ok(()),
    }
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

fn activate_sidebar_row(state: &mut FleetState, row: usize) {
    let Some(Some(hit)) = state.hits.get(row).cloned() else {
        return;
    };
    let (index, method) = match hit {
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
    if let (Some(method), Some(target)) = (method, state.instances[index].api_target.clone()) {
        std::thread::spawn(move || {
            let _ = ApiClient::for_target(target).request(Request {
                id: "fleet-focus".to_string(),
                method,
            });
        });
    }
}

fn render(state: &mut FleetState, cols: u16, rows: u16) {
    let sidebar_width = state.sidebar_width.min(cols.saturating_sub(2));
    let (sidebar, hits) = render_sidebar(state, sidebar_width, rows);
    state.hits = hits;
    let frame = compose_frame(
        sidebar,
        state.instances[state.active].frame.as_ref(),
        cols,
        rows,
        sidebar_width,
    );
    let encoded = state.blit_encoder.encode(&frame, state.repaint_pending);
    let mut stdout = io::stdout();
    let _ = stdout.write_all(&encoded.bytes);
    let _ = stdout.flush();
    state.blit_encoder.commit(frame, encoded);
    state.repaint_pending = false;
}

fn render_sidebar(
    state: &FleetState,
    width: u16,
    height: u16,
) -> (FrameData, Vec<Option<SidebarHit>>) {
    if width == 0 || height == 0 {
        return (
            FrameData {
                cells: Vec::new(),
                width,
                height,
                cursor: None,
                hyperlinks: Vec::new(),
                graphics: Vec::new(),
            },
            vec![None; height as usize],
        );
    }

    let (app, workspace_routes, agent_routes) = build_sidebar_state(state, width, height);
    let backend = TestBackend::new(width, height);
    let mut terminal = ratatui::Terminal::new(backend)
        .expect("fleet sidebar TestBackend construction should not fail");
    terminal
        .draw(|frame| crate::ui::render_client_sidebar(&app, frame))
        .expect("fleet sidebar render should not fail");

    let mut hits = vec![None; height as usize];
    for card in &app.view.workspace_card_areas {
        let Some(hit) = workspace_routes.get(&card.ws_idx) else {
            continue;
        };
        for row in card.rect.y..card.rect.y.saturating_add(card.rect.height).min(height) {
            hits[row as usize] = Some(hit.clone());
        }
    }

    let (_, detail_area) =
        crate::ui::expanded_sidebar_sections(app.view.sidebar_rect, app.sidebar_section_split);
    let metrics = crate::ui::agent_panel_scroll_metrics(&app, detail_area);
    let body =
        crate::ui::agent_panel_body_rect(detail_area, crate::ui::should_show_scrollbar(metrics));
    let entries = crate::ui::agent_panel_entries(&app);
    let scroll = app.agent_panel_scroll.min(metrics.max_offset_from_bottom);
    let mut row_y = body.y;
    let body_bottom = body.y.saturating_add(body.height);
    for (entry_idx, entry) in entries.iter().enumerate().skip(scroll) {
        let entry_height = crate::ui::agent_entry_height_in_body(&app, entry, body.height);
        if row_y.saturating_add(entry_height) > body_bottom {
            break;
        }
        if let Some(hit) = agent_routes.get(&(entry.ws_idx, entry.pane_id)) {
            for row in row_y..row_y.saturating_add(entry_height).min(height) {
                hits[row as usize] = Some(hit.clone());
            }
        }
        row_y = row_y
            .saturating_add(entry_height)
            .saturating_add(crate::ui::agent_entry_gap(&app, entry_idx, entries.len()))
            .min(body_bottom);
    }

    let buffer = terminal.backend().buffer().clone();
    (
        FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, None, &[]),
        hits,
    )
}

fn build_sidebar_state(
    state: &FleetState,
    width: u16,
    height: u16,
) -> (
    crate::app::AppState,
    HashMap<usize, SidebarHit>,
    HashMap<(usize, crate::layout::PaneId), SidebarHit>,
) {
    let mut app = crate::app::AppState::empty_for_client_rendering();
    app.sidebar_width = width;
    app.default_sidebar_width = state.sidebar_width;
    app.sidebar_min_width = width;
    app.sidebar_max_width = width;
    app.sidebar_section_split = 0.5;
    app.sidebar_collapsed = false;
    app.status_indicators = state.visual.status_indicators;
    app.agent_panel_sort = state.visual.agent_panel_sort;
    app.sidebar_agents = state.visual.sidebar_agents.clone();
    app.sidebar_spaces = state.visual.sidebar_spaces.clone();
    app.mouse_capture = state.visual.mouse_capture;
    app.palette = state.visual.palette.clone();
    app.theme_name = state.visual.theme_name.clone();
    app.theme_runtime = state.visual.theme_runtime.clone();
    app.mode = crate::app::Mode::Terminal;
    app.view.layout = crate::app::state::ViewLayout::Desktop;
    app.view.sidebar_rect = Rect::new(0, 0, width, height);
    app.view.terminal_area = Rect::new(width, 0, 0, height);

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
    app.view.workspace_card_areas =
        crate::ui::compute_workspace_card_areas(&app, app.view.sidebar_rect);

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
    sidebar: FrameData,
    content: Option<&FrameData>,
    width: u16,
    height: u16,
    sidebar_width: u16,
) -> FrameData {
    let blank = sidebar.cells.first().cloned().unwrap_or(CellData {
        symbol: " ".to_string(),
        fg: 0,
        bg: 0,
        modifier: 0,
        skip: false,
        hyperlink: None,
    });
    let mut cells = vec![blank; width as usize * height as usize];
    for row in 0..height.min(sidebar.height) {
        for col in 0..sidebar_width.min(sidebar.width) {
            cells[row as usize * width as usize + col as usize] =
                sidebar.cells[row as usize * sidebar.width as usize + col as usize].clone();
        }
    }

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
        graphics: Vec::new(),
    }
}
