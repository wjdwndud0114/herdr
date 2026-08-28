use std::collections::HashMap;
use std::io::{self, BufRead as _, BufReader, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    AgentStatus, EmptyParams, Method, PaneTarget, Request, ResponseResult, SessionSnapshot,
    TabTarget, WorkspaceCloseParams, WorkspaceCreateParams, WorkspaceRenameParams, WorkspaceTarget,
};
use crate::ipc::LocalStream;
use crate::platform::PrefixInputSource;
use crate::protocol::{
    self, AppSurface, CellData, ClientInputEvent, ClientMessage, ClientMouseButton,
    ClientMouseKind, FrameData, RenderEncoding, ServerMessage, MAX_FRAME_SIZE,
    MAX_GRAPHICS_FRAME_SIZE,
};

const LOCAL_INSTANCE_ID: &str = "local";
const RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(750);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);

pub(super) fn is_enabled() -> bool {
    crate::fleet::load().enabled_instances().next().is_some()
}

struct Instance {
    id: String,
    name: String,
    remote_target: Option<String>,
    remote_session: Option<String>,
    bridge_connecting: bool,
    content_connecting: bool,
    bridge_status: Option<crate::remote::FleetRemoteBridgeStatus>,
    writer: Option<LocalStream>,
    content_generation: u64,
    api_target: Option<ConnectionTarget>,
    api_connected: bool,
    snapshot: Option<SessionSnapshot>,
    frame: Option<FrameData>,
    error: Option<String>,
    action_error: Option<String>,
    retry_at: Instant,
    retry_delay: Duration,
    mouse_capture: bool,
    mouse_sgr_pixels: bool,
    keyboard_report_all: bool,
    prefix_input_active: bool,
    window_title: Option<String>,
    request_tx: Option<std::sync::mpsc::Sender<ApiCommand>>,
    client_socket: Option<PathBuf>,
}

struct ApiCommand {
    id: String,
    label: String,
    method: Method,
}

enum FleetEvent {
    BridgeReady {
        instance_id: String,
        result: Result<crate::remote::FleetRemoteBridge, String>,
    },
    ContentReady {
        instance_id: String,
        generation: u64,
        result: Result<LocalStream, String>,
    },
    ServerMessage {
        instance_id: String,
        generation: u64,
        message: ServerMessage,
    },
    Disconnected {
        instance_id: String,
        generation: u64,
    },
    Snapshot {
        instance_id: String,
        result: Result<SessionSnapshot, String>,
    },
    ApiResult {
        instance_id: String,
        label: String,
        result: Result<(), String>,
    },
    AgentStatusChanged {
        instance_id: String,
        event: crate::api::schema::PaneAgentStatusChangedEvent,
        previous_status: AgentStatus,
    },
}

#[derive(Clone)]
enum SidebarHit {
    Instance { index: usize },
    Workspace { index: usize, workspace_id: String },
    Tab { index: usize, tab_id: String },
    Pane { index: usize, pane_id: String },
}

struct FleetState {
    instances: Vec<Instance>,
    bridges: Vec<crate::remote::FleetRemoteBridge>,
    active: usize,
    sidebar: crate::app::AppState,
    workspace_routes: HashMap<usize, SidebarHit>,
    tab_routes: HashMap<(usize, usize), SidebarHit>,
    pane_routes: HashMap<(usize, crate::layout::PaneId), SidebarHit>,
    projected_terminal_ids: HashMap<String, crate::terminal::TerminalId>,
    projected_pane_ids: HashMap<String, crate::layout::PaneId>,
    quit_requested: bool,
    host: super::ClientState,
    host_mouse_capture: Arc<AtomicBool>,
    host_sgr_pixels: Arc<AtomicBool>,
    prefix_input_source: crate::platform::RealPrefixInputSource,
    pending_prefix_bytes: Vec<u8>,
    cell_width_px: u32,
    cell_height_px: u32,
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
    sidebar_collapsed_mode: crate::config::SidebarCollapsedModeConfig,
    confirm_close: bool,
    show_agent_labels_on_pane_borders: bool,
    sound: crate::config::SoundConfig,
    toast: crate::config::ToastConfig,
    keybinds: crate::config::Keybinds,
    prefix_code: crossterm::event::KeyCode,
    prefix_mods: crossterm::event::KeyModifiers,
    mouse_capture: bool,
    mouse_scroll_lines: usize,
    redraw_on_focus_gained: bool,
    host_cursor: crate::config::HostCursorModeConfig,
    kitty_graphics_enabled: bool,
    remote_image_paste_key: Option<(crossterm::event::KeyCode, crossterm::event::KeyModifiers)>,
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
        sidebar_collapsed_mode: config.ui.sidebar_collapsed_mode,
        confirm_close: config.ui.confirm_close,
        show_agent_labels_on_pane_borders: config.ui.show_agent_labels_on_pane_borders,
        sound: config.ui.sound.clone(),
        toast: config.ui.toast.clone(),
        keybinds: config.keybinds(),
        prefix_code,
        prefix_mods,
        mouse_capture: config.ui.mouse_capture,
        mouse_scroll_lines: config.ui.mouse_scroll_lines(),
        redraw_on_focus_gained: config.ui.redraw_on_focus_gained,
        host_cursor: config.ui.host_cursor,
        kitty_graphics_enabled: config.experimental.kitty_graphics,
        remote_image_paste_key: config.remote_image_paste_key().ok().flatten(),
    }
}

pub(super) fn run() -> io::Result<()> {
    super::init_logging();
    let loaded = crate::config::Config::load();
    crate::terminal_modes::clear_host_mouse_reporting(&mut io::stdout())?;
    let config_diagnostic = crate::config::config_diagnostic_summary(&loaded.diagnostics);
    let mut sidebar = crate::app::client_shell_state_from_config(&loaded.config, config_diagnostic);
    let visual = fleet_visual_config(&loaded.config);
    let mouse_capture = visual.mouse_capture;
    let registry = crate::fleet::load();
    if let Some(width) = registry.presentation().sidebar_width {
        sidebar.sidebar_width = width.clamp(sidebar.sidebar_min_width, sidebar.sidebar_max_width);
        sidebar.sidebar_width_source = crate::app::state::SidebarWidthSource::Persisted;
    }
    if let Some(split) = registry.presentation().sidebar_section_split {
        sidebar.sidebar_section_split = split;
    }
    sidebar.collapsed_space_keys = registry.presentation().collapsed_space_keys.clone();
    let (cols, rows, cell_width_px, cell_height_px, _exact_cell_size) =
        super::initial_terminal_geometry(visual.kitty_graphics_enabled);
    crate::ui::compute_client_shell_view(&mut sidebar, Rect::new(0, 0, cols, rows));
    let content = sidebar.view.terminal_area;
    let content_cols = content.width.max(2);
    let content_rows = content.height.max(1);
    let bridges = Vec::new();
    let mut instances = Vec::new();
    instances.push(Instance {
        id: LOCAL_INSTANCE_ID.to_string(),
        name: "local".to_string(),
        remote_target: None,
        remote_session: None,
        bridge_connecting: false,
        content_connecting: false,
        bridge_status: None,
        writer: None,
        content_generation: 0,
        api_target: Some(ConnectionTarget::LocalSession(crate::session::active_name())),
        api_connected: false,
        snapshot: None,
        frame: None,
        error: None,
        action_error: None,
        retry_at: Instant::now(),
        retry_delay: RECONNECT_INITIAL_DELAY,
        mouse_capture,
        mouse_sgr_pixels: false,
        keyboard_report_all: false,
        prefix_input_active: false,
        window_title: None,
        request_tx: None,
        client_socket: None,
    });

    for definition in registry.enabled_instances() {
        let instance = Instance {
            id: definition.id.as_str().to_string(),
            name: definition.name.clone(),
            remote_target: Some(definition.target.clone()),
            remote_session: definition.session.clone(),
            bridge_connecting: false,
            content_connecting: false,
            bridge_status: None,
            writer: None,
            content_generation: 0,
            api_target: None,
            api_connected: false,
            snapshot: None,
            frame: None,
            error: None,
            action_error: None,
            retry_at: Instant::now(),
            retry_delay: RECONNECT_INITIAL_DELAY,
            mouse_capture,
            mouse_sgr_pixels: false,
            keyboard_report_all: false,
            prefix_input_active: false,
            window_title: None,
            request_tx: None,
            client_socket: None,
        };
        instances.push(instance);
    }

    let local_socket = crate::server::socket_paths::client_socket_path();
    instances[0].client_socket = Some(local_socket.clone());
    match connect_app(
        &local_socket,
        content_cols,
        content_rows,
        cell_width_px,
        cell_height_px,
    ) {
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
        bridges,
        cols,
        rows,
        cell_width_px,
        cell_height_px,
        sidebar,
        mouse_capture,
        should_quit,
        visual,
    ));

    drop(terminal_guard);
    runtime.shutdown_timeout(Duration::from_millis(100));
    crate::logging::shutdown("client");
    result.map_err(|err| io::Error::other(err.to_string()))
}

fn connect_app(
    path: &PathBuf,
    cols: u16,
    rows: u16,
    cell_width_px: u32,
    cell_height_px: u32,
) -> Result<LocalStream, ClientError> {
    let mut stream =
        crate::ipc::connect_local_stream(path).map_err(ClientError::ConnectionFailed)?;
    super::do_handshake(
        &mut stream,
        cols,
        rows,
        cell_width_px,
        cell_height_px,
        false,
        RenderEncoding::SemanticFrame,
        false,
        AppSurface::Content,
    )?;
    Ok(stream)
}

async fn run_loop(
    instances: Vec<Instance>,
    bridges: Vec<crate::remote::FleetRemoteBridge>,
    cols: u16,
    rows: u16,
    initial_cell_width_px: u32,
    initial_cell_height_px: u32,
    sidebar: crate::app::AppState,
    mouse_capture: bool,
    should_quit: Arc<AtomicBool>,
    visual: FleetVisualConfig,
) -> Result<(), ClientError> {
    let (input_tx, mut input_rx) = tokio::sync::mpsc::channel::<ClientLoopEvent>(256);
    let (fleet_tx, mut fleet_rx) = tokio::sync::mpsc::channel::<FleetEvent>(256);
    let host_mouse_capture = Arc::new(AtomicBool::new(mouse_capture));
    let host_sgr_pixels = Arc::new(AtomicBool::new(false));

    let will_query_host_terminal_theme = super::should_query_host_terminal_theme();
    let will_query_host_cell_size =
        super::host_cell_size_query_required(visual.kitty_graphics_enabled);
    let stdin_quit = should_quit.clone();
    let stdin_capture = host_mouse_capture.clone();
    let stdin_sgr_pixels = host_sgr_pixels.clone();
    let stdin_tx = input_tx.clone();
    #[cfg(unix)]
    let direct_graphics_response = Arc::new(std::sync::Mutex::new(
        super::direct_graphics::ResponseMatcher::default(),
    ));
    #[cfg(unix)]
    let stdin_direct_response = direct_graphics_response.clone();
    #[cfg(unix)]
    let stdin_direct_response_active = stdin_direct_response
        .lock()
        .map(|matcher| matcher.active_handle())
        .unwrap_or_default();
    std::thread::spawn(move || {
        super::input::stdin_reader_loop(
            stdin_tx,
            &stdin_quit,
            will_query_host_terminal_theme,
            will_query_host_cell_size,
            stdin_capture,
            stdin_sgr_pixels,
            #[cfg(unix)]
            stdin_direct_response,
            #[cfg(unix)]
            stdin_direct_response_active,
        );
    });
    if will_query_host_terminal_theme {
        super::query_host_terminal_theme();
    }
    if will_query_host_cell_size {
        super::query_host_cell_size();
    }

    let resize_quit = should_quit.clone();
    let resize_tx = input_tx.clone();
    let reported_cell_size = Arc::new(AtomicU64::new(0));
    let resize_cell_size = reported_cell_size.clone();
    let kitty_graphics_enabled = visual.kitty_graphics_enabled;
    std::thread::spawn(move || {
        super::resize_poll_loop(
            resize_tx,
            cols,
            rows,
            initial_cell_width_px,
            initial_cell_height_px,
            kitty_graphics_enabled,
            &resize_cell_size,
            &resize_quit,
        );
    });

    let mut instances = instances;
    for instance in &mut instances {
        if let Some(writer) = &instance.writer {
            let read_stream = writer.try_clone().map_err(ClientError::ConnectionFailed)?;
            spawn_server_reader(
                instance.id.clone(),
                instance.content_generation,
                read_stream,
                fleet_tx.clone(),
                should_quit.clone(),
                visual.kitty_graphics_enabled,
            );
        }
        if let Some(target) = instance.api_target.clone() {
            instance.request_tx = Some(spawn_api_worker(
                instance.id.clone(),
                target.clone(),
                fleet_tx.clone(),
                should_quit.clone(),
            ));
            spawn_snapshot_watcher(
                instance.id.clone(),
                target,
                fleet_tx.clone(),
                should_quit.clone(),
            );
        }
    }

    let host = super::ClientState {
        blit_encoder: crate::protocol::render_ansi::BlitEncoder::new(),
        mouse_capture_active: mouse_capture,
        keyboard_report_all_active: false,
        reported_size: (cols, rows),
        sound_config: visual.sound.clone(),
        kitty_graphics_enabled: visual.kitty_graphics_enabled,
        #[cfg(unix)]
        direct_graphics_response,
        #[cfg(unix)]
        retired_direct_graphics: None,
        attach_escape: None,
        #[cfg(unix)]
        mouse_scroll_lines: visual.mouse_scroll_lines,
        remote_image_paste_key: visual.remote_image_paste_key,
        redraw_on_focus_gained: visual.redraw_on_focus_gained,
        repaint_pending: true,
        draw_host_cursor: super::should_draw_host_cursor(visual.host_cursor),
    };
    let mut state = FleetState {
        instances,
        bridges,
        active: 0,
        sidebar,
        workspace_routes: HashMap::new(),
        tab_routes: HashMap::new(),
        pane_routes: HashMap::new(),
        projected_terminal_ids: HashMap::new(),
        projected_pane_ids: HashMap::new(),
        quit_requested: false,
        host,
        host_mouse_capture,
        host_sgr_pixels,
        prefix_input_source: crate::platform::RealPrefixInputSource::default(),
        pending_prefix_bytes: Vec::new(),
        cell_width_px: initial_cell_width_px,
        cell_height_px: initial_cell_height_px,
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
                    let (sidebar_changed, render_requested) = handle_fleet_event(
                        &mut state,
                        event,
                        &fleet_tx,
                        &should_quit,
                    );
                    if sidebar_changed {
                        sync_sidebar_model(&mut state, current_cols, current_rows);
                    }
                    if render_requested {
                        render(&mut state, current_cols, current_rows);
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                reconnect_content_streams(
                    &mut state,
                    current_cols,
                    current_rows,
                    &fleet_tx,
                );
            }
        }
    }

    for instance in &mut state.instances {
        if let Some(writer) = &mut instance.writer {
            let _ = super::write_to_server(writer, &ClientMessage::Detach);
        }
    }
    Ok(())
}

fn schedule_instance_retry(instance: &mut Instance) {
    instance.retry_at = Instant::now() + instance.retry_delay;
    instance.retry_delay = instance
        .retry_delay
        .saturating_mul(2)
        .min(RECONNECT_MAX_DELAY);
}

fn reset_instance_retry(instance: &mut Instance) {
    instance.retry_at = Instant::now();
    instance.retry_delay = RECONNECT_INITIAL_DELAY;
}

fn reconnect_content_streams(
    state: &mut FleetState,
    cols: u16,
    rows: u16,
    event_tx: &tokio::sync::mpsc::Sender<FleetEvent>,
) {
    update_sidebar_geometry(state, cols, rows);
    let content = state.sidebar.view.terminal_area;
    let active = state.active;

    for (index, instance) in state.instances.iter_mut().enumerate() {
        if instance.client_socket.is_none() {
            if instance.bridge_connecting || Instant::now() < instance.retry_at {
                continue;
            }
            let Some(target) = instance.remote_target.clone() else {
                continue;
            };
            let instance_id = instance.id.clone();
            let session = instance.remote_session.clone();
            let bridge_tx = event_tx.clone();
            instance.bridge_connecting = true;
            std::thread::spawn(move || {
                let result = crate::remote::retry_fleet_remote_bridge(
                    &target,
                    session.as_deref(),
                    &instance_id,
                )
                .map_err(|err| err.to_string());
                let _ = bridge_tx.blocking_send(FleetEvent::BridgeReady {
                    instance_id,
                    result,
                });
            });
            continue;
        }

        if index != active
            || instance.writer.is_some()
            || instance.content_connecting
            || Instant::now() < instance.retry_at
        {
            continue;
        }
        let Some(socket) = instance.client_socket.clone() else {
            continue;
        };
        let instance_id = instance.id.clone();
        let content_tx = event_tx.clone();
        let content_cols = content.width.max(2);
        let content_rows = content.height.max(1);
        let cell_width_px = state.cell_width_px;
        let cell_height_px = state.cell_height_px;
        instance.content_generation = instance.content_generation.wrapping_add(1);
        let generation = instance.content_generation;
        instance.content_connecting = true;
        std::thread::spawn(move || {
            let result = connect_app(
                &socket,
                content_cols,
                content_rows,
                cell_width_px,
                cell_height_px,
            )
            .map_err(|err| err.to_string());
            let _ = content_tx.blocking_send(FleetEvent::ContentReady {
                instance_id,
                generation,
                result,
            });
        });
    }
}

fn spawn_api_worker(
    instance_id: String,
    target: ConnectionTarget,
    event_tx: tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: Arc<AtomicBool>,
) -> std::sync::mpsc::Sender<ApiCommand> {
    let (tx, rx) = std::sync::mpsc::channel::<ApiCommand>();
    std::thread::spawn(move || {
        let client = ApiClient::for_target(target);
        while !should_quit.load(Ordering::Acquire) {
            let Ok(command) = rx.recv_timeout(Duration::from_millis(100)) else {
                continue;
            };
            let result = client
                .request(Request {
                    id: command.id.to_string(),
                    method: command.method,
                })
                .map(|_| ())
                .map_err(|err| err.to_string());
            if let Err(err) = &result {
                tracing::warn!(id = command.id, %err, "fleet API command failed");
            }
            if event_tx
                .blocking_send(FleetEvent::ApiResult {
                    instance_id: instance_id.clone(),
                    label: command.label,
                    result,
                })
                .is_err()
            {
                return;
            }
        }
    });
    tx
}

fn spawn_server_reader(
    instance_id: String,
    generation: u64,
    mut stream: LocalStream,
    event_tx: tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: Arc<AtomicBool>,
    kitty_graphics_enabled: bool,
) {
    std::thread::spawn(move || {
        let _ = stream.set_nonblocking(false);
        while !should_quit.load(Ordering::Acquire) {
            let max_frame_size = if kitty_graphics_enabled {
                MAX_GRAPHICS_FRAME_SIZE
            } else {
                MAX_FRAME_SIZE
            };
            match protocol::read_message(&mut stream, max_frame_size) {
                Ok(message) => {
                    if event_tx
                        .blocking_send(FleetEvent::ServerMessage {
                            instance_id: instance_id.clone(),
                            generation,
                            message,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) => {
                    let _ = event_tx.blocking_send(FleetEvent::Disconnected {
                        instance_id,
                        generation,
                    });
                    return;
                }
            }
        }
    });
}

#[derive(Clone)]
struct PaneWatchState {
    pane_id: String,
    status: AgentStatus,
}

struct SnapshotWatch {
    panes: Vec<PaneWatchState>,
    connected: bool,
}

fn spawn_snapshot_watcher(
    instance_id: String,
    target: ConnectionTarget,
    event_tx: tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let mut retry_delay = RECONNECT_INITIAL_DELAY;
        while !should_quit.load(Ordering::Acquire) {
            let client = ApiClient::for_target(target.clone());
            let snapshot = match send_snapshot(&instance_id, &client, &event_tx) {
                Ok(snapshot) => snapshot,
                Err(()) => return,
            };
            if !snapshot.connected {
                std::thread::sleep(retry_delay);
                retry_delay = retry_delay.saturating_mul(2).min(RECONNECT_MAX_DELAY);
                continue;
            }
            retry_delay = RECONNECT_INITIAL_DELAY;

            let result = watch_instance_events(
                &instance_id,
                &client,
                &event_tx,
                &should_quit,
                &snapshot.panes,
            );
            if should_quit.load(Ordering::Acquire) {
                return;
            }
            match result {
                Ok(true) => continue,
                Ok(false) => return,
                Err(error) => {
                    let _ = event_tx.blocking_send(FleetEvent::Snapshot {
                        instance_id: instance_id.clone(),
                        result: Err(error),
                    });
                }
            }
            std::thread::sleep(retry_delay);
            retry_delay = retry_delay.saturating_mul(2).min(RECONNECT_MAX_DELAY);
        }
    });
}

fn send_snapshot(
    instance_id: &str,
    client: &ApiClient,
    event_tx: &tokio::sync::mpsc::Sender<FleetEvent>,
) -> Result<SnapshotWatch, ()> {
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
    let connected = result.is_ok();
    let mut panes = result
        .as_ref()
        .map(|snapshot| {
            snapshot
                .panes
                .iter()
                .map(|pane| PaneWatchState {
                    pane_id: pane.pane_id.clone(),
                    status: pane.agent_status,
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    panes.sort_by(|left, right| left.pane_id.cmp(&right.pane_id));
    event_tx
        .blocking_send(FleetEvent::Snapshot {
            instance_id: instance_id.to_string(),
            result,
        })
        .map_err(|_| ())?;
    Ok(SnapshotWatch { panes, connected })
}

fn watch_instance_events(
    instance_id: &str,
    client: &ApiClient,
    event_tx: &tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: &AtomicBool,
    pane_states: &[PaneWatchState],
) -> Result<bool, String> {
    use crate::api::schema::{
        EventsSubscribeParams, Subscription, SubscriptionEventData, SubscriptionEventEnvelope,
    };

    let mut stream =
        crate::ipc::connect_local_stream(&client.socket_path()).map_err(|err| err.to_string())?;
    let mut subscriptions = vec![
        Subscription::WorkspaceCreated {},
        Subscription::WorkspaceUpdated {},
        Subscription::WorkspaceMetadataUpdated {},
        Subscription::WorkspaceRenamed {},
        Subscription::WorkspaceMoved {},
        Subscription::WorkspaceReordered {},
        Subscription::WorkspaceClosed {},
        Subscription::WorkspaceFocused {},
        Subscription::WorktreeCreated {},
        Subscription::WorktreeOpened {},
        Subscription::WorktreeRemoved {},
        Subscription::TabCreated {},
        Subscription::TabClosed {},
        Subscription::TabFocused {},
        Subscription::TabRenamed {},
        Subscription::TabMoved {},
        Subscription::PaneCreated {},
        Subscription::PaneClosed {},
        Subscription::PaneUpdated {},
        Subscription::PaneFocused {},
        Subscription::PaneMoved {},
        Subscription::PaneExited {},
        Subscription::PaneAgentDetected {},
        Subscription::LayoutUpdated {},
    ];
    subscriptions.extend(
        pane_states
            .iter()
            .map(|pane| Subscription::PaneAgentStatusChanged {
                pane_id: pane.pane_id.clone(),
                agent_status: None,
            }),
    );
    let request = Request {
        id: format!("fleet-events:{instance_id}"),
        method: Method::EventsSubscribe(EventsSubscribeParams { subscriptions }),
    };
    let encoded = serde_json::to_vec(&request).map_err(|err| err.to_string())?;
    stream.write_all(&encoded).map_err(|err| err.to_string())?;
    stream.write_all(b"\n").map_err(|err| err.to_string())?;
    stream.flush().map_err(|err| err.to_string())?;
    let mut last_status = pane_states
        .iter()
        .map(|pane| (pane.pane_id.clone(), pane.status))
        .collect::<HashMap<_, _>>();
    let pane_ids = pane_states
        .iter()
        .map(|pane| pane.pane_id.as_str())
        .collect::<Vec<_>>();
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    while !should_quit.load(Ordering::Acquire) {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => return Err("event stream disconnected".to_string()),
            Ok(_) => {
                if let Ok(subscription_event) =
                    serde_json::from_str::<SubscriptionEventEnvelope>(&line)
                {
                    if let SubscriptionEventData::PaneAgentStatusChanged(event) =
                        subscription_event.data
                    {
                        if let Some(previous_status) =
                            last_status.insert(event.pane_id.clone(), event.agent_status)
                        {
                            if previous_status != event.agent_status {
                                let _ = event_tx.blocking_send(FleetEvent::AgentStatusChanged {
                                    instance_id: instance_id.to_string(),
                                    event,
                                    previous_status,
                                });
                            }
                        }
                    }
                }

                let updated = match send_snapshot(instance_id, client, event_tx) {
                    Ok(snapshot) if snapshot.connected => snapshot,
                    Ok(_) => return Err("snapshot refresh failed".to_string()),
                    Err(()) => return Ok(false),
                };
                let updated_ids = updated
                    .panes
                    .iter()
                    .map(|pane| pane.pane_id.as_str())
                    .collect::<Vec<_>>();
                if updated_ids != pane_ids {
                    return Ok(true);
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.to_string()),
        }
    }
    Ok(false)
}

fn handle_fleet_event(
    state: &mut FleetState,
    event: FleetEvent,
    event_tx: &tokio::sync::mpsc::Sender<FleetEvent>,
    should_quit: &Arc<AtomicBool>,
) -> (bool, bool) {
    let instance_id = match &event {
        FleetEvent::BridgeReady { instance_id, .. }
        | FleetEvent::ContentReady { instance_id, .. }
        | FleetEvent::ServerMessage { instance_id, .. }
        | FleetEvent::Disconnected { instance_id, .. }
        | FleetEvent::Snapshot { instance_id, .. }
        | FleetEvent::ApiResult { instance_id, .. }
        | FleetEvent::AgentStatusChanged { instance_id, .. } => instance_id,
    };
    let Some(index) = state
        .instances
        .iter()
        .position(|instance| &instance.id == instance_id)
    else {
        return (false, false);
    };

    let event_generation = match &event {
        FleetEvent::ContentReady { generation, .. }
        | FleetEvent::ServerMessage { generation, .. }
        | FleetEvent::Disconnected { generation, .. } => Some(*generation),
        _ => None,
    };
    if event_generation
        .is_some_and(|generation| generation != state.instances[index].content_generation)
    {
        return (false, false);
    }

    match event {
        FleetEvent::BridgeReady { result, .. } => {
            state.instances[index].bridge_connecting = false;
            let changed = match result {
                Ok(bridge) => {
                    let client_socket = bridge.client_socket().to_path_buf();
                    let api_target =
                        ConnectionTarget::SocketPath(bridge.api_socket().to_path_buf());
                    state.instances[index].bridge_status = Some(bridge.status());
                    state.instances[index].client_socket = Some(client_socket);
                    state.instances[index].api_target = Some(api_target.clone());
                    state.instances[index].request_tx = Some(spawn_api_worker(
                        state.instances[index].id.clone(),
                        api_target.clone(),
                        event_tx.clone(),
                        should_quit.clone(),
                    ));
                    spawn_snapshot_watcher(
                        state.instances[index].id.clone(),
                        api_target,
                        event_tx.clone(),
                        should_quit.clone(),
                    );
                    state.instances[index].error = None;
                    reset_instance_retry(&mut state.instances[index]);
                    state.bridges.push(bridge);
                    true
                }
                Err(err) => {
                    let changed = state.instances[index].error.as_deref() != Some(err.as_str());
                    state.instances[index].error = Some(err);
                    schedule_instance_retry(&mut state.instances[index]);
                    changed
                }
            };
            if changed {
                state.host.request_repaint();
            }
            (changed, changed)
        }
        FleetEvent::ContentReady {
            generation, result, ..
        } => {
            state.instances[index].content_connecting = false;
            let changed = match result {
                Ok(writer) => match writer.try_clone() {
                    Ok(reader) => {
                        spawn_server_reader(
                            state.instances[index].id.clone(),
                            generation,
                            reader,
                            event_tx.clone(),
                            should_quit.clone(),
                            state.host.kitty_graphics_enabled,
                        );
                        state.instances[index].writer = Some(writer);
                        state.instances[index].error = None;
                        reset_instance_retry(&mut state.instances[index]);
                        super::query_host_terminal_theme();
                        true
                    }
                    Err(err) => {
                        let error = err.to_string();
                        let changed =
                            state.instances[index].error.as_deref() != Some(error.as_str());
                        state.instances[index].error = Some(error);
                        schedule_instance_retry(&mut state.instances[index]);
                        changed
                    }
                },
                Err(error) => {
                    let error = state.instances[index]
                        .bridge_status
                        .as_ref()
                        .and_then(crate::remote::FleetRemoteBridgeStatus::client_error)
                        .unwrap_or(error);
                    let changed = state.instances[index].error.as_deref() != Some(error.as_str());
                    state.instances[index].error = Some(error);
                    schedule_instance_retry(&mut state.instances[index]);
                    changed
                }
            };
            if changed {
                state.host.request_repaint();
            }
            (changed, changed)
        }
        FleetEvent::ServerMessage { message, .. } => match message {
            ServerMessage::Frame(frame) => {
                state.instances[index].frame = Some(frame);
                (false, true)
            }
            ServerMessage::Terminal(_) | ServerMessage::Graphics { .. } => {
                tracing::debug!(instance = %state.instances[index].id, "ignored non-semantic fleet frame");
                (false, false)
            }
            ServerMessage::Notify {
                kind,
                message,
                body,
            } => {
                super::handle_notify(kind, &message, body.as_deref(), &state.host.sound_config);
                (false, false)
            }
            ServerMessage::Clipboard { data } if index == state.active => {
                super::forward_clipboard(&data);
                (false, false)
            }
            ServerMessage::WindowTitle { title } if index == state.active => {
                state.instances[index].window_title = title.clone();
                let _ = crate::terminal_effects::write_window_title(
                    &mut io::stdout(),
                    title.as_deref(),
                );
                (false, false)
            }
            ServerMessage::WindowTitle { title } => {
                state.instances[index].window_title = title;
                (false, false)
            }
            ServerMessage::ReloadSoundConfig => {
                super::reload_local_client_config(
                    &mut state.host.sound_config,
                    &mut state.host.redraw_on_focus_gained,
                    &mut state.host.draw_host_cursor,
                    &mut state.host.remote_image_paste_key,
                );
                reload_fleet_visual_config(state);
                (true, true)
            }
            ServerMessage::MouseCapture {
                enabled,
                sgr_pixels,
            } => {
                state.instances[index].mouse_capture = enabled;
                state.instances[index].mouse_sgr_pixels = enabled && sgr_pixels;
                if index == state.active {
                    let _ = apply_active_host_modes(state);
                }
                (false, false)
            }
            ServerMessage::KittyKeyboardReportAll { enabled } => {
                state.instances[index].keyboard_report_all = enabled;
                if index == state.active {
                    let _ = apply_active_host_modes(state);
                }
                (false, false)
            }
            ServerMessage::PrefixInputSource { active } => {
                state.instances[index].prefix_input_active = active;
                if index == state.active {
                    apply_active_prefix_input_source(state);
                }
                (false, false)
            }
            ServerMessage::ServerShutdown { reason } => {
                state.instances[index].error =
                    Some(reason.unwrap_or_else(|| "server shut down".to_string()));
                state.instances[index].content_generation =
                    state.instances[index].content_generation.wrapping_add(1);
                state.instances[index].content_connecting = false;
                state.instances[index].writer = None;
                state.instances[index].frame = None;
                schedule_instance_retry(&mut state.instances[index]);
                (true, true)
            }
            _ => (false, false),
        },
        FleetEvent::Disconnected { .. } => {
            state.instances[index].error = Some("disconnected".to_string());
            state.instances[index].content_connecting = false;
            state.instances[index].writer = None;
            state.instances[index].frame = None;
            schedule_instance_retry(&mut state.instances[index]);
            (true, true)
        }
        FleetEvent::Snapshot { result, .. } => match result {
            Ok(snapshot) => {
                let changed = state.instances[index].snapshot.as_ref() != Some(&snapshot)
                    || !state.instances[index].api_connected;
                state.instances[index].snapshot = Some(snapshot);
                state.instances[index].api_connected = true;
                if state.instances[index].writer.is_some() || index != state.active {
                    state.instances[index].error = None;
                }
                (changed, changed)
            }
            Err(error) => {
                state.instances[index].api_connected = false;
                let error = state.instances[index]
                    .bridge_status
                    .as_ref()
                    .and_then(crate::remote::FleetRemoteBridgeStatus::api_error)
                    .unwrap_or(error);
                let changed = state.instances[index].error.as_deref() != Some(error.as_str());
                state.instances[index].error = Some(error);
                (changed, changed)
            }
        },
        FleetEvent::ApiResult { label, result, .. } => {
            let next_error = result.err().map(|error| format!("{label}: {error}"));
            let changed = state.instances[index].action_error != next_error;
            state.instances[index].action_error = next_error;
            if changed {
                state.host.request_repaint();
            }
            (false, changed)
        }
        FleetEvent::AgentStatusChanged {
            event,
            previous_status,
            ..
        } => {
            handle_background_agent_status_change(state, index, event, previous_status);
            (false, false)
        }
    }
}

fn handle_background_agent_status_change(
    state: &mut FleetState,
    index: usize,
    event: crate::api::schema::PaneAgentStatusChangedEvent,
    previous_status: AgentStatus,
) {
    if index == state.active || state.visual.toast.delay_seconds != 0 {
        return;
    }

    let previous_state = native_agent_state(previous_status).0;
    let next_state = native_agent_state(event.agent_status).0;
    let agent_label = event
        .display_agent
        .as_deref()
        .or(event.agent.as_deref())
        .unwrap_or("agent");
    let known_agent = event
        .agent
        .as_deref()
        .and_then(crate::detect::parse_agent_label);

    if state.host.sound_config.allows(known_agent) {
        if let Some(sound) =
            crate::app::actions::notification_sound_for_state_change_with_agent_labels(
                false,
                previous_state,
                next_state,
                event.agent.as_deref(),
                event.agent.as_deref(),
            )
        {
            crate::sound::play(sound, &state.host.sound_config);
        }
    }

    let Some(kind) = crate::app::actions::notification_toast_for_state_change_with_agent_labels(
        false,
        previous_state,
        next_state,
        event.agent.as_deref(),
        event.agent.as_deref(),
    ) else {
        return;
    };
    let event_text = match kind {
        crate::app::state::ToastKind::NeedsAttention => "needs attention",
        crate::app::state::ToastKind::Finished => "finished",
        crate::app::state::ToastKind::UpdateInstalled => "updated",
    };
    let workspace_label = state.instances[index]
        .snapshot
        .as_ref()
        .and_then(|snapshot| {
            snapshot
                .workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == event.workspace_id)
        })
        .map(|workspace| workspace.label.as_str())
        .unwrap_or("workspace");
    let title = format!("{agent_label} {event_text}");
    let context = format!("{} · {workspace_label}", state.instances[index].name);

    match state.visual.toast.delivery {
        crate::config::ToastDelivery::Off => {}
        crate::config::ToastDelivery::Herdr => {
            state.sidebar.toast = Some(crate::app::state::ToastNotification {
                kind,
                title,
                context,
                position: None,
                target: None,
            });
            state.host.request_repaint();
        }
        crate::config::ToastDelivery::Terminal => super::handle_notify(
            crate::protocol::NotifyKind::Toast,
            &title,
            Some(&context),
            &state.host.sound_config,
        ),
        crate::config::ToastDelivery::System => super::handle_notify(
            crate::protocol::NotifyKind::SystemToast,
            &title,
            Some(&context),
            &state.host.sound_config,
        ),
    }
}

fn apply_active_host_modes(state: &mut FleetState) -> Result<(), ClientError> {
    let desired_mouse = state.instances[state.active].mouse_capture;
    let desired_sgr_pixels = desired_mouse && state.instances[state.active].mouse_sgr_pixels;
    if desired_mouse != state.host.mouse_capture_active
        || desired_sgr_pixels != state.host_sgr_pixels.load(Ordering::Acquire)
    {
        super::set_mouse_capture(desired_mouse, desired_sgr_pixels)
            .map_err(ClientError::ConnectionFailed)?;
        state.host.mouse_capture_active = desired_mouse;
        state
            .host_mouse_capture
            .store(desired_mouse, Ordering::Release);
        state
            .host_sgr_pixels
            .store(desired_sgr_pixels, Ordering::Release);
    }
    let desired_keyboard = state.instances[state.active].keyboard_report_all;
    if desired_keyboard != state.host.keyboard_report_all_active {
        crate::terminal_modes::set_host_kitty_keyboard_report_all(
            &mut io::stdout(),
            desired_keyboard,
        )
        .map_err(ClientError::ConnectionFailed)?;
        state.host.keyboard_report_all_active = desired_keyboard;
    }
    Ok(())
}

fn apply_active_prefix_input_source(state: &mut FleetState) {
    if state.instances[state.active].prefix_input_active {
        state.prefix_input_source.switch_to_ascii();
    } else {
        state.prefix_input_source.restore();
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
        ClientLoopEvent::Resize(cols, rows, cell_width_px, cell_height_px) => {
            state.host.reported_size = (cols, rows);
            state.cell_width_px = cell_width_px;
            state.cell_height_px = cell_height_px;
            update_sidebar_geometry(state, cols, rows);
            resize_content_streams(state, cols, rows)?;
            state.host.request_repaint();
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
    let parsed = crate::raw_input::parse_raw_input_bytes_sync(&data);
    if crate::raw_input::events_require_host_surface_redraw(
        &parsed,
        state.host.redraw_on_focus_gained,
    ) {
        state.host.request_repaint();
    }
    if crate::raw_input::events_require_host_terminal_theme_query(&parsed) {
        super::query_host_terminal_theme();
    }
    if let Some((width_px, height_px)) = super::reported_cell_size_from_events(&parsed) {
        state.cell_width_px = width_px;
        state.cell_height_px = height_px;
    }
    let active_is_remote = state.active != 0;
    if super::should_bridge_clipboard_image_paste(
        &data,
        active_is_remote,
        state.host.remote_image_paste_key,
    ) {
        if let Some(image) = crate::platform::read_clipboard_image() {
            write_active_remote_image(state, image, "clipboard paste");
            return Ok(());
        }
    }
    if let Some(image) = super::read_image_file_from_terminal_drop(&data, active_is_remote) {
        write_active_remote_image(state, image, "file drop");
        return Ok(());
    }

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
                let previous_mode = state.sidebar.mode;
                match crate::app::handle_client_sidebar_key(&mut state.sidebar, key) {
                    crate::app::ClientSidebarInput::Forward => {
                        if previous_mode == crate::app::Mode::Terminal
                            && state.sidebar.mode == crate::app::Mode::Prefix
                        {
                            state.pending_prefix_bytes = raw.to_vec();
                        }
                        forward.extend_from_slice(raw);
                    }
                    crate::app::ClientSidebarInput::ForwardWithPrefix => {
                        flush_active_input(state, &mut forward)?;
                        let mut replay = state.pending_prefix_bytes.clone();
                        replay.extend_from_slice(raw);
                        write_active(state, ClientMessage::Input { data: replay })?;
                        shell_changed = true;
                    }
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
            crate::raw_input::RawInputEvent::HostDefaultColor { kind, color } => {
                flush_active_input(state, &mut forward)?;
                shell_changed |= state.sidebar.update_host_terminal_theme_state(kind, color);
                broadcast_input(state, raw)?;
            }
            crate::raw_input::RawInputEvent::HostPaletteColors { colors } => {
                flush_active_input(state, &mut forward)?;
                shell_changed |= state.sidebar.update_host_terminal_palette_state(&colors);
                broadcast_input(state, raw)?;
            }
            crate::raw_input::RawInputEvent::HostColorSchemeChanged(appearance) => {
                flush_active_input(state, &mut forward)?;
                shell_changed |= state
                    .sidebar
                    .set_host_terminal_appearance_for_presentation(Some(appearance), true);
                broadcast_input(state, raw)?;
            }
            crate::raw_input::RawInputEvent::HostCellSizeReport {
                width_px,
                height_px,
            } => {
                state.cell_width_px = width_px;
                state.cell_height_px = height_px;
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
        state.host.request_repaint();
        persist_fleet_presentation(state);
        render(state, current_cols, current_rows);
    }
    Ok(())
}

fn persist_fleet_presentation(state: &mut FleetState) {
    state.sidebar.session_dirty = false;
    let presentation = crate::fleet::FleetPresentation {
        sidebar_width: Some(state.sidebar.sidebar_width),
        sidebar_section_split: Some(state.sidebar.sidebar_section_split),
        collapsed_space_keys: state.sidebar.collapsed_space_keys.clone(),
    };
    if crate::fleet::load().presentation() == &presentation {
        return;
    }
    if let Err(err) = crate::fleet::update(|registry| {
        registry.set_presentation(presentation);
        Ok(())
    }) {
        tracing::warn!(err = %err, "failed to persist fleet presentation");
    }
}

fn broadcast_input(state: &mut FleetState, data: &[u8]) -> Result<(), ClientError> {
    write_active(
        state,
        ClientMessage::Input {
            data: data.to_vec(),
        },
    )
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
    update_sidebar_geometry(state, current_cols, current_rows);
    let content_rect = state.sidebar.view.terminal_area;
    let sidebar_width = state.sidebar.view.sidebar_rect.width;
    if state.sidebar.mode == crate::app::Mode::Prefix {
        cancel_active_server_prefix(state)?;
        state.sidebar.mode = crate::app::Mode::Terminal;
    }
    let sidebar_related = state.sidebar.mode != crate::app::Mode::Terminal
        || mouse.column < content_rect.x
        || mouse.row < content_rect.y
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
            column: mouse.column.saturating_sub(content_rect.x),
            row: mouse.row.saturating_sub(content_rect.y),
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
    update_sidebar_geometry(state, cols, rows);
    let content = state.sidebar.view.terminal_area;
    let content_cols = content.width.max(2);
    let content_rows = content.height.max(1);
    let index = state.active;
    state.instances[index].frame = None;
    if let Some(writer) = state.instances[index].writer.as_mut() {
        if let Err(err) = super::write_to_server(
            writer,
            &ClientMessage::Resize {
                cols: content_cols,
                rows: content_rows,
                cell_width_px: state.cell_width_px,
                cell_height_px: state.cell_height_px,
            },
        ) {
            mark_content_stream_disconnected(state, index, err);
        }
    }
    Ok(())
}

fn write_active(state: &mut FleetState, message: ClientMessage) -> Result<(), ClientError> {
    let index = state.active;
    let Some(writer) = state.instances[index].writer.as_mut() else {
        return Ok(());
    };
    if let Err(err) = super::write_to_server(writer, &message) {
        mark_content_stream_disconnected(state, index, err);
    }
    Ok(())
}

fn write_active_remote_image(
    state: &mut FleetState,
    image: crate::platform::ClipboardImage,
    source: &'static str,
) {
    let index = state.active;
    let Some(writer) = state.instances[index].writer.as_mut() else {
        return;
    };
    if let Err(err) = super::write_remote_image_to_server(writer, image, source) {
        mark_content_stream_disconnected(state, index, io::Error::other(err.to_string()));
    }
}

fn mark_content_stream_disconnected(state: &mut FleetState, index: usize, err: io::Error) {
    let Some(instance) = state.instances.get_mut(index) else {
        return;
    };
    tracing::debug!(instance = %instance.id, %err, "fleet content stream write failed");
    instance.error = Some("disconnected".to_string());
    instance.content_generation = instance.content_generation.wrapping_add(1);
    instance.content_connecting = false;
    instance.writer = None;
    instance.frame = None;
    schedule_instance_retry(instance);
    state.host.request_repaint();
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
            focus_sidebar_route(state, ws_idx)
        }
        crate::app::ClientSidebarAction::FocusTab { ws_idx, tab_idx } => {
            focus_tab_route(state, ws_idx, tab_idx)
        }
        crate::app::ClientSidebarAction::FocusPane { ws_idx, pane_id } => {
            focus_pane_route(state, ws_idx, pane_id)
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
            let close_group = state
                .sidebar
                .workspace_close_would_close_worktree_group(ws_idx);
            if let Some(SidebarHit::Workspace {
                index,
                workspace_id,
            }) = state.workspace_routes.get(&ws_idx).cloned()
            {
                request_for_instance(
                    state,
                    index,
                    "fleet-workspace-close",
                    Method::WorkspaceClose(WorkspaceCloseParams {
                        workspace_id,
                        close_group,
                    }),
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

fn focus_sidebar_route(state: &mut FleetState, ws_idx: usize) {
    let route = state.workspace_routes.get(&ws_idx).cloned();
    activate_sidebar_route(state, ws_idx, route);
}

fn focus_tab_route(state: &mut FleetState, ws_idx: usize, tab_idx: usize) {
    let route = state.tab_routes.get(&(ws_idx, tab_idx)).cloned();
    activate_sidebar_route(state, ws_idx, route);
}

fn focus_pane_route(state: &mut FleetState, ws_idx: usize, pane_id: crate::layout::PaneId) {
    let route = state
        .pane_routes
        .get(&(ws_idx, pane_id))
        .or_else(|| state.workspace_routes.get(&ws_idx))
        .cloned();
    activate_sidebar_route(state, ws_idx, route);
}

fn detach_instance_content(state: &mut FleetState, index: usize) {
    let Some(instance) = state.instances.get_mut(index) else {
        return;
    };
    if let Some(mut writer) = instance.writer.take() {
        let _ = super::write_to_server(&mut writer, &ClientMessage::Detach);
    }
    instance.content_generation = instance.content_generation.wrapping_add(1);
    instance.content_connecting = false;
    instance.frame = None;
}

fn activate_sidebar_route(state: &mut FleetState, ws_idx: usize, route: Option<SidebarHit>) {
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
        SidebarHit::Tab { index, tab_id } => (index, Some(Method::TabFocus(TabTarget { tab_id }))),
        SidebarHit::Pane { index, pane_id } => {
            (index, Some(Method::PaneFocus(PaneTarget { pane_id })))
        }
    };
    if state.active != index {
        let previous = state.active;
        detach_instance_content(state, previous);
        state.active = index;
        reset_instance_retry(&mut state.instances[index]);
        super::query_host_terminal_theme();
    } else if state.instances[index].writer.is_none() {
        reset_instance_retry(&mut state.instances[index]);
    }
    state.host.request_repaint();
    state.sidebar.active = Some(ws_idx);
    state.sidebar.selected = ws_idx;
    if let Some(method) = method {
        request_for_instance(state, index, "fleet-focus", method);
    }
    let _ = apply_active_host_modes(state);
    apply_active_prefix_input_source(state);
    let _ = crate::terminal_effects::write_window_title(
        &mut io::stdout(),
        state.instances[index].window_title.as_deref(),
    );
}

fn api_command_label(id: &str) -> &str {
    match id {
        "fleet-workspace-create" => "create workspace",
        "fleet-workspace-rename" => "rename workspace",
        "fleet-workspace-close" => "close workspace",
        "fleet-focus" => "focus target",
        "fleet-reload-config" => "reload config",
        _ => "fleet action",
    }
}

fn request_for_active(state: &mut FleetState, id: &str, method: Method) {
    request_for_instance(state, state.active, id, method);
}

fn request_for_instance(state: &mut FleetState, index: usize, id: &str, method: Method) {
    let Some(request_tx) = state
        .instances
        .get(index)
        .and_then(|instance| instance.request_tx.as_ref())
        .cloned()
    else {
        if let Some(instance) = state.instances.get_mut(index) {
            instance.action_error = Some(format!(
                "{}: instance API is not connected",
                api_command_label(id)
            ));
        }
        state.host.request_repaint();
        return;
    };
    if request_tx
        .send(ApiCommand {
            id: id.to_string(),
            label: api_command_label(id).to_string(),
            method,
        })
        .is_err()
    {
        if let Some(instance) = state.instances.get_mut(index) {
            instance.action_error = Some(format!(
                "{}: instance API worker stopped",
                api_command_label(id)
            ));
        }
        state.host.request_repaint();
    }
}

fn render(state: &mut FleetState, cols: u16, rows: u16) {
    if cols == 0 || rows == 0 {
        return;
    }
    update_sidebar_geometry(state, cols, rows);
    let content_rect = state.sidebar.view.terminal_area;
    let base = compose_frame(
        state.instances[state.active].frame.as_ref(),
        cols,
        rows,
        content_rect,
    );
    let active_instance = &state.instances[state.active];
    let presentation_error = active_instance
        .action_error
        .as_ref()
        .map(|error| (format!("{} action", active_instance.name), error.clone()))
        .or_else(|| {
            active_instance
                .writer
                .is_none()
                .then(|| active_instance.error.clone())
                .flatten()
                .map(|error| (active_instance.name.clone(), error))
        });
    let Some(base_buffer) = base.to_ratatui_buffer() else {
        return;
    };
    let backend = TestBackend::new(cols, rows);
    let mut terminal = ratatui::Terminal::new(backend)
        .expect("fleet shell TestBackend construction should not fail");
    terminal
        .draw(|frame| {
            frame.buffer_mut().clone_from(&base_buffer);
            if let Some((instance_name, error)) = &presentation_error {
                crate::ui::render_client_connection_error(
                    frame,
                    content_rect,
                    instance_name,
                    error,
                    &state.sidebar.palette,
                );
            }
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
    let frame = if state.host.draw_host_cursor {
        crate::protocol::render_ansi::frame_with_drawn_cursor(frame)
    } else {
        frame
    };
    let encoded = if state.host.draw_host_cursor {
        state
            .host
            .blit_encoder
            .encode_with_suppressed_visible_cursor(&frame, state.host.repaint_pending)
    } else {
        state
            .host
            .blit_encoder
            .encode(&frame, state.host.repaint_pending)
    };
    let mut stdout = io::stdout();
    let graphics = if state.host.kitty_graphics_enabled {
        frame.graphics.as_slice()
    } else {
        &[]
    };
    let _ = super::write_encoded_frame_with_graphics(&mut stdout, &encoded.bytes, graphics);
    let _ = stdout.flush();
    state.host.blit_encoder.commit(frame, encoded);
    state.host.repaint_pending = false;
}

fn apply_fleet_visual_config(state: &mut FleetState, visual: FleetVisualConfig) {
    state.host.sound_config = visual.sound.clone();
    state.host.mouse_scroll_lines = visual.mouse_scroll_lines;
    state.host.redraw_on_focus_gained = visual.redraw_on_focus_gained;
    state.host.remote_image_paste_key = visual.remote_image_paste_key;
    state.host.draw_host_cursor = super::should_draw_host_cursor(visual.host_cursor);
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
    let (mut model, workspace_routes, tab_routes, pane_routes) = build_sidebar_state(state);
    state.sidebar.terminals = std::mem::take(&mut model.terminals);
    state.sidebar.workspaces = std::mem::take(&mut model.workspaces);
    state.sidebar.active = model.active;
    if !matches!(
        state.sidebar.mode,
        crate::app::Mode::RenameWorkspace
            | crate::app::Mode::ContextMenu
            | crate::app::Mode::ConfirmClose
            | crate::app::Mode::Navigate
    ) {
        state.sidebar.selected = model.selected;
    }
    state.workspace_routes = workspace_routes;
    state.tab_routes = tab_routes;
    state.pane_routes = pane_routes;
    update_sidebar_geometry(state, cols, rows);
}

fn current_sidebar_width(state: &FleetState, cols: u16) -> u16 {
    state
        .sidebar
        .view
        .sidebar_rect
        .width
        .min(cols.saturating_sub(2))
}

fn update_sidebar_geometry(state: &mut FleetState, cols: u16, rows: u16) {
    crate::ui::compute_client_shell_view(&mut state.sidebar, Rect::new(0, 0, cols, rows));
}

fn build_sidebar_state(
    state: &mut FleetState,
) -> (
    crate::app::AppState,
    HashMap<usize, SidebarHit>,
    HashMap<(usize, usize), SidebarHit>,
    HashMap<(usize, crate::layout::PaneId), SidebarHit>,
) {
    let mut app = crate::app::AppState::empty_for_client_rendering();

    let active_instance = state.active;
    let (instances, projected_terminal_ids, projected_pane_ids) = (
        &state.instances,
        &mut state.projected_terminal_ids,
        &mut state.projected_pane_ids,
    );

    let mut workspace_routes = HashMap::new();
    let mut tab_routes = HashMap::new();
    let mut pane_routes = HashMap::new();
    let mut active_workspace = None;

    for (instance_idx, instance) in instances.iter().enumerate() {
        let snapshot_workspaces = instance
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.workspaces.as_slice())
            .unwrap_or(&[]);
        let connection_failed = !instance.api_connected && instance.error.is_some();
        let stale = !instance.api_connected;

        if snapshot_workspaces.is_empty() {
            let status = if connection_failed {
                "offline"
            } else if instance.api_connected {
                "no spaces"
            } else {
                "connecting"
            };
            let projection_key = format!("{}\0empty", instance.id);
            let terminal_id = projected_terminal_ids
                .entry(projection_key.clone())
                .or_insert_with(crate::terminal::TerminalId::alloc)
                .clone();
            let pane_id = *projected_pane_ids
                .entry(projection_key)
                .or_insert_with(crate::layout::PaneId::alloc);
            let (workspace, _) = crate::workspace::Workspace::sidebar_placeholder_with_tabs(
                format!("fleet:{}:empty", instance.id),
                format!("{} · {status}", instance.name),
                None,
                vec![crate::workspace::SidebarPlaceholderTab {
                    label: None,
                    number: 1,
                    pane_terminals: vec![(terminal_id.clone(), true)],
                    pane_ids: vec![pane_id],
                    focused_pane_idx: Some(0),
                }],
                0,
            );
            let ws_idx = app.workspaces.len();
            let mut terminal = crate::terminal::TerminalState::new(terminal_id.clone(), "/".into());
            if connection_failed {
                terminal.state = crate::detect::AgentState::Blocked;
            } else if instance.bridge_connecting || instance.content_connecting {
                terminal.state = crate::detect::AgentState::Working;
            }
            app.terminals.insert(terminal_id, terminal);
            app.workspaces.push(workspace);
            workspace_routes.insert(
                ws_idx,
                SidebarHit::Instance {
                    index: instance_idx,
                },
            );
            if instance_idx == active_instance && active_workspace.is_none() {
                active_workspace = Some(ws_idx);
            }
            continue;
        }

        let snapshot = instance.snapshot.as_ref().expect("snapshot checked above");
        for workspace_info in snapshot_workspaces {
            let mut terminals = Vec::new();
            let snapshot_tabs = snapshot
                .tabs
                .iter()
                .filter(|tab| tab.workspace_id == workspace_info.workspace_id)
                .collect::<Vec<_>>();
            let active_tab_idx = snapshot_tabs
                .iter()
                .position(|tab| {
                    tab.tab_id == workspace_info.active_tab_id
                        || snapshot.focused_tab_id.as_deref() == Some(tab.tab_id.as_str())
                })
                .unwrap_or(0);
            let mut placeholder_tabs = Vec::new();
            let mut remote_pane_ids = Vec::new();

            for tab in snapshot_tabs {
                let snapshot_panes = snapshot
                    .panes
                    .iter()
                    .filter(|pane| pane.tab_id == tab.tab_id)
                    .collect::<Vec<_>>();
                let mut pane_terminals = Vec::new();
                let mut tab_projected_pane_ids = Vec::new();
                let mut tab_remote_pane_ids = Vec::new();
                let mut focused_pane_idx = None;

                for pane in snapshot_panes {
                    let projection_key = format!("{}\0{}", instance.id, pane.pane_id);
                    let terminal_id = projected_terminal_ids
                        .entry(projection_key.clone())
                        .or_insert_with(crate::terminal::TerminalId::alloc)
                        .clone();
                    let projected_pane_id = *projected_pane_ids
                        .entry(projection_key)
                        .or_insert_with(crate::layout::PaneId::alloc);
                    let agent = snapshot
                        .agents
                        .iter()
                        .find(|agent| agent.pane_id == pane.pane_id);
                    let (pane_state, seen) = if stale {
                        (crate::detect::AgentState::Unknown, false)
                    } else {
                        native_agent_state(
                            agent.map_or(pane.agent_status, |agent| agent.agent_status),
                        )
                    };
                    let mut terminal = crate::terminal::TerminalState::new(
                        terminal_id.clone(),
                        pane.foreground_cwd
                            .as_deref()
                            .or(pane.cwd.as_deref())
                            .unwrap_or("/")
                            .into(),
                    );
                    let detected_agent = pane
                        .agent
                        .as_deref()
                        .or(pane.display_agent.as_deref())
                        .and_then(crate::detect::parse_agent_label);
                    terminal.set_detected_state_with_screen_signals_at(
                        detected_agent,
                        pane_state,
                        false,
                        false,
                        false,
                        false,
                        Instant::now(),
                    );
                    if let Some(agent) = agent {
                        terminal.last_agent_state_change_seq = Some(agent.state_change_seq);
                        if let Some(name) = &agent.name {
                            terminal.set_agent_name(name.clone());
                        }
                    }
                    let _ = terminal.set_agent_metadata(crate::terminal::AgentMetadataReport {
                        source: "fleet-snapshot".to_string(),
                        agent_label: pane.agent.clone(),
                        applies_to_source: None,
                        title: agent
                            .and_then(|agent| agent.title.clone())
                            .or_else(|| pane.title.clone()),
                        display_agent: agent
                            .and_then(|agent| agent.display_agent.clone())
                            .or_else(|| pane.display_agent.clone()),
                        state_labels: agent
                            .map(|agent| agent.state_labels.clone())
                            .unwrap_or_else(|| pane.state_labels.clone()),
                        clear_title: false,
                        clear_display_agent: false,
                        clear_state_labels: false,
                        ttl: None,
                        seq: Some(agent.map_or(pane.revision, |agent| agent.revision)),
                    });
                    if let Some(label) = pane.label.clone() {
                        terminal.set_manual_label(label);
                    }
                    terminal.set_terminal_title(pane.terminal_title.clone());
                    patch_metadata_tokens(&mut terminal.metadata_tokens, &pane.tokens);
                    if pane.focused
                        || snapshot.focused_pane_id.as_deref() == Some(pane.pane_id.as_str())
                    {
                        focused_pane_idx = Some(pane_terminals.len());
                    }
                    pane_terminals.push((terminal_id.clone(), seen));
                    terminals.push((terminal_id, terminal));
                    tab_projected_pane_ids.push(projected_pane_id);
                    tab_remote_pane_ids.push(pane.pane_id.clone());
                }

                placeholder_tabs.push(crate::workspace::SidebarPlaceholderTab {
                    label: Some(tab.label.clone()),
                    number: tab.number,
                    pane_terminals,
                    pane_ids: tab_projected_pane_ids,
                    focused_pane_idx,
                });
                remote_pane_ids.push(tab_remote_pane_ids);
            }

            if placeholder_tabs.is_empty() {
                let projection_key =
                    format!("{}\0{}\0empty", instance.id, workspace_info.workspace_id);
                let terminal_id = projected_terminal_ids
                    .entry(projection_key.clone())
                    .or_insert_with(crate::terminal::TerminalId::alloc)
                    .clone();
                let pane_id = *projected_pane_ids
                    .entry(projection_key)
                    .or_insert_with(crate::layout::PaneId::alloc);
                let (workspace_state, seen) = if stale {
                    (crate::detect::AgentState::Unknown, false)
                } else {
                    native_agent_state(workspace_info.agent_status)
                };
                let mut terminal =
                    crate::terminal::TerminalState::new(terminal_id.clone(), "/".into());
                terminal.state = workspace_state;
                placeholder_tabs.push(crate::workspace::SidebarPlaceholderTab {
                    label: None,
                    number: 1,
                    pane_terminals: vec![(terminal_id.clone(), seen)],
                    pane_ids: vec![pane_id],
                    focused_pane_idx: Some(0),
                });
                remote_pane_ids.push(Vec::new());
                terminals.push((terminal_id, terminal));
            }

            let label = if stale {
                format!("{} · {} · offline", instance.name, workspace_info.label)
            } else {
                format!("{} · {}", instance.name, workspace_info.label)
            };
            let (mut workspace, tab_pane_ids) =
                crate::workspace::Workspace::sidebar_placeholder_with_tabs(
                    format!("fleet:{}:{}", instance.id, workspace_info.workspace_id),
                    label,
                    workspace_info.tokens.get("branch").cloned(),
                    placeholder_tabs,
                    active_tab_idx,
                );
            if let Some(worktree) = &workspace_info.worktree {
                workspace.worktree_space = Some(crate::workspace::WorktreeSpaceMembership {
                    key: format!("fleet:{}:{}", instance.id, worktree.repo_key),
                    label: worktree.repo_name.clone(),
                    repo_root: worktree.repo_root.clone().into(),
                    checkout_path: worktree.checkout_path.clone().into(),
                    is_linked_worktree: worktree.is_linked_worktree,
                });
            }
            let mut workspace_tokens = workspace_info.tokens.clone();
            workspace_tokens.insert("host".to_string(), instance.name.clone());
            workspace_tokens.insert(
                "connection".to_string(),
                if stale { "offline" } else { "connected" }.to_string(),
            );
            patch_metadata_tokens(&mut workspace.metadata_tokens, &workspace_tokens);

            let ws_idx = app.workspaces.len();
            for (terminal_id, terminal) in terminals {
                app.terminals.insert(terminal_id, terminal);
            }
            workspace_routes.insert(
                ws_idx,
                SidebarHit::Workspace {
                    index: instance_idx,
                    workspace_id: workspace_info.workspace_id.clone(),
                },
            );
            for (tab_idx, tab) in workspace.tabs.iter().enumerate() {
                if let Some(tab_info) = snapshot.tabs.iter().find(|tab_info| {
                    tab_info.workspace_id == workspace_info.workspace_id
                        && tab_info.number == tab.number
                }) {
                    tab_routes.insert(
                        (ws_idx, tab_idx),
                        SidebarHit::Tab {
                            index: instance_idx,
                            tab_id: tab_info.tab_id.clone(),
                        },
                    );
                }
            }
            for (pane_ids, targets) in tab_pane_ids.iter().zip(remote_pane_ids) {
                for (pane_id, target) in pane_ids.iter().zip(targets) {
                    pane_routes.insert(
                        (ws_idx, *pane_id),
                        SidebarHit::Pane {
                            index: instance_idx,
                            pane_id: target,
                        },
                    );
                }
            }

            app.workspaces.push(workspace);

            if instance_idx == active_instance
                && (workspace_info.focused
                    || snapshot.focused_workspace_id.as_deref()
                        == Some(workspace_info.workspace_id.as_str()))
            {
                active_workspace = Some(ws_idx);
            } else if instance_idx == active_instance && active_workspace.is_none() {
                active_workspace = Some(ws_idx);
            }
        }
    }

    app.active = active_workspace.or_else(|| (!app.workspaces.is_empty()).then_some(0));
    app.selected = app.active.unwrap_or(0);

    (app, workspace_routes, tab_routes, pane_routes)
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
    content_rect: Rect,
) -> FrameData {
    let blank = CellData {
        symbol: " ".to_string(),
        fg: 0,
        bg: 0,
        modifier: 0,
        skip: false,
        hyperlink: None,
    };
    let mut cells = vec![blank; width as usize * height as usize];

    let mut cursor = None;
    let mut hyperlinks = Vec::new();
    if let Some(content) = content {
        for row in 0..content_rect.height.min(content.height) {
            for col in 0..content_rect.width.min(content.width) {
                let host_row = content_rect.y.saturating_add(row);
                let host_col = content_rect.x.saturating_add(col);
                cells[host_row as usize * width as usize + host_col as usize] =
                    content.cells[row as usize * content.width as usize + col as usize].clone();
            }
        }
        cursor = content.cursor.clone().map(|mut cursor| {
            cursor.x = cursor.x.saturating_add(content_rect.x);
            cursor.y = cursor.y.saturating_add(content_rect.y);
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
            .map(|frame| {
                shift_graphics_coordinates(&frame.graphics, content_rect.x, content_rect.y)
            })
            .unwrap_or_default(),
    }
}

/// Kitty image placements use absolute cursor coordinates. Content-surface
/// servers encode those coordinates relative to column zero, so shift each
/// cursor-position command past the aggregate sidebar before blitting it.
fn shift_graphics_coordinates(graphics: &[u8], column_offset: u16, row_offset: u16) -> Vec<u8> {
    if graphics.is_empty() || (column_offset == 0 && row_offset == 0) {
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
            let row_end = index;
            if index > row_start && graphics.get(index) == Some(&b';') {
                index += 1;
                let column_start = index;
                while graphics.get(index).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                }
                if index > column_start && graphics.get(index) == Some(&b'H') {
                    shifted.extend_from_slice(&graphics[sequence_start..row_start]);
                    let row = std::str::from_utf8(&graphics[row_start..row_end])
                        .ok()
                        .and_then(|value| value.trim_end_matches(';').parse::<u16>().ok())
                        .unwrap_or(1);
                    shifted
                        .extend_from_slice(row.saturating_add(row_offset).to_string().as_bytes());
                    shifted.push(b';');
                    let column = std::str::from_utf8(&graphics[column_start..index])
                        .ok()
                        .and_then(|value| value.parse::<u16>().ok())
                        .unwrap_or(1);
                    shifted.extend_from_slice(
                        column.saturating_add(column_offset).to_string().as_bytes(),
                    );
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
