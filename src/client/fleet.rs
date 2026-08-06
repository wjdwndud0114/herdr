use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossterm::event::{MouseButton, MouseEventKind};
use interprocess::local_socket::traits::Stream as _;
use interprocess::TryClone as _;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};

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
    let area = Rect::new(0, 0, width, height);
    let mut buffer = Buffer::empty(area);
    let background = Style::default().fg(Color::Gray).bg(Color::Rgb(24, 24, 31));
    buffer.set_style(area, background);
    let mut hits = vec![None; height as usize];
    let mut row = 0u16;
    put_line(
        &mut buffer,
        width,
        row,
        " HERDR FLEET",
        Style::default()
            .fg(Color::Cyan)
            .bg(Color::Rgb(24, 24, 31))
            .add_modifier(Modifier::BOLD),
    );
    row += 2;

    for (index, instance) in state.instances.iter().enumerate() {
        if row >= height {
            break;
        }
        let active = index == state.active;
        let online = instance.writer.is_some();
        let marker = if active { ">" } else { " " };
        let state_marker = if online { "+" } else { "-" };
        let label = format!("{marker}{state_marker} {}", instance.name);
        let style = if active {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
                .fg(if online {
                    Color::White
                } else {
                    Color::DarkGray
                })
                .bg(Color::Rgb(24, 24, 31))
                .add_modifier(Modifier::BOLD)
        };
        put_line(&mut buffer, width, row, &label, style);
        hits[row as usize] = Some(SidebarHit::Instance { index });
        row += 1;

        if let Some(snapshot) = &instance.snapshot {
            for workspace in &snapshot.workspaces {
                if row >= height {
                    break;
                }
                let label = format!(
                    "  {} {}",
                    status_marker(workspace.agent_status),
                    workspace.label
                );
                put_line(&mut buffer, width, row, &label, background);
                hits[row as usize] = Some(SidebarHit::Workspace {
                    index,
                    workspace_id: workspace.workspace_id.clone(),
                });
                row += 1;

                for agent in snapshot
                    .agents
                    .iter()
                    .filter(|agent| agent.workspace_id == workspace.workspace_id)
                {
                    if row >= height {
                        break;
                    }
                    let agent_name = agent
                        .name
                        .as_deref()
                        .or(agent.display_agent.as_deref())
                        .or(agent.agent.as_deref())
                        .unwrap_or(&agent.pane_id);
                    let label = format!("    {} {agent_name}", status_marker(agent.agent_status));
                    put_line(
                        &mut buffer,
                        width,
                        row,
                        &label,
                        Style::default()
                            .fg(status_color(agent.agent_status))
                            .bg(Color::Rgb(24, 24, 31)),
                    );
                    hits[row as usize] = Some(SidebarHit::Agent {
                        index,
                        target: agent.pane_id.clone(),
                    });
                    row += 1;
                }
            }
        } else if let Some(error) = &instance.error {
            put_line(
                &mut buffer,
                width,
                row,
                &format!("  {error}"),
                Style::default().fg(Color::Red).bg(Color::Rgb(24, 24, 31)),
            );
            row += 1;
        }
        row = row.saturating_add(1);
    }

    (
        FrameData::from_ratatui_buffer_with_hyperlinks(&buffer, None, &[]),
        hits,
    )
}

fn put_line(buffer: &mut Buffer, width: u16, row: u16, text: &str, style: Style) {
    if width == 0 || row >= buffer.area.height {
        return;
    }
    buffer.set_stringn(0, row, text, width as usize, style);
}

fn status_marker(status: AgentStatus) -> &'static str {
    match status {
        AgentStatus::Working => "*",
        AgentStatus::Blocked => "!",
        AgentStatus::Done => "+",
        AgentStatus::Idle => ".",
        AgentStatus::Unknown => "?",
    }
}

fn status_color(status: AgentStatus) -> Color {
    match status {
        AgentStatus::Working => Color::Yellow,
        AgentStatus::Blocked => Color::Red,
        AgentStatus::Done => Color::Green,
        AgentStatus::Idle => Color::Gray,
        AgentStatus::Unknown => Color::DarkGray,
    }
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
