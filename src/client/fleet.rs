use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use interprocess::local_socket::traits::{Listener as _, Stream as _};
use interprocess::TryClone as _;

use crate::ipc::{LocalListener, LocalStream};
use crate::protocol::endpoint::{
    EndpointClientHello, EndpointServerWelcome, BLOB_CODEC_V1, ENDPOINT_HELLO_KIND,
    ENDPOINT_PROTOCOL_GENERATION, ENDPOINT_SNAPSHOT_KIND, ENDPOINT_WELCOME_KIND, INPUT_CODEC_V1,
    SNAPSHOT_CODEC_V1, SURFACE_CODEC_V1,
};
use crate::protocol::{
    self, ClientClipboardImageTarget, ClientMessage, ClientShellSnapshot, ClientShellWorkspace,
    PaneSurfaceFrame, PaneSurfacePatch, ServerMessage, SurfaceGraphicsAssetKey,
    SurfaceGraphicsSource, SurfaceGraphicsTarget, MAX_GRAPHICS_FRAME_SIZE,
};

const FLEET_BOOT_ID: &str = "herdr-fleet";
const CONNECTION_WORKSPACE_ID: &str = "__connection__";
const INITIAL_AUTOMATIC_ATTEMPTS: u8 = 2;
const RETRY_INITIAL: Duration = Duration::from_millis(750);
const RETRY_MAX: Duration = Duration::from_secs(30);

pub(super) fn is_enabled() -> bool {
    crate::fleet::load().enabled_instances().next().is_some()
}

pub(super) fn run() -> io::Result<()> {
    let proxy = FleetProxy::start()?;
    super::run_client_at(
        proxy.path.clone(),
        crate::server::socket_paths::client_socket_path(),
    )
}

struct FleetProxy {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    socket_identity: crate::ipc::SocketFileIdentity,
}

impl FleetProxy {
    fn start() -> io::Result<Self> {
        let path =
            std::env::temp_dir().join(format!("herdr-fleet-{}-client.sock", std::process::id()));
        crate::ipc::prepare_socket_path(&path, |path| {
            format!("fleet proxy socket busy at {}", path.display())
        })?;
        let listener = crate::ipc::bind_private_local_listener(&path)?;
        crate::ipc::restrict_socket_permissions(&path, 0o600)?;
        let socket_identity = crate::ipc::socket_file_identity(&path)?;
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = std::thread::spawn(move || {
            if let Err(error) = run_proxy(listener, thread_stop) {
                tracing::warn!(%error, "fleet proxy stopped");
            }
        });
        Ok(Self {
            path,
            stop,
            thread: Some(thread),
            socket_identity,
        })
    }
}

impl Drop for FleetProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = crate::ipc::connect_local_stream(&self.path);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = crate::ipc::remove_socket_file_if_owned(&self.path, &self.socket_identity);
    }
}

#[derive(Clone)]
struct Definition {
    id: String,
    name: String,
    target: Option<String>,
    session: Option<String>,
}

struct Endpoint {
    definition: Definition,
    bridge: Option<crate::remote::FleetRemoteBridge>,
    bridge_status: Option<crate::remote::FleetRemoteBridgeStatus>,
    socket: Option<PathBuf>,
    writer: Option<LocalStream>,
    generation: u64,
    connecting_bridge: bool,
    connecting_endpoint: bool,
    snapshot: Option<Box<ClientShellSnapshot>>,
    known_agents: Vec<crate::protocol::ClientShellAgent>,
    known_agent_order: Vec<String>,
    surface: Option<PaneSurfaceFrame>,
    error: Option<String>,
    retry_at: Instant,
    retry_delay: Duration,
    automatic_attempts_remaining: u8,
    attempt_in_progress: bool,
    live_retry: bool,
    ever_connected: bool,
}

impl Endpoint {
    fn new(definition: Definition) -> Self {
        Self {
            definition,
            bridge: None,
            bridge_status: None,
            socket: None,
            writer: None,
            generation: 0,
            connecting_bridge: false,
            connecting_endpoint: false,
            snapshot: None,
            known_agents: Vec::new(),
            known_agent_order: Vec::new(),
            surface: None,
            error: None,
            retry_at: Instant::now(),
            retry_delay: RETRY_INITIAL,
            automatic_attempts_remaining: INITIAL_AUTOMATIC_ATTEMPTS,
            attempt_in_progress: false,
            live_retry: false,
            ever_connected: false,
        }
    }

    fn begin_attempt(&mut self) -> bool {
        if self.attempt_in_progress {
            return true;
        }
        if !self.ever_connected && !self.live_retry {
            if self.automatic_attempts_remaining == 0 {
                return false;
            }
            self.automatic_attempts_remaining -= 1;
        }
        self.attempt_in_progress = true;
        true
    }

    fn enable_live_retry(&mut self) {
        self.live_retry = true;
        self.retry_at = Instant::now();
        self.retry_delay = RETRY_INITIAL;
    }

    fn waiting_for_user(&self) -> bool {
        !self.ever_connected
            && !self.live_retry
            && self.automatic_attempts_remaining == 0
            && !self.attempt_in_progress
    }

    fn fail(&mut self, error: String) {
        self.writer = None;
        self.snapshot = None;
        self.surface = None;
        self.connecting_endpoint = false;
        self.attempt_in_progress = false;
        self.error = Some(error);
        self.retry_at = Instant::now() + self.retry_delay;
        self.retry_delay = self.retry_delay.saturating_mul(2).min(RETRY_MAX);
    }

    fn connected(&mut self, writer: LocalStream) {
        self.writer = Some(writer);
        self.connecting_endpoint = false;
        self.attempt_in_progress = false;
        self.live_retry = false;
        self.ever_connected = true;
        self.error = None;
        self.retry_at = Instant::now();
        self.retry_delay = RETRY_INITIAL;
    }
}

enum Event {
    Client(ClientMessage),
    ClientClosed,
    BridgeReady {
        index: usize,
        result: Result<crate::remote::FleetRemoteBridge, String>,
    },
    EndpointReady {
        index: usize,
        generation: u64,
        result: Result<(LocalStream, LocalStream), String>,
    },
    EndpointMessage {
        index: usize,
        generation: u64,
        message: ServerMessage,
    },
    EndpointClosed {
        index: usize,
        generation: u64,
        error: String,
    },
}

struct State {
    endpoints: Vec<Endpoint>,
    active: usize,
    revision: u64,
    hello: ClientMessage,
    last_resize: Option<ClientMessage>,
    client: LocalStream,
    tx: mpsc::Sender<Event>,
    stop: Arc<AtomicBool>,
}

fn run_proxy(listener: LocalListener, stop: Arc<AtomicBool>) -> io::Result<()> {
    let mut client = listener.accept()?;
    let hello: ClientMessage = protocol::read_message(&mut client, protocol::MAX_FRAME_SIZE)
        .map_err(|error| io::Error::other(error.to_string()))?;
    let rejection = endpoint_hello_rejection(&hello);
    let welcome = if let Some((code, message)) = rejection.as_ref() {
        EndpointServerWelcome::incompatible(code, message)
    } else {
        EndpointServerWelcome::compatible(
            crate::server::client_commands::supported_client_shell_method_names()
                .iter()
                .map(|method| (*method).to_owned())
                .collect(),
        )
    };
    write_server(
        &mut client,
        &ServerMessage::EndpointControl {
            kind: ENDPOINT_WELCOME_KIND.to_owned(),
            data: serde_json::to_string(&welcome).map_err(io::Error::other)?,
        },
    )?;
    if rejection.is_some() {
        return Ok(());
    }

    let registry = crate::fleet::load();
    let mut definitions = vec![Definition {
        id: "local".to_owned(),
        name: "local".to_owned(),
        target: None,
        session: None,
    }];
    definitions.extend(registry.enabled_instances().map(|remote| Definition {
        id: remote.id.as_str().to_owned(),
        name: remote.name.clone(),
        target: Some(remote.target.clone()),
        session: remote.session.clone(),
    }));
    let mut endpoints = definitions
        .into_iter()
        .map(Endpoint::new)
        .collect::<Vec<_>>();
    endpoints[0].socket = Some(crate::server::socket_paths::client_socket_path());

    let (tx, rx) = mpsc::channel();
    let mut client_reader = client.try_clone()?;
    let client_tx = tx.clone();
    std::thread::spawn(move || loop {
        match protocol::read_message(&mut client_reader, MAX_GRAPHICS_FRAME_SIZE) {
            Ok(message) => {
                if client_tx.send(Event::Client(message)).is_err() {
                    break;
                }
            }
            Err(_) => {
                let _ = client_tx.send(Event::ClientClosed);
                break;
            }
        }
    });

    let mut state = State {
        endpoints,
        active: 0,
        revision: 0,
        hello,
        last_resize: None,
        client,
        tx,
        stop,
    };
    send_projection(&mut state)?;
    while !state.stop.load(Ordering::Acquire) {
        start_due_connections(&mut state);
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(Event::ClientClosed) => break,
            Ok(Event::Client(message)) => {
                if handle_client_message(&mut state, message)? {
                    break;
                }
            }
            Ok(Event::BridgeReady { index, result }) => {
                bridge_ready(&mut state, index, result);
                send_projection(&mut state)?;
            }
            Ok(Event::EndpointReady {
                index,
                generation,
                result,
            }) => {
                endpoint_ready(&mut state, index, generation, result);
                send_projection(&mut state)?;
            }
            Ok(Event::EndpointMessage {
                index,
                generation,
                message,
            }) => {
                if state
                    .endpoints
                    .get(index)
                    .is_some_and(|endpoint| endpoint.generation == generation)
                {
                    handle_endpoint_message(&mut state, index, message)?;
                }
            }
            Ok(Event::EndpointClosed {
                index,
                generation,
                error,
            }) => {
                if state
                    .endpoints
                    .get(index)
                    .is_some_and(|endpoint| endpoint.generation == generation)
                {
                    state.endpoints[index].fail(error);
                    send_projection(&mut state)?;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => refresh_bridge_errors(&mut state)?,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    broadcast(&mut state, &ClientMessage::Detach);
    Ok(())
}

fn endpoint_hello_rejection(hello: &ClientMessage) -> Option<(String, String)> {
    let ClientMessage::EndpointControl { kind, data } = hello else {
        return Some((
            "invalid_hello".to_owned(),
            "fleet mode requires the stable client endpoint".to_owned(),
        ));
    };
    if kind != ENDPOINT_HELLO_KIND {
        return Some((
            "invalid_hello".to_owned(),
            format!("expected endpoint hello, received {kind:?}"),
        ));
    }
    let hello: EndpointClientHello = match serde_json::from_str(data) {
        Ok(hello) => hello,
        Err(error) => {
            return Some((
                "invalid_hello".to_owned(),
                format!("invalid endpoint hello: {error}"),
            ));
        }
    };
    if hello.generation != ENDPOINT_PROTOCOL_GENERATION {
        return Some((
            "unsupported_generation".to_owned(),
            format!(
                "endpoint generation {} is unsupported; fleet supports generation {ENDPOINT_PROTOCOL_GENERATION}",
                hello.generation
            ),
        ));
    }
    (!hello.supports_required_codecs()).then(|| {
        (
            "no_common_core".to_owned(),
            "client and fleet have no compatible endpoint core codecs".to_owned(),
        )
    })
}

fn start_due_connections(state: &mut State) {
    for index in 0..state.endpoints.len() {
        let endpoint = &mut state.endpoints[index];
        if endpoint.writer.is_some()
            || endpoint.connecting_endpoint
            || Instant::now() < endpoint.retry_at
        {
            continue;
        }
        if !endpoint.begin_attempt() {
            continue;
        }
        if endpoint.socket.is_none() {
            if endpoint.connecting_bridge {
                continue;
            }
            let Some(target) = endpoint.definition.target.clone() else {
                continue;
            };
            let session = endpoint.definition.session.clone();
            let instance_id = endpoint.definition.id.clone();
            let tx = state.tx.clone();
            endpoint.connecting_bridge = true;
            std::thread::spawn(move || {
                let result = crate::remote::retry_fleet_remote_bridge(
                    &target,
                    session.as_deref(),
                    &instance_id,
                )
                .map_err(|error| error.to_string());
                let _ = tx.send(Event::BridgeReady { index, result });
            });
            continue;
        }
        let socket = endpoint.socket.clone().expect("socket checked");
        let hello = state.hello.clone();
        let tx = state.tx.clone();
        endpoint.connecting_endpoint = true;
        endpoint.generation = endpoint.generation.wrapping_add(1);
        let generation = endpoint.generation;
        std::thread::spawn(move || {
            let result = connect_endpoint(&socket, &hello);
            let _ = tx.send(Event::EndpointReady {
                index,
                generation,
                result,
            });
        });
    }
}

fn connect_endpoint(
    socket: &Path,
    hello: &ClientMessage,
) -> Result<(LocalStream, LocalStream), String> {
    let mut stream = crate::ipc::connect_local_stream(socket).map_err(|error| error.to_string())?;
    stream
        .set_recv_timeout(Some(Duration::from_secs(5)))
        .map_err(|error| error.to_string())?;
    write_client(&mut stream, hello).map_err(|error| error.to_string())?;
    let welcome: ServerMessage = protocol::read_message(&mut stream, protocol::MAX_FRAME_SIZE)
        .map_err(|error| error.to_string())?;
    stream
        .set_recv_timeout(None)
        .map_err(|error| error.to_string())?;
    let ServerMessage::EndpointControl { kind, data } = welcome else {
        return Err("endpoint did not complete the stable handshake".to_owned());
    };
    if kind != ENDPOINT_WELCOME_KIND {
        return Err(format!("expected endpoint welcome, received {kind:?}"));
    }
    let welcome: EndpointServerWelcome = serde_json::from_str(&data)
        .map_err(|error| format!("invalid endpoint welcome: {error}"))?;
    if let Some(error) = welcome.error {
        return Err(error.message);
    }
    if let Some(error) = endpoint_welcome_rejection(&welcome) {
        return Err(error);
    }
    let reader = stream.try_clone().map_err(|error| error.to_string())?;
    Ok((stream, reader))
}

fn endpoint_welcome_rejection(welcome: &EndpointServerWelcome) -> Option<String> {
    if welcome.generation != ENDPOINT_PROTOCOL_GENERATION {
        return Some(format!(
            "remote endpoint generation {}, fleet generation {ENDPOINT_PROTOCOL_GENERATION}",
            welcome.generation
        ));
    }
    let codecs = [
        (
            "snapshot",
            welcome.snapshot_codec.as_str(),
            SNAPSHOT_CODEC_V1,
        ),
        ("surface", welcome.surface_codec.as_str(), SURFACE_CODEC_V1),
        ("input", welcome.input_codec.as_str(), INPUT_CODEC_V1),
        ("blob", welcome.blob_codec.as_str(), BLOB_CODEC_V1),
    ];
    codecs
        .into_iter()
        .find(|(_, actual, expected)| actual != expected)
        .map(|(name, actual, expected)| {
            format!("remote endpoint selected unsupported {name} codec {actual:?}; expected {expected:?}")
        })
}

fn bridge_ready(
    state: &mut State,
    index: usize,
    result: Result<crate::remote::FleetRemoteBridge, String>,
) {
    let Some(endpoint) = state.endpoints.get_mut(index) else {
        return;
    };
    endpoint.connecting_bridge = false;
    match result {
        Ok(bridge) => {
            endpoint.socket = Some(bridge.client_socket().to_path_buf());
            endpoint.bridge_status = Some(bridge.status());
            endpoint.bridge = Some(bridge);
            endpoint.error = None;
            endpoint.retry_at = Instant::now();
            endpoint.retry_delay = RETRY_INITIAL;
        }
        Err(error) => endpoint.fail(error),
    }
}

fn endpoint_ready(
    state: &mut State,
    index: usize,
    generation: u64,
    result: Result<(LocalStream, LocalStream), String>,
) {
    let Some(endpoint) = state.endpoints.get_mut(index) else {
        return;
    };
    if endpoint.generation != generation {
        return;
    }
    match result {
        Ok((writer, mut reader)) => {
            endpoint.connected(writer);
            let tx = state.tx.clone();
            std::thread::spawn(move || loop {
                match protocol::read_message(&mut reader, MAX_GRAPHICS_FRAME_SIZE) {
                    Ok(message) => {
                        if tx
                            .send(Event::EndpointMessage {
                                index,
                                generation,
                                message,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Event::EndpointClosed {
                            index,
                            generation,
                            error: error.to_string(),
                        });
                        break;
                    }
                }
            });
        }
        Err(error) => endpoint.fail(error),
    }
}

fn refresh_bridge_errors(state: &mut State) -> io::Result<()> {
    let mut changed = false;
    for endpoint in &mut state.endpoints {
        let next = endpoint
            .bridge_status
            .as_ref()
            .and_then(|status| status.client_error());
        if next.is_some() && next != endpoint.error {
            endpoint.error = next;
            changed = true;
        }
    }
    if changed {
        send_projection(state)?;
    }
    Ok(())
}

fn handle_client_message(state: &mut State, message: ClientMessage) -> io::Result<bool> {
    match message {
        ClientMessage::Detach => {
            broadcast(state, &ClientMessage::Detach);
            return Ok(true);
        }
        ClientMessage::ClientShellResize { .. } => {
            state.last_resize = Some(message.clone());
            send_to_active(state, &message)?;
        }
        ClientMessage::ClientShellPaneInput { pane_id, events } => {
            if let Some((index, raw)) = split_id(state, &pane_id) {
                switch_active(state, index)?;
                send_to(
                    state,
                    index,
                    &ClientMessage::ClientShellPaneInput {
                        pane_id: raw,
                        events,
                    },
                )?;
            }
        }
        ClientMessage::ClientShellPopupInput {
            terminal_id,
            events,
        } => {
            if let Some((index, raw)) = split_id(state, &terminal_id) {
                switch_active(state, index)?;
                send_to(
                    state,
                    index,
                    &ClientMessage::ClientShellPopupInput {
                        terminal_id: raw,
                        events,
                    },
                )?;
            } else {
                send_to_active(
                    state,
                    &ClientMessage::ClientShellPopupInput {
                        terminal_id,
                        events,
                    },
                )?;
            }
        }
        ClientMessage::ClientShellEndpointRequest { request, .. } => {
            match rewrite_request(state, &request) {
                Ok((index, request)) => {
                    switch_active(state, index)?;
                    let Some(boot_id) = state.endpoints[index]
                        .snapshot
                        .as_deref()
                        .map(|snapshot| snapshot.boot_id.clone())
                    else {
                        show_error(state, index)?;
                        return Ok(false);
                    };
                    send_to(
                        state,
                        index,
                        &ClientMessage::ClientShellEndpointRequest { boot_id, request },
                    )?;
                }
                Err(index) => {
                    state.endpoints[index].enable_live_retry();
                    if state.active == index {
                        send_projection(state)?;
                    } else {
                        switch_active(state, index)?;
                    }
                    show_error(state, index)?;
                }
            }
        }
        ClientMessage::ClipboardImage {
            target,
            extension,
            data,
        } => {
            let (index, target) = match target {
                ClientClipboardImageTarget::Pane(id) => split_id(state, &id)
                    .map(|(index, raw)| (index, ClientClipboardImageTarget::Pane(raw)))
                    .unwrap_or((state.active, ClientClipboardImageTarget::Pane(id))),
                ClientClipboardImageTarget::Popup(id) => split_id(state, &id)
                    .map(|(index, raw)| (index, ClientClipboardImageTarget::Popup(raw)))
                    .unwrap_or((state.active, ClientClipboardImageTarget::Popup(id))),
                ClientClipboardImageTarget::DirectTerminal => {
                    (state.active, ClientClipboardImageTarget::DirectTerminal)
                }
            };
            send_to(
                state,
                index,
                &ClientMessage::ClipboardImage {
                    target,
                    extension,
                    data,
                },
            )?;
        }
        ClientMessage::ClientShellHostTheme { .. }
        | ClientMessage::ClientShellFocus { .. }
        | ClientMessage::ClientShellMouseCapture { .. } => broadcast(state, &message),
        ClientMessage::ClientShellHello { .. } => {}
        _ => send_to_active(state, &message)?,
    }
    Ok(false)
}

fn handle_endpoint_message(
    state: &mut State,
    index: usize,
    message: ServerMessage,
) -> io::Result<()> {
    match message {
        ServerMessage::EndpointControl { kind, data } => {
            if kind == ENDPOINT_SNAPSHOT_KIND {
                let snapshot: ClientShellSnapshot =
                    serde_json::from_str(&data).map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid endpoint snapshot: {error}"),
                        )
                    })?;
                state.endpoints[index].known_agents = snapshot.agents.clone();
                state.endpoints[index].known_agent_order = snapshot.agent_order.clone();
                state.endpoints[index].snapshot = Some(Box::new(snapshot));
                state.endpoints[index].error = None;
                send_projection(state)?;
            } else if index == state.active {
                write_server(
                    &mut state.client,
                    &ServerMessage::EndpointControl { kind, data },
                )?;
            }
        }
        ServerMessage::ClientShellSnapshot(_) => {
            state.endpoints[index].fail("endpoint sent an unnegotiated binary snapshot".to_owned());
            send_projection(state)?;
        }
        ServerMessage::PaneSurface(surface) => {
            state.endpoints[index].surface = Some(surface.clone());
            if index == state.active {
                let surface = namespace_surface(state, index, surface);
                write_server(&mut state.client, &ServerMessage::PaneSurface(surface))?;
            }
        }
        ServerMessage::PaneSurfacePatch(patch) => {
            apply_cached_patch(&mut state.endpoints[index].surface, &patch);
            if index == state.active {
                let patch = namespace_patch(state, index, patch);
                write_server(&mut state.client, &ServerMessage::PaneSurfacePatch(patch))?;
            }
        }
        ServerMessage::ClientShellEndpointResponseChunk {
            request_id,
            final_chunk,
            data,
            ..
        } => {
            write_server(
                &mut state.client,
                &ServerMessage::ClientShellEndpointResponseChunk {
                    boot_id: FLEET_BOOT_ID.to_owned(),
                    request_id,
                    final_chunk,
                    data,
                },
            )?;
        }
        ServerMessage::SemanticNotification(mut notification) => {
            let endpoint_id = &state.endpoints[index].definition.id;
            notification.workspace_id = notification
                .workspace_id
                .map(|id| namespace(endpoint_id, &id));
            notification.tab_id = notification.tab_id.map(|id| namespace(endpoint_id, &id));
            notification.pane_id = notification.pane_id.map(|id| namespace(endpoint_id, &id));
            write_server(
                &mut state.client,
                &ServerMessage::SemanticNotification(notification),
            )?;
        }
        ServerMessage::ServerShutdown { reason } => {
            state.endpoints[index].fail(reason.unwrap_or_else(|| "server shut down".to_owned()));
            send_projection(state)?;
        }
        ServerMessage::Welcome { .. } => {}
        other if index == state.active => write_server(&mut state.client, &other)?,
        _ => {}
    }
    Ok(())
}

fn send_projection(state: &mut State) -> io::Result<()> {
    state.revision = state.revision.wrapping_add(1).max(1);
    let snapshot = merged_snapshot(state);
    let message = crate::protocol::endpoint::snapshot_message(&snapshot)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    write_server(&mut state.client, &message)?;
    if let Some(surface) = state.endpoints[state.active].surface.clone() {
        let surface = namespace_surface(state, state.active, surface);
        write_server(&mut state.client, &ServerMessage::PaneSurface(surface))?;
    }
    Ok(())
}

fn merged_snapshot(state: &State) -> ClientShellSnapshot {
    let active = state.endpoints[state.active].snapshot.as_deref();
    let mut merged = active.cloned().unwrap_or_else(empty_snapshot);
    merged.boot_id = FLEET_BOOT_ID.to_owned();
    merged.revision = state.revision;
    let active_id = &state.endpoints[state.active].definition.id;
    merged.focused_workspace_id = active
        .and_then(|s| s.focused_workspace_id.as_deref())
        .map(|id| namespace(active_id, id));
    merged.focused_tab_id = active
        .and_then(|s| s.focused_tab_id.as_deref())
        .map(|id| namespace(active_id, id));
    merged.focused_pane_id = active
        .and_then(|s| s.focused_pane_id.as_deref())
        .map(|id| namespace(active_id, id));
    merged.workspaces.clear();
    merged.tabs.clear();
    merged.panes.clear();
    merged.agents.clear();
    merged.agent_order.clear();
    for (index, endpoint) in state.endpoints.iter().enumerate() {
        let endpoint_id = &endpoint.definition.id;
        if let Some(snapshot) = endpoint.snapshot.as_deref() {
            for item in &snapshot.workspaces {
                let mut item = item.clone();
                item.workspace_id = namespace(endpoint_id, &item.workspace_id);
                item.active_tab_id = namespace(endpoint_id, &item.active_tab_id);
                item.label = format!("{} · {}", endpoint.definition.name, item.label);
                item.focused = index == state.active && item.focused;
                merged.workspaces.push(item);
            }
            for item in &snapshot.tabs {
                let mut item = item.clone();
                item.tab_id = namespace(endpoint_id, &item.tab_id);
                item.workspace_id = namespace(endpoint_id, &item.workspace_id);
                item.focused = index == state.active && item.focused;
                merged.tabs.push(item);
            }
            for item in &snapshot.panes {
                let mut item = item.clone();
                item.pane_id = namespace(endpoint_id, &item.pane_id);
                item.workspace_id = namespace(endpoint_id, &item.workspace_id);
                item.tab_id = namespace(endpoint_id, &item.tab_id);
                item.focused = index == state.active && item.focused;
                merged.panes.push(item);
            }
            for item in &snapshot.agents {
                let mut item = item.clone();
                item.pane_id = namespace(endpoint_id, &item.pane_id);
                item.workspace_id = namespace(endpoint_id, &item.workspace_id);
                item.tab_id = namespace(endpoint_id, &item.tab_id);
                item.focused = index == state.active && item.focused;
                merged.agents.push(item);
            }
            merged.agent_order.extend(
                snapshot
                    .agent_order
                    .iter()
                    .map(|id| namespace(endpoint_id, id)),
            );
        } else {
            let detail = if endpoint.waiting_for_user() {
                "offline · click to retry"
            } else if endpoint.error.is_some() {
                "reconnecting"
            } else {
                "connecting"
            };
            merged.workspaces.push(ClientShellWorkspace {
                workspace_id: namespace(endpoint_id, CONNECTION_WORKSPACE_ID),
                active_tab_id: String::new(),
                new_workspace_cwd: String::new(),
                number: index + 1,
                label: format!("{} · {detail}", endpoint.definition.name),
                custom_label: true,
                branch: None,
                git_ahead_behind: None,
                tokens: Vec::new(),
                worktree: None,
                focused: index == state.active,
                agent_status: crate::api::schema::AgentStatus::Blocked,
            });
            append_disconnected_known_agents(&mut merged, endpoint);
        }
    }
    merged
}

fn append_disconnected_known_agents(snapshot: &mut ClientShellSnapshot, endpoint: &Endpoint) {
    let endpoint_id = &endpoint.definition.id;
    for item in &endpoint.known_agents {
        let mut item = item.clone();
        item.pane_id = namespace(endpoint_id, &item.pane_id);
        item.workspace_id = namespace(endpoint_id, CONNECTION_WORKSPACE_ID);
        item.tab_id = namespace(endpoint_id, &item.tab_id);
        item.focused = false;
        item.agent_status = crate::api::schema::AgentStatus::Blocked;
        snapshot.agents.push(item);
    }
    snapshot.agent_order.extend(
        endpoint
            .known_agent_order
            .iter()
            .map(|id| namespace(endpoint_id, id)),
    );
}

fn empty_snapshot() -> ClientShellSnapshot {
    ClientShellSnapshot {
        boot_id: FLEET_BOOT_ID.to_owned(),
        revision: 0,
        config_diagnostic: None,
        product_announcement: None,
        update_available: None,
        update_install_command: "herdr update".to_owned(),
        server_keybindings_toml: None,
        latest_release_notes_available: false,
        integration_updates_available: false,
        worktree_directory: String::new(),
        release_notes: None,
        focused_workspace_id: None,
        focused_tab_id: None,
        focused_pane_id: None,
        tab_bar_right: Vec::new(),
        tab_bar_right_separator: String::new(),
        agent_view_label: None,
        agent_order: Vec::new(),
        workspaces: Vec::new(),
        tabs: Vec::new(),
        panes: Vec::new(),
        agents: Vec::new(),
        commands: Vec::new(),
    }
}

fn switch_active(state: &mut State, index: usize) -> io::Result<()> {
    if index == state.active || index >= state.endpoints.len() {
        return Ok(());
    }
    state.active = index;
    send_projection(state)?;
    if state.endpoints[index].writer.is_some() {
        if let Some(resize) = state.last_resize.clone() {
            send_to(state, index, &resize)?;
        }
    }
    Ok(())
}

fn show_error(state: &mut State, index: usize) -> io::Result<()> {
    let endpoint = &state.endpoints[index];
    let message = endpoint
        .error
        .clone()
        .unwrap_or_else(|| format!("{} is still connecting", endpoint.definition.name));
    write_server(
        &mut state.client,
        &ServerMessage::ClientShellError { message },
    )
}

fn send_to_active(state: &mut State, message: &ClientMessage) -> io::Result<()> {
    send_to(state, state.active, message)
}

fn send_to(state: &mut State, index: usize, message: &ClientMessage) -> io::Result<()> {
    let result = state
        .endpoints
        .get_mut(index)
        .and_then(|endpoint| endpoint.writer.as_mut())
        .map(|writer| write_client(writer, message));
    match result {
        Some(Ok(())) => Ok(()),
        Some(Err(error)) => {
            state.endpoints[index].fail(error.to_string());
            send_projection(state)
        }
        None => show_error(state, index),
    }
}

fn broadcast(state: &mut State, message: &ClientMessage) {
    for endpoint in &mut state.endpoints {
        if let Some(writer) = endpoint.writer.as_mut() {
            let _ = write_client(writer, message);
        }
    }
}

fn rewrite_request(state: &State, request: &str) -> Result<(usize, String), usize> {
    let mut value: serde_json::Value = match serde_json::from_str(request) {
        Ok(value) => value,
        Err(_) => return Ok((state.active, request.to_owned())),
    };
    let mut owner = None;
    find_owner(state, &value, &mut owner);
    let index = owner.unwrap_or(state.active);
    let endpoint_id = &state.endpoints[index].definition.id;
    if contains_string(&value, &namespace(endpoint_id, CONNECTION_WORKSPACE_ID)) {
        return Err(index);
    }
    strip_namespace(&mut value, endpoint_id);
    serde_json::to_string(&value)
        .map(|request| (index, request))
        .map_err(|_| index)
}

fn find_owner(state: &State, value: &serde_json::Value, owner: &mut Option<usize>) {
    match value {
        serde_json::Value::String(text) => {
            if let Some((index, _)) = split_id(state, text) {
                *owner = Some(index);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                find_owner(state, value, owner);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                find_owner(state, value, owner);
            }
        }
        _ => {}
    }
}

fn contains_string(value: &serde_json::Value, target: &str) -> bool {
    match value {
        serde_json::Value::String(text) => text == target,
        serde_json::Value::Array(values) => {
            values.iter().any(|value| contains_string(value, target))
        }
        serde_json::Value::Object(values) => {
            values.values().any(|value| contains_string(value, target))
        }
        _ => false,
    }
}

fn strip_namespace(value: &mut serde_json::Value, endpoint_id: &str) {
    match value {
        serde_json::Value::String(text) => {
            if let Some(raw) = text.strip_prefix(&prefix(endpoint_id)) {
                *text = raw.to_owned();
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                strip_namespace(value, endpoint_id);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                strip_namespace(value, endpoint_id);
            }
        }
        _ => {}
    }
}

fn split_id(state: &State, id: &str) -> Option<(usize, String)> {
    state
        .endpoints
        .iter()
        .enumerate()
        .find_map(|(index, endpoint)| {
            id.strip_prefix(&prefix(&endpoint.definition.id))
                .map(|raw| (index, raw.to_owned()))
        })
}

fn prefix(endpoint_id: &str) -> String {
    format!("fleet/{endpoint_id}/")
}
fn namespace(endpoint_id: &str, id: &str) -> String {
    format!("{}{id}", prefix(endpoint_id))
}

fn namespace_surface(
    state: &State,
    index: usize,
    mut surface: PaneSurfaceFrame,
) -> PaneSurfaceFrame {
    let endpoint_id = &state.endpoints[index].definition.id;
    surface.boot_id = FLEET_BOOT_ID.to_owned();
    surface.projection_revision = state.revision;
    for pane in &mut surface.panes {
        pane.pane_id = namespace(endpoint_id, &pane.pane_id);
    }
    if let Some(popup) = surface.popup.as_mut() {
        popup.terminal_id = namespace(endpoint_id, &popup.terminal_id);
    }
    for asset in &mut surface.graphics.assets {
        namespace_graphics_key(endpoint_id, &mut asset.key);
    }
    for placement in &mut surface.graphics.placements {
        namespace_graphics_key(endpoint_id, &mut placement.asset);
    }
    for key in &mut surface.graphics.retained_assets {
        namespace_graphics_key(endpoint_id, key);
    }
    surface
}

fn namespace_patch(state: &State, index: usize, mut patch: PaneSurfacePatch) -> PaneSurfacePatch {
    let endpoint_id = &state.endpoints[index].definition.id;
    patch.boot_id = FLEET_BOOT_ID.to_owned();
    patch.projection_revision = state.revision;
    for pane in &mut patch.panes {
        pane.pane_id = namespace(endpoint_id, &pane.pane_id);
    }
    patch
}

fn namespace_graphics_key(endpoint_id: &str, key: &mut SurfaceGraphicsAssetKey) {
    match &mut key.source {
        SurfaceGraphicsSource::Terminal { target, .. } => match target {
            SurfaceGraphicsTarget::Pane { pane_id } => *pane_id = namespace(endpoint_id, pane_id),
            SurfaceGraphicsTarget::Popup { terminal_id } => {
                *terminal_id = namespace(endpoint_id, terminal_id)
            }
        },
        SurfaceGraphicsSource::PaneLayer { pane_id, .. } => {
            *pane_id = namespace(endpoint_id, pane_id)
        }
    }
}

fn apply_cached_patch(surface: &mut Option<PaneSurfaceFrame>, patch: &PaneSurfacePatch) {
    let Some(surface) = surface.as_mut() else {
        return;
    };
    if surface.boot_id != patch.boot_id
        || surface.projection_revision != patch.projection_revision
        || surface.surface_revision != patch.base_surface_revision
    {
        return;
    }
    for row in &patch.rows {
        let start = usize::from(row.y) * usize::from(surface.frame.width) + usize::from(row.x);
        let end = start.saturating_add(row.cells.len());
        if end <= surface.frame.cells.len() {
            surface.frame.cells[start..end].clone_from_slice(&row.cells);
        }
    }
    for updated in &patch.panes {
        if let Some(existing) = surface
            .panes
            .iter_mut()
            .find(|pane| pane.pane_id == updated.pane_id)
        {
            *existing = updated.clone();
        }
    }
    surface.frame.cursor = patch.cursor.clone();
    surface.surface_revision = patch.surface_revision;
}

fn write_client(stream: &mut LocalStream, message: &ClientMessage) -> io::Result<()> {
    protocol::write_message(stream, message).map_err(|error| io::Error::other(error.to_string()))
}

fn write_server(stream: &mut LocalStream, message: &ServerMessage) -> io::Result<()> {
    protocol::write_message(stream, message).map_err(|error| io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello() -> ClientMessage {
        ClientMessage::EndpointControl {
            kind: ENDPOINT_HELLO_KIND.to_owned(),
            data: serde_json::to_string(&EndpointClientHello {
                generation: ENDPOINT_PROTOCOL_GENERATION,
                cell_width_px: 8,
                cell_height_px: 16,
                surface_size: crate::protocol::ClientSurfaceSize { cols: 80, rows: 24 },
                pixel_mouse: true,
                direct_graphics: false,
                endpoint_keybindings: true,
                mouse_capture: true,
                snapshot_codecs: vec![SNAPSHOT_CODEC_V1.to_owned()],
                surface_codecs: vec![SURFACE_CODEC_V1.to_owned()],
                input_codecs: vec![INPUT_CODEC_V1.to_owned()],
                blob_codecs: vec![BLOB_CODEC_V1.to_owned()],
            })
            .expect("endpoint hello serializes"),
        }
    }

    fn endpoint() -> Endpoint {
        Endpoint::new(Definition {
            id: "remote".to_owned(),
            name: "remote".to_owned(),
            target: Some("host".to_owned()),
            session: None,
        })
    }

    #[test]
    fn initial_connection_retries_once_then_waits_for_user() {
        let mut endpoint = endpoint();

        assert!(endpoint.begin_attempt());
        endpoint.fail("first".to_owned());
        assert!(!endpoint.waiting_for_user());

        assert!(endpoint.begin_attempt());
        endpoint.fail("second".to_owned());
        assert!(endpoint.waiting_for_user());
        assert!(!endpoint.begin_attempt());
    }

    #[test]
    fn user_retry_and_post_connection_reconnect_are_live() {
        let mut endpoint = endpoint();
        assert!(endpoint.begin_attempt());
        endpoint.fail("first".to_owned());
        assert!(endpoint.begin_attempt());
        endpoint.fail("second".to_owned());

        endpoint.enable_live_retry();
        assert!(endpoint.begin_attempt());
        endpoint.fail("manual".to_owned());
        assert!(endpoint.begin_attempt());

        endpoint.live_retry = false;
        endpoint.ever_connected = true;
        endpoint.attempt_in_progress = false;
        assert!(endpoint.begin_attempt());
    }

    #[test]
    fn stable_endpoint_handshake_requires_generation_and_core_codecs() {
        assert_eq!(endpoint_hello_rejection(&hello()), None);

        let mut invalid: EndpointClientHello = match hello() {
            ClientMessage::EndpointControl { data, .. } => {
                serde_json::from_str(&data).expect("valid endpoint hello")
            }
            _ => unreachable!(),
        };
        invalid.snapshot_codecs.clear();
        let invalid = ClientMessage::EndpointControl {
            kind: ENDPOINT_HELLO_KIND.to_owned(),
            data: serde_json::to_string(&invalid).expect("endpoint hello serializes"),
        };
        assert_eq!(
            endpoint_hello_rejection(&invalid).map(|(code, _)| code),
            Some("no_common_core".to_owned())
        );

        let mut welcome = EndpointServerWelcome::compatible(Vec::new());
        assert_eq!(endpoint_welcome_rejection(&welcome), None);
        welcome.surface_codec = "unknown".to_owned();
        assert!(endpoint_welcome_rejection(&welcome)
            .is_some_and(|error| error.contains("unsupported surface codec")));
    }

    #[test]
    fn disconnected_endpoint_keeps_known_agents_blocked() {
        let mut endpoint = endpoint();
        endpoint
            .known_agents
            .push(crate::protocol::ClientShellAgent {
                pane_id: "pane".to_owned(),
                workspace_id: "workspace".to_owned(),
                tab_id: "tab".to_owned(),
                name: Some("agent".to_owned()),
                display_agent: None,
                agent: None,
                title: None,
                terminal_title: None,
                terminal_title_stripped: None,
                agent_status: crate::api::schema::AgentStatus::Working,
                state_change_seq: 1,
                state_labels: Vec::new(),
                tokens: Vec::new(),
                focused: true,
            });
        endpoint.known_agent_order.push("pane".to_owned());
        let mut snapshot = empty_snapshot();
        append_disconnected_known_agents(&mut snapshot, &endpoint);
        let agent = snapshot.agents.first().expect("known agent retained");
        assert_eq!(agent.pane_id, "fleet/remote/pane");
        assert_eq!(
            agent.workspace_id, "fleet/remote/__connection__",
            "offline agents remain visible under the endpoint status workspace"
        );
        assert_eq!(agent.agent_status, crate::api::schema::AgentStatus::Blocked);
        assert!(!agent.focused);
        assert_eq!(snapshot.agent_order, vec!["fleet/remote/pane"]);

        endpoint.known_agents.clear();
        endpoint.known_agent_order.clear();
        let mut snapshot = empty_snapshot();
        append_disconnected_known_agents(&mut snapshot, &endpoint);
        assert!(snapshot.agents.is_empty());
    }
}
