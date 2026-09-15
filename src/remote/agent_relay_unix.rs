use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const RELAY_DIR: &str = "ssh-agent";
const ENABLED_MARKER: &str = "enabled";
const DISABLED_MARKER: &str = "disabled";
const SERVER_ENV_MARKER: &str = "server-env-v1";
const AGENT_SOCKET: &str = "agent.sock";
const CONTROL_SOCKET: &str = "control.sock";
const SOCKET_MODE: u32 = 0o600;
const DIRECTORY_MODE: u32 = 0o700;
const ACCEPT_POLL: Duration = Duration::from_millis(50);
const RELAY_STOP_TIMEOUT: Duration = Duration::from_secs(1);
const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PROVIDER_PATH_BYTES: usize = 4096;

#[derive(Clone)]
struct Provider {
    id: u64,
    socket: PathBuf,
}

pub(crate) fn prepare_relay() -> io::Result<()> {
    let dir = ensure_private_relay_dir()?;
    remove_private_marker(&dir.join(DISABLED_MARKER))?;
    let marker = dir.join(ENABLED_MARKER);
    let marker_result = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(SOCKET_MODE)
        .open(&marker)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => validate_marker(&marker),
        Err(error) => Err(error),
    };
    marker_result?;
    if UnixStream::connect(control_socket_path()).is_ok() {
        return Ok(());
    }

    let exe = std::env::current_exe()?;
    let mut command = Command::new(exe);
    if let Some(session) = crate::session::active_name() {
        command.arg("--session").arg(session);
    }
    command
        .arg("remote-agent-relay-daemon")
        .env_remove("SSH_AUTH_SOCK")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    crate::platform::detach_server_daemon_command(&mut command);
    let _ = crate::platform::launch_server_daemon_command(&mut command)?;

    let deadline = Instant::now() + PROVIDER_CONNECT_TIMEOUT;
    let mut delay = Duration::from_millis(25);
    loop {
        match UnixStream::connect(control_socket_path()) {
            Ok(_) => return Ok(()),
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(Duration::from_millis(250));
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("remote stable SSH-agent endpoint unavailable: {error}"),
                ));
            }
        }
    }
}

pub(crate) fn disable_relay() -> io::Result<()> {
    let dir = ensure_private_relay_dir()?;
    remove_private_marker(&dir.join(ENABLED_MARKER))?;
    remove_private_marker(&dir.join(SERVER_ENV_MARKER))?;
    create_private_marker(&dir.join(DISABLED_MARKER))?;
    let deadline = Instant::now() + RELAY_STOP_TIMEOUT;
    while UnixStream::connect(control_socket_path()).is_ok() {
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "remote SSH-agent relay did not stop promptly",
            ));
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

pub(crate) fn print_server_migration_status() -> io::Result<()> {
    let migration_needed = !private_marker_exists(SERVER_ENV_MARKER)?;
    println!(
        "{}",
        if migration_needed {
            "needed"
        } else {
            "complete"
        }
    );
    Ok(())
}

pub(crate) fn mark_server_migrated() -> io::Result<()> {
    let dir = ensure_private_relay_dir()?;
    validate_marker(&dir.join(ENABLED_MARKER))?;
    create_private_marker(&dir.join(SERVER_ENV_MARKER))
}

pub(crate) fn run_relay_daemon() -> io::Result<()> {
    let dir = ensure_private_relay_dir()?;
    validate_marker(&dir.join(ENABLED_MARKER))?;
    let agent_socket = dir.join(AGENT_SOCKET);
    let control_socket = dir.join(CONTROL_SOCKET);
    prepare_owned_socket_path(&agent_socket)?;
    prepare_owned_socket_path(&control_socket)?;
    let agent_listener = UnixListener::bind(&agent_socket)?;
    restrict_socket(&agent_socket)?;
    let agent_identity = crate::ipc::socket_file_identity(&agent_socket)?;
    let control_listener = match UnixListener::bind(&control_socket) {
        Ok(listener) => listener,
        Err(error) => {
            let _ = crate::ipc::remove_socket_file_if_owned(&agent_socket, &agent_identity);
            return Err(error);
        }
    };
    if let Err(error) = restrict_socket(&control_socket) {
        let _ = crate::ipc::remove_socket_file_if_owned(&agent_socket, &agent_identity);
        let _ = fs::remove_file(&control_socket);
        return Err(error);
    }
    let control_identity = crate::ipc::socket_file_identity(&control_socket)?;
    agent_listener.set_nonblocking(true)?;
    control_listener.set_nonblocking(true)?;
    tracing::info!("stable remote SSH-agent relay is ready");
    run_relay(
        agent_listener,
        control_listener,
        Arc::new(AtomicBool::new(false)),
    );
    let _ = crate::ipc::remove_socket_file_if_owned(&agent_socket, &agent_identity);
    let _ = crate::ipc::remove_socket_file_if_owned(&control_socket, &control_identity);
    Ok(())
}

pub(crate) fn activate_server_environment() -> io::Result<()> {
    if relay_enabled()? {
        std::env::set_var("SSH_AUTH_SOCK", stable_agent_socket_path());
    } else if relay_explicitly_disabled()? {
        std::env::remove_var("SSH_AUTH_SOCK");
    }
    Ok(())
}

pub(crate) fn apply_pane_environment(command: &mut portable_pty::CommandBuilder) {
    if relay_enabled().unwrap_or(false) {
        command.env("SSH_AUTH_SOCK", stable_agent_socket_path());
    } else if relay_explicitly_disabled().unwrap_or(false) {
        command.env_remove("SSH_AUTH_SOCK");
    }
}

pub(crate) fn run_provider() -> io::Result<()> {
    let forwarded = std::env::var_os("SSH_AUTH_SOCK")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::other("agent forwarding enabled but no forwarded SSH agent was supplied")
        })?;
    let forwarded = validate_provider_socket(&forwarded)?;
    prepare_relay()?;

    let deadline = Instant::now() + PROVIDER_CONNECT_TIMEOUT;
    let mut delay = Duration::from_millis(50);
    let mut control = loop {
        match UnixStream::connect(control_socket_path()) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(delay);
                delay = delay.saturating_mul(2).min(Duration::from_millis(500));
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("remote stable SSH-agent endpoint unavailable: {error}"),
                ));
            }
        }
    };

    let path = forwarded.to_str().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "forwarded SSH agent path is not UTF-8",
        )
    })?;
    if path.len() > MAX_PROVIDER_PATH_BYTES || path.contains('\n') || path.contains('\r') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "forwarded SSH agent path is invalid",
        ));
    }
    control.write_all(path.as_bytes())?;
    control.write_all(b"\n")?;
    control.flush()?;
    let mut acknowledged = [0_u8; 1];
    control.read_exact(&mut acknowledged)?;
    if acknowledged[0] != 1 {
        return Err(io::Error::other(
            "remote stable SSH-agent endpoint rejected the provider",
        ));
    }

    io::stdout().write_all(b"ready\n")?;
    io::stdout().flush()?;
    let mut buffer = [0_u8; 1];
    loop {
        match control.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

pub(crate) fn stable_agent_socket_path() -> PathBuf {
    relay_dir().join(AGENT_SOCKET)
}

fn relay_dir() -> PathBuf {
    crate::session::data_dir().join(RELAY_DIR)
}

fn control_socket_path() -> PathBuf {
    relay_dir().join(CONTROL_SOCKET)
}

fn relay_enabled() -> io::Result<bool> {
    private_marker_exists(ENABLED_MARKER)
}

fn relay_explicitly_disabled() -> io::Result<bool> {
    private_marker_exists(DISABLED_MARKER)
}

fn private_marker_exists(name: &str) -> io::Result<bool> {
    let dir = relay_dir();
    match fs::symlink_metadata(&dir) {
        Ok(metadata) => validate_private_relay_dir(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    }
    let marker = dir.join(name);
    match fs::symlink_metadata(&marker) {
        Ok(_) => validate_marker(&marker).map(|()| true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn create_private_marker(marker: &Path) -> io::Result<()> {
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(SOCKET_MODE)
        .open(marker)
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => validate_marker(marker),
        Err(error) => Err(error),
    }
}

fn remove_private_marker(marker: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(marker) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    validate_marker(marker)?;
    let expected = (metadata.dev(), metadata.ino());
    let current = fs::symlink_metadata(marker)?;
    if (current.dev(), current.ino()) != expected {
        return Err(io::Error::other(
            "remote SSH-agent marker changed while checking it",
        ));
    }
    fs::remove_file(marker)
}

fn ensure_private_relay_dir() -> io::Result<PathBuf> {
    let dir = relay_dir();
    let parent = dir.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "relay directory has no parent")
    })?;
    fs::create_dir_all(parent)?;
    match fs::DirBuilder::new().mode(DIRECTORY_MODE).create(&dir) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(&dir)?;
    validate_private_relay_dir(&metadata)?;
    Ok(dir)
}

fn validate_private_relay_dir(metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.file_type().is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "remote SSH-agent relay directory must be an owner-only real directory",
        ));
    }
    Ok(())
}

fn validate_marker(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "remote SSH-agent relay marker is not a private owner-owned regular file",
        ));
    }
    Ok(())
}

fn restrict_socket(path: &Path) -> io::Result<()> {
    let mut permissions = fs::symlink_metadata(path)?.permissions();
    permissions.set_mode(SOCKET_MODE);
    fs::set_permissions(path, permissions)
}

fn prepare_owned_socket_path(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket() || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "refusing to replace a non-socket or non-owned SSH-agent endpoint",
        ));
    }
    if UnixStream::connect(path).is_ok() {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "remote SSH-agent relay is already running",
        ));
    }
    let expected = (metadata.dev(), metadata.ino());
    let current = fs::symlink_metadata(path)?;
    if (current.dev(), current.ino()) != expected
        || !current.file_type().is_socket()
        || current.uid() != unsafe { libc::geteuid() }
    {
        return Err(io::Error::other(
            "remote SSH-agent endpoint changed while checking it",
        ));
    }
    fs::remove_file(path)
}

fn validate_provider_socket(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "forwarded SSH agent path is not absolute",
        ));
    }
    let resolved = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(&resolved)?;
    if !metadata.file_type().is_socket() || metadata.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "forwarded SSH agent is not an owner-owned Unix socket",
        ));
    }
    Ok(resolved)
}

fn run_relay(agent: UnixListener, control: UnixListener, stop: Arc<AtomicBool>) {
    let providers = Arc::new(Mutex::new(Vec::<Provider>::new()));
    let next_provider = Arc::new(AtomicU64::new(1));
    while !stop.load(Ordering::Acquire) && relay_enabled().unwrap_or(false) {
        loop {
            match control.accept() {
                Ok((stream, _)) => {
                    if let Err(error) = stream.set_nonblocking(false) {
                        tracing::warn!(%error, "remote SSH-agent provider stream setup failed");
                        continue;
                    }
                    let providers = Arc::clone(&providers);
                    let next_provider = Arc::clone(&next_provider);
                    thread::spawn(move || register_provider(stream, providers, next_provider));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    tracing::warn!(%error, "remote SSH-agent provider listener failed");
                    break;
                }
            }
        }
        loop {
            match agent.accept() {
                Ok((stream, _)) => {
                    if let Err(error) = stream.set_nonblocking(false) {
                        tracing::warn!(%error, "remote SSH-agent client stream setup failed");
                        continue;
                    }
                    let providers = Arc::clone(&providers);
                    thread::spawn(move || proxy_agent(stream, providers));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => {
                    tracing::warn!(%error, "stable remote SSH-agent listener failed");
                    break;
                }
            }
        }
        thread::sleep(ACCEPT_POLL);
    }
}

fn register_provider(
    mut stream: UnixStream,
    providers: Arc<Mutex<Vec<Provider>>>,
    next_provider: Arc<AtomicU64>,
) {
    let path =
        match read_provider_path(&mut stream).and_then(|path| validate_provider_socket(&path)) {
            Ok(path) => path,
            Err(error) => {
                tracing::warn!(%error, "rejected remote SSH-agent provider");
                return;
            }
        };
    let id = next_provider.fetch_add(1, Ordering::Relaxed);
    providers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(Provider { id, socket: path });
    if stream.write_all(&[1]).is_err() {
        remove_provider(&providers, id);
        return;
    }
    tracing::info!(provider = id, "remote SSH-agent provider registered");
    let mut probe = [0_u8; 1];
    loop {
        match stream.read(&mut probe) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    remove_provider(&providers, id);
    tracing::info!(provider = id, "remote SSH-agent provider disconnected");
}

fn read_provider_path(stream: &mut UnixStream) -> io::Result<PathBuf> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    while bytes.len() <= MAX_PROVIDER_PATH_BYTES {
        stream.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            let path = String::from_utf8(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            return Ok(PathBuf::from(path));
        }
        bytes.push(byte[0]);
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "forwarded SSH agent path is too long",
    ))
}

fn proxy_agent(client: UnixStream, providers: Arc<Mutex<Vec<Provider>>>) {
    let candidates = providers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .rev()
        .cloned()
        .collect::<Vec<_>>();
    for provider in candidates {
        match validate_provider_socket(&provider.socket).and_then(|path| UnixStream::connect(path))
        {
            Ok(upstream) => {
                if let Err(error) = copy_bidirectional(client, upstream) {
                    tracing::debug!(%error, "remote SSH-agent relay connection closed");
                }
                return;
            }
            Err(error) => {
                tracing::warn!(provider = provider.id, %error, "remote SSH-agent provider became stale");
                remove_provider(&providers, provider.id);
            }
        }
    }
    tracing::warn!("remote stable SSH-agent endpoint has no healthy provider");
}

fn copy_bidirectional(client: UnixStream, upstream: UnixStream) -> io::Result<()> {
    let mut client_read = client.try_clone()?;
    let mut upstream_write = upstream.try_clone()?;
    let upload = thread::spawn(move || {
        let _ = io::copy(&mut client_read, &mut upstream_write);
        let _ = upstream_write.shutdown(std::net::Shutdown::Write);
    });
    let mut upstream_read = upstream;
    let mut client_write = client;
    let result = io::copy(&mut upstream_read, &mut client_write).map(|_| ());
    let _ = client_write.shutdown(std::net::Shutdown::Write);
    let _ = upload.join();
    result
}

fn remove_provider(providers: &Mutex<Vec<Provider>>, id: u64) {
    providers
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .retain(|provider| provider.id != id);
}
