#[cfg(unix)]
mod agent_relay_unix;
mod attach;
#[cfg(unix)]
mod host_unix;

#[cfg(unix)]
pub(crate) use agent_relay_unix::{
    activate_server_environment as activate_remote_agent_environment,
    apply_pane_environment as apply_remote_agent_pane_environment,
    disable_relay as disable_remote_agent_relay,
    mark_server_migrated as mark_remote_agent_server_migrated,
    prepare_relay as prepare_remote_agent_relay,
    print_server_migration_status as print_remote_agent_server_migration_status,
    run_provider as run_remote_agent_provider, run_relay_daemon as run_remote_agent_relay_daemon,
};
pub(crate) use attach::*;
#[cfg(unix)]
pub(crate) use host_unix::{run_remote_api_bridge, run_remote_client_bridge};

#[cfg(windows)]
pub(crate) fn run_remote_client_bridge() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote Windows hosts are not supported yet",
    ))
}

#[cfg(windows)]
pub(crate) fn run_remote_api_bridge() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote Windows hosts are not supported yet",
    ))
}

#[cfg(windows)]
pub(crate) fn prepare_remote_agent_relay() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

#[cfg(windows)]
pub(crate) fn disable_remote_agent_relay() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

#[cfg(windows)]
pub(crate) fn print_remote_agent_server_migration_status() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

#[cfg(windows)]
pub(crate) fn mark_remote_agent_server_migrated() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

#[cfg(windows)]
pub(crate) fn run_remote_agent_provider() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

#[cfg(windows)]
pub(crate) fn run_remote_agent_relay_daemon() -> std::io::Result<()> {
    Err(std::io::Error::other(
        "remote SSH-agent relays require a Unix remote host",
    ))
}

pub(crate) fn print_remote_error_hint(err: &std::io::Error, target: &str) {
    if is_remote_auth_error(err) {
        eprintln!(
            "hint: verify SSH access first with `{}`.",
            ssh_check_command(target)
        );
        eprintln!(
            "hint: if your SSH key has a passphrase, load it into ssh-agent with `ssh-add` before running `herdr --remote`."
        );
    }
}

fn is_remote_auth_error(err: &std::io::Error) -> bool {
    let message = err.to_string();
    message.contains("Permission denied")
        && (message.contains("(publickey")
            || message.contains("(keyboard-interactive")
            || message.contains("(password"))
}

fn ssh_check_command(target: &str) -> String {
    format!("ssh {}", shell_quote(target))
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '@' | '%' | '_' | '+' | '=' | ':' | ',' | '.' | '/' | '-'
                )
        })
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_auth_error_matches_ssh_auth_denied() {
        let err = std::io::Error::other(
            "remote platform detection failed: user@host: Permission denied (publickey).",
        );

        assert!(is_remote_auth_error(&err));
    }

    #[test]
    fn remote_auth_error_matches_keyboard_interactive_denied() {
        let err = std::io::Error::other(
            "remote server status failed: user@host: Permission denied (keyboard-interactive).",
        );

        assert!(is_remote_auth_error(&err));
    }

    #[test]
    fn remote_auth_error_ignores_non_auth_errors() {
        let err = std::io::Error::other("remote platform detection failed: unsupported platform");

        assert!(!is_remote_auth_error(&err));
    }

    #[test]
    fn ssh_check_command_quotes_remote_target() {
        assert_eq!(ssh_check_command("host name"), "ssh 'host name'");
    }
}
