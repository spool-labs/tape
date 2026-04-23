//! Thin wrappers around the system `ssh` and `scp` commands.
//!
//! A full russh integration is overkill for a 20-node test setup: shelling out
//! to the host's ssh client picks up the user's config and agent for free, and
//! keeps this module trivial to debug.

use std::path::Path;
use std::process::Stdio;

use anyhow::{Context, Result, bail};
use tokio::process::Command;

use crate::settings::SshSettings;

fn ssh_base_args(settings: &SshSettings, private_key: &Path) -> Vec<String> {
    vec![
        "-o".into(),
        format!("ConnectTimeout={}", settings.timeout_secs),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        "-i".into(),
        private_key.display().to_string(),
    ]
}

/// Run a shell command on the remote host and return stdout.
pub async fn exec(
    settings: &SshSettings,
    private_key: &Path,
    host: &str,
    command: &str,
) -> Result<String> {
    let mut args = ssh_base_args(settings, private_key);
    args.push(format!("{}@{}", settings.user, host));
    args.push(command.to_string());

    let output = Command::new("ssh")
        .args(&args)
        .stdin(Stdio::null())
        .output()
        .await
        .with_context(|| format!("spawning ssh to {host}"))?;

    if !output.status.success() {
        bail!(
            "ssh {host} `{command}` exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// Copy a local file to the remote host.
pub async fn upload(
    settings: &SshSettings,
    private_key: &Path,
    host: &str,
    local: &Path,
    remote: &str,
) -> Result<()> {
    let mut args = ssh_base_args(settings, private_key);
    args.push(local.display().to_string());
    args.push(format!("{}@{}:{}", settings.user, host, remote));

    let status = Command::new("scp")
        .args(&args)
        .status()
        .await
        .with_context(|| format!("spawning scp to {host}"))?;
    if !status.success() {
        bail!("scp to {host} exited {status}");
    }
    Ok(())
}

/// Copy a local directory recursively to the remote host.
pub async fn upload_dir(
    settings: &SshSettings,
    private_key: &Path,
    host: &str,
    local: &Path,
    remote: &str,
) -> Result<()> {
    let mut args = ssh_base_args(settings, private_key);
    args.insert(0, "-r".into());
    args.push(local.display().to_string());
    args.push(format!("{}@{}:{}", settings.user, host, remote));

    let status = Command::new("scp")
        .args(&args)
        .status()
        .await
        .with_context(|| format!("spawning scp -r to {host}"))?;
    if !status.success() {
        bail!("scp -r to {host} exited {status}");
    }
    Ok(())
}

/// Poll ssh against the host until a trivial command succeeds. Used right
/// after droplet creation to gate further steps on sshd being up.
pub async fn wait_until_reachable(
    settings: &SshSettings,
    private_key: &Path,
    host: &str,
    timeout: std::time::Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if exec(settings, private_key, host, "true").await.is_ok() {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("ssh to {host} unreachable after {timeout:?}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// Exec interactively: inherits stdio so the user's shell is attached. Used by
/// `tape-network testnet ssh <n>`.
pub async fn interactive(
    settings: &SshSettings,
    private_key: &Path,
    host: &str,
    extra: &[String],
) -> Result<()> {
    let mut args = ssh_base_args(settings, private_key);
    args.push(format!("{}@{}", settings.user, host));
    for arg in extra {
        args.push(arg.clone());
    }
    let status = Command::new("ssh")
        .args(&args)
        .status()
        .await
        .with_context(|| format!("spawning interactive ssh to {host}"))?;
    if !status.success() {
        bail!("ssh {host} exited {status}");
    }
    Ok(())
}
