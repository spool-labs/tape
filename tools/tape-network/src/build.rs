//! `tape-network build-linux` — compile the Linux `tape-node` binary on an
//! ephemeral (or reusable) DO droplet and pull the artifact back.
//!
//! Exists because cross-compiling from macOS against the Solana + RocksDB +
//! openssl dep tree is a world of pain; provisioning a 10-minute Linux box is
//! the pragmatic path.

use std::path::Path;
use std::process::Command as StdCommand;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tracing::info;

use crate::cloud::{self, Instance};
use crate::settings::{BuildSource, Settings};
use crate::ssh;

/// Default size for the builder droplet. 8 vCPU / 16GB is a reasonable
/// sweet-spot: beefy enough that a cold RocksDB+Solana build takes ~10 min
/// instead of 40, cheap enough that a throwaway build costs pennies.
const DEFAULT_BUILDER_SIZE: &str = "s-8vcpu-16gb";

const BUILDER_TAG_SUFFIX: &str = "-builder";
const REMOTE_SOURCE_DIR: &str = "/root/tapedrive";
const LOCAL_BINARY_REL: &str = "target/x86_64-unknown-linux-gnu/release/tape-node";

pub async fn run(
    settings: &Settings,
    keep: bool,
    size_override: Option<String>,
) -> Result<()> {
    let size = size_override.unwrap_or_else(|| DEFAULT_BUILDER_SIZE.to_string());
    let builder_tag = format!("{}{}", settings.testbed_id, BUILDER_TAG_SUFFIX);

    let source_root = match &settings.build.source {
        BuildSource::Local { path } => path.clone(),
        BuildSource::Git { .. } => {
            bail!("build-linux only supports build.source.kind = local right now");
        }
    };

    let instance = find_or_create_builder(settings, &builder_tag, &size).await?;
    let host = instance
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow!("builder droplet has no public IP"))?;

    info!(host = %host, "waiting for ssh");
    ssh::wait_until_reachable(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        Duration::from_secs(180),
    )
    .await?;

    info!("installing build deps + rust toolchain");
    install_deps(settings, host).await?;

    info!("rsyncing source");
    sync_source(settings, host, &source_root)?;

    info!("building tape-node (release, features=metrics)");
    run_build(settings, host).await?;

    info!("fetching binary back to operator workstation");
    fetch_binary(settings, host, &source_root)?;

    if keep {
        info!(id = %instance.provider_id, ip = %host, "builder droplet kept (--keep)");
    } else {
        info!(id = %instance.provider_id, "destroying builder droplet");
        let provider = cloud::from_settings(settings)?;
        provider.delete_instance(&instance.provider_id).await?;
    }

    println!(
        "tape-node binary ready at {}/{}",
        source_root.display(),
        LOCAL_BINARY_REL
    );
    Ok(())
}

async fn find_or_create_builder(
    settings: &Settings,
    builder_tag: &str,
    size: &str,
) -> Result<Instance> {
    let provider = cloud::from_settings(settings)?;
    let existing = provider.list_instances_by_tag(builder_tag).await?;
    if let Some(inst) = existing.into_iter().next() {
        info!(
            id = %inst.provider_id,
            ip = ?inst.public_ip,
            "reusing existing builder droplet"
        );
        // Still need to wait for IP if still provisioning
        if inst.public_ip.is_some() {
            return Ok(inst);
        }
        return cloud::digitalocean::wait_until_ready(
            provider.as_ref(),
            &[inst.provider_id],
            Duration::from_secs(300),
        )
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("builder droplet failed to become ready"));
    }

    info!(tag = %builder_tag, size = %size, "provisioning new builder droplet");
    let name = format!("{}{}", settings.testbed_id, BUILDER_TAG_SUFFIX);
    let created = provider
        .create_one(&name, builder_tag, Some(size))
        .await?;

    let ready = cloud::digitalocean::wait_until_ready(
        provider.as_ref(),
        &[created.provider_id.clone()],
        Duration::from_secs(600),
    )
    .await?;
    ready
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("builder droplet never became ready"))
}

async fn install_deps(settings: &Settings, host: &str) -> Result<()> {
    let script = r#"set -e
export DEBIAN_FRONTEND=noninteractive
# Most apt mirrors are fine; only update if rustup isn't already on PATH to
# save ~30s on reruns.
if ! command -v cargo >/dev/null 2>&1; then
  apt-get update -qq
  apt-get install -y -qq build-essential clang pkg-config libssl-dev curl protobuf-compiler rsync
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable >/dev/null
fi
. "$HOME/.cargo/env"
rustc --version
"#;
    ssh::exec(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        script,
    )
    .await?;
    Ok(())
}

fn sync_source(settings: &Settings, host: &str, source_root: &Path) -> Result<()> {
    // rsync via ssh. Excludes target/ and .git to keep it fast + small.
    let ssh_cmd = format!(
        "ssh -i {} -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout={}",
        settings.cloud.ssh_private_key_file.display(),
        settings.ssh.timeout_secs
    );
    let src = format!("{}/", source_root.display());
    let dst = format!("{}@{}:{}/", settings.ssh.user, host, REMOTE_SOURCE_DIR);

    let status = StdCommand::new("rsync")
        .args(["-az", "--delete"])
        .args([
            "--exclude",
            "target/",
            "--exclude",
            ".git/",
            "--exclude",
            "work/",
            "--exclude",
            "test-ledger/",
            "--exclude",
            ".codebase-index/",
            "--exclude",
            "db_tapestore*",
        ])
        .args(["-e", &ssh_cmd])
        .arg(&src)
        .arg(&dst)
        .status()
        .context("spawning rsync")?;
    if !status.success() {
        bail!("rsync exited {status}");
    }
    Ok(())
}

async fn run_build(settings: &Settings, host: &str) -> Result<()> {
    let cmd = format!(
        r#"source $HOME/.cargo/env && cd {REMOTE_SOURCE_DIR} && cargo build --release -p tape-node --features metrics 2>&1 | tail -20"#
    );
    let output = ssh::exec(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        &cmd,
    )
    .await?;
    // Print the tail so the operator sees progress / errors.
    print!("{output}");
    Ok(())
}

fn fetch_binary(settings: &Settings, host: &str, source_root: &Path) -> Result<()> {
    let local = source_root.join(LOCAL_BINARY_REL);
    if let Some(parent) = local.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let ssh_flags = vec![
        "-o".to_string(),
        format!("ConnectTimeout={}", settings.ssh.timeout_secs),
        "-o".into(),
        "StrictHostKeyChecking=accept-new".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        "-i".into(),
        settings.cloud.ssh_private_key_file.display().to_string(),
    ];
    let remote = format!(
        "{}@{}:{}/target/release/tape-node",
        settings.ssh.user, host, REMOTE_SOURCE_DIR
    );
    let status = StdCommand::new("scp")
        .args(&ssh_flags)
        .arg(&remote)
        .arg(&local)
        .status()
        .context("spawning scp")?;
    if !status.success() {
        bail!("scp exited {status}");
    }
    Ok(())
}
