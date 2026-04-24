//! `tape-network upgrade` — rolling binary upgrade: per droplet in turn, stop
//! service → upload new binary → start service → wait for health.
//!
//! Per `docs/testnet-deployment.md` decision 10, we go one node at a time and
//! trust peers to handle transient drops gracefully; no epoch-boundary
//! coordination is required.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Result, anyhow};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::bootstrap;
use crate::cloud::{self, Instance};
use crate::settings::Settings;
use crate::ssh;

const HTTP_PORT: u16 = 80;

pub async fn run(settings: &Settings, binary: Option<PathBuf>) -> Result<()> {
    let binary = match binary {
        Some(p) => p,
        None => bootstrap::find_node_binary(settings)?,
    };
    info!(binary = %binary.display(), "using binary");

    let provider = cloud::from_settings(settings)?;
    let mut droplets = provider.list_instances().await?;
    droplets.sort_by(|a, b| a.name.cmp(&b.name));
    if droplets.is_empty() {
        warn!("no droplets in this testbed; nothing to upgrade");
        return Ok(());
    }

    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .build()?;

    for droplet in &droplets {
        upgrade_one(settings, &client, droplet, &binary).await?;
    }

    info!(count = droplets.len(), "rolling upgrade complete");
    Ok(())
}

async fn upgrade_one(
    settings: &Settings,
    client: &reqwest::Client,
    droplet: &Instance,
    binary: &Path,
) -> Result<()> {
    let host = droplet
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow!("droplet {} has no public IP", droplet.name))?;
    let key = &settings.cloud.ssh_private_key_file;
    let working = settings.network.working_dir.display();

    info!(host = %host, "stopping service");
    ssh::exec(&settings.ssh, key, host, "systemctl stop tape-node").await?;

    info!(host = %host, "uploading binary");
    ssh::upload(
        &settings.ssh,
        key,
        host,
        binary,
        &format!("{working}/bin/tape-node"),
    )
    .await?;
    ssh::exec(
        &settings.ssh,
        key,
        host,
        &format!(
            "chmod 0755 {working}/bin/tape-node && chown tape:tape {working}/bin/tape-node"
        ),
    )
    .await?;

    info!(host = %host, "starting service");
    ssh::exec(&settings.ssh, key, host, "systemctl start tape-node").await?;

    info!(host = %host, "waiting for health");
    wait_for_health(client, host, Duration::from_secs(120)).await?;
    info!(host = %host, "node healthy");
    Ok(())
}

async fn wait_for_health(
    client: &reqwest::Client,
    host: &str,
    timeout: Duration,
) -> Result<()> {
    let url = format!("http://{host}:{HTTP_PORT}/v1/health");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(resp) = client.get(&url).send().await {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("health check timed out for {host}");
        }
        sleep(Duration::from_secs(3)).await;
    }
}
