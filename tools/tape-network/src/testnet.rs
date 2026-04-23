//! `tape-network testnet …` — droplet-level operations.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use tracing::info;

use crate::cloud::{self, Instance};
use crate::settings::Settings;

/// Ensure the testbed has a droplet for each node index 0..count, provisioning
/// any missing ones and waiting until they're all active with public IPv4.
/// Returns the instances in index order (not alphabetical).
///
/// Idempotent against the existing fleet: an already-present droplet named
/// `{testbed_id}-node-{i}` is reused; only missing indices trigger a create.
/// This is what makes post-crash recovery work — after `testnet destroy --node 2`,
/// a subsequent call gap-fills node-2 without touching node-0 and node-1.
pub async fn ensure_fleet(settings: &Settings, count: u32) -> Result<Vec<Instance>> {
    let provider = cloud::from_settings(settings)?;
    let existing = provider.list_instances().await?;
    let mut by_name: HashMap<String, Instance> =
        existing.into_iter().map(|i| (i.name.clone(), i)).collect();

    let mut fleet: Vec<Instance> = Vec::with_capacity(count as usize);
    let mut to_wait: Vec<String> = Vec::new();

    for i in 0..count {
        let name = format!("{}-node-{i}", settings.testbed_id);
        let instance = match by_name.remove(&name) {
            Some(inst) => inst,
            None => {
                info!(name = %name, "provisioning missing droplet");
                provider
                    .create_one(&name, &settings.testbed_id, None)
                    .await?
            }
        };
        if instance.status != "active" || instance.public_ip.is_none() {
            to_wait.push(instance.provider_id.clone());
        }
        fleet.push(instance);
    }

    if !to_wait.is_empty() {
        info!(count = to_wait.len(), "waiting for droplets to become ready");
        let ready = cloud::digitalocean::wait_until_ready(
            provider.as_ref(),
            &to_wait,
            Duration::from_secs(10 * 60),
        )
        .await?;
        let ready_map: HashMap<String, Instance> =
            ready.into_iter().map(|i| (i.provider_id.clone(), i)).collect();
        for slot in fleet.iter_mut() {
            if let Some(updated) = ready_map.get(&slot.provider_id) {
                *slot = updated.clone();
            }
        }
    }

    Ok(fleet)
}

/// Provision (or gap-fill) droplets for this testbed. If `count` is unset,
/// uses `settings.network.node_count`.
pub async fn deploy(settings: &Settings, count: Option<u32>) -> Result<Vec<Instance>> {
    let count = count.unwrap_or(settings.network.node_count);
    info!(count, testbed = %settings.testbed_id, "ensuring fleet");
    let fleet = ensure_fleet(settings, count).await?;
    for inst in &fleet {
        println!(
            "{} {} {}",
            inst.name,
            inst.public_ip.as_deref().unwrap_or("-"),
            inst.status
        );
    }
    Ok(fleet)
}

/// Delete every droplet tagged with this testbed id.
pub async fn destroy(settings: &Settings) -> Result<()> {
    let provider = cloud::from_settings(settings)?;
    let existing = provider.list_instances().await?;
    if existing.is_empty() {
        println!("no droplets found for testbed {}", settings.testbed_id);
        return Ok(());
    }
    for inst in &existing {
        info!(name = %inst.name, id = %inst.provider_id, "deleting droplet");
        provider.delete_instance(&inst.provider_id).await?;
    }
    println!("deleted {} droplets", existing.len());
    Ok(())
}

/// Delete a single droplet by index, leaving the rest of the fleet running.
/// Useful for simulating a crash: local keys remain, on-chain registration
/// persists. Resurrect with `tape-network bootstrap`, which will detect the
/// missing droplet, provision a replacement, and rebind `network_address`.
pub async fn destroy_one(settings: &Settings, node_index: usize) -> Result<()> {
    let target = resolve_droplet(settings, node_index).await?;
    let provider = cloud::from_settings(settings)?;
    info!(name = %target.name, id = %target.provider_id, "deleting droplet");
    provider.delete_instance(&target.provider_id).await?;
    println!("deleted {}", target.name);
    Ok(())
}

/// Print status of all droplets in this testbed, one per line.
pub async fn status(settings: &Settings) -> Result<()> {
    let provider = cloud::from_settings(settings)?;
    let list = provider.list_instances().await?;
    if list.is_empty() {
        println!("no droplets for testbed {}", settings.testbed_id);
        return Ok(());
    }
    for inst in list {
        println!(
            "{:<32} {:<16} {:<16} {}",
            inst.name,
            inst.public_ip.as_deref().unwrap_or("-"),
            inst.region,
            inst.status
        );
    }
    Ok(())
}

/// Stream or print the `tape-node` systemd journal for the n-th droplet.
pub async fn logs(
    settings: &Settings,
    node_index: usize,
    tail: usize,
    follow: bool,
) -> Result<()> {
    let target = resolve_droplet(settings, node_index).await?;
    let host = target
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("droplet {} has no public IP", target.name))?;
    let key = &settings.cloud.ssh_private_key_file;

    if follow {
        let args = vec![
            "journalctl".into(),
            "-u".into(),
            "tape-node".into(),
            "--no-pager".into(),
            "-f".into(),
            format!("-n{tail}"),
        ];
        crate::ssh::interactive(&settings.ssh, key, host, &args).await
    } else {
        let cmd = format!("journalctl -u tape-node --no-pager -n{tail}");
        let output = crate::ssh::exec(&settings.ssh, key, host, &cmd).await?;
        print!("{output}");
        Ok(())
    }
}

async fn resolve_droplet(settings: &Settings, node_index: usize) -> Result<Instance> {
    let provider = cloud::from_settings(settings)?;
    let mut list = provider.list_instances().await?;
    list.sort_by(|a, b| a.name.cmp(&b.name));
    list.into_iter()
        .nth(node_index)
        .ok_or_else(|| anyhow::anyhow!("no droplet at index {node_index}"))
}

/// Open an interactive ssh session to the n-th droplet (ordered by name).
pub async fn ssh(settings: &Settings, node_index: usize, extra: Vec<String>) -> Result<()> {
    let provider = cloud::from_settings(settings)?;
    let mut list = provider.list_instances().await?;
    list.sort_by(|a, b| a.name.cmp(&b.name));

    let target = list
        .get(node_index)
        .ok_or_else(|| anyhow::anyhow!("no droplet at index {node_index} (have {})", list.len()))?;
    let host = target
        .public_ip
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("droplet {} has no public IP", target.name))?;

    crate::ssh::interactive(
        &settings.ssh,
        &settings.cloud.ssh_private_key_file,
        host,
        &extra,
    )
    .await
}
