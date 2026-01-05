//! Network diagnostics commands.

use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Subcommand;
use comfy_table::{presets::UTF8_FULL, Table};

use crate::output::{format_number, OutputFormat};
use crate::utils::spinner;
use crate::Context;

use rpc_client::{RpcConfig, RpcClient};
use tape_core::types::NetworkAddress;

#[derive(Subcommand, Debug)]
pub enum NetworkCommand {
    /// List committee peers.
    Peers {
        /// Show all nodes, not just committee members.
        #[arg(long)]
        all: bool,
    },

    /// Ping a storage node.
    Ping {
        /// Node URL (e.g., http://node.example.com:8080).
        node: String,

        /// Number of pings.
        #[arg(short, long, default_value = "4")]
        count: u32,

        /// Timeout in seconds.
        #[arg(long, default_value = "5")]
        timeout: u64,
    },

    /// Network overview.
    Status,
}

pub async fn execute(ctx: &Context, cmd: NetworkCommand) -> Result<()> {
    match cmd {
        NetworkCommand::Peers { all } => list_peers(ctx, all).await,
        NetworkCommand::Ping { node, count, timeout } => ping_node(ctx, &node, count, timeout).await,
        NetworkCommand::Status => show_status(ctx).await,
    }
}

async fn list_peers(ctx: &Context, show_all: bool) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let pb = spinner("Fetching nodes from chain...");

    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    let client = RpcClient::new(config)?;

    // Fetch all nodes from chain
    let nodes = client.get_all_nodes().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch nodes: {}", e))?;

    // Fetch current epoch info
    let epoch = client.get_epoch().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch epoch: {}", e))?;

    pb.finish_and_clear();

    if nodes.is_empty() {
        ctx.print("No nodes registered on chain.");
        return Ok(());
    }

    // Filter to active nodes if not showing all
    let filtered_nodes: Vec<_> = if show_all {
        nodes
    } else {
        // Show only nodes that have been active recently
        nodes.into_iter()
            .filter(|(_, node)| {
                // Consider a node "active" if it was updated in the last 2 epochs
                let epoch_diff = epoch.id.as_u64().saturating_sub(node.latest_epoch.as_u64());
                epoch_diff <= 2
            })
            .collect()
    };

    match ctx.output {
        OutputFormat::Json => {
            let json_nodes: Vec<_> = filtered_nodes.iter().map(|(pubkey, node)| {
                let name = String::from_utf8_lossy(&node.metadata.name)
                    .trim_end_matches('\0')
                    .to_string();
                let address = format_network_address(&node.metadata.network_address);

                serde_json::json!({
                    "pubkey": pubkey.to_string(),
                    "id": node.id.as_u64(),
                    "name": name,
                    "network_address": address,
                    "registered_epoch": node.registered_epoch.as_u64(),
                    "latest_epoch": node.latest_epoch.as_u64(),
                    "storage_capacity": node.preferences.storage_capacity.as_u64(),
                })
            }).collect();
            println!("{}", serde_json::to_string_pretty(&json_nodes)?);
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL);
            table.set_header(vec!["ID", "Name", "Address", "Capacity", "Last Epoch"]);

            for (_pubkey, node) in &filtered_nodes {
                let name = String::from_utf8_lossy(&node.metadata.name)
                    .trim_end_matches('\0')
                    .to_string();
                let address = format_network_address(&node.metadata.network_address);

                table.add_row(vec![
                    &node.id.as_u64().to_string(),
                    &if name.is_empty() { "(unnamed)".to_string() } else { name },
                    &address,
                    &format!("{} MB", node.preferences.storage_capacity.as_u64()),
                    &node.latest_epoch.as_u64().to_string(),
                ]);
            }

            println!("{}", table);
            println!("\nTotal: {} nodes", filtered_nodes.len());
            if !show_all {
                println!("(Use --all to show all registered nodes)");
            }
        }
    }

    Ok(())
}

async fn ping_node(ctx: &Context, node_url: &str, count: u32, timeout: u64) -> Result<()> {
    // Ensure URL has scheme
    let url = if node_url.starts_with("http://") || node_url.starts_with("https://") {
        node_url.to_string()
    } else {
        format!("http://{}", node_url)
    };

    // Append /health if not already present
    let health_url = if url.ends_with("/health") {
        url.clone()
    } else {
        format!("{}/health", url.trim_end_matches('/'))
    };

    ctx.print(&format!("Pinging {} ({} attempts)...", health_url, count));
    println!();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(timeout))
        .build()?;

    let mut successes = 0u32;
    let mut failures = 0u32;
    let mut latencies = Vec::new();

    for i in 1..=count {
        let start = Instant::now();
        let result = client.get(&health_url).send().await;
        let elapsed = start.elapsed();

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    latencies.push(ms);
                    successes += 1;
                    println!(
                        "  [{}/{}] OK - {:.2}ms (HTTP {})",
                        i, count, ms, response.status()
                    );
                } else {
                    failures += 1;
                    println!(
                        "  [{}/{}] FAIL - HTTP {}",
                        i, count, response.status()
                    );
                }
            }
            Err(e) => {
                failures += 1;
                if e.is_timeout() {
                    println!("  [{}/{}] TIMEOUT after {}s", i, count, timeout);
                } else if e.is_connect() {
                    println!("  [{}/{}] CONNECTION FAILED", i, count);
                } else {
                    println!("  [{}/{}] ERROR: {}", i, count, e);
                }
            }
        }

        // Small delay between pings
        if i < count {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    println!();

    // Calculate statistics
    let stats = if !latencies.is_empty() {
        let min = latencies.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = latencies.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg = latencies.iter().sum::<f64>() / latencies.len() as f64;

        // Calculate stddev
        let variance = latencies.iter()
            .map(|x| (x - avg).powi(2))
            .sum::<f64>() / latencies.len() as f64;
        let stddev = variance.sqrt();

        Some((min, avg, max, stddev))
    } else {
        None
    };

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "url": health_url,
                "count": count,
                "successes": successes,
                "failures": failures,
                "latency_ms": stats.map(|(min, avg, max, stddev)| {
                    serde_json::json!({
                        "min": min,
                        "avg": avg,
                        "max": max,
                        "stddev": stddev,
                    })
                }),
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("--- {} ping statistics ---", health_url);
            println!(
                "{} requests transmitted, {} received, {:.1}% packet loss",
                count,
                successes,
                (failures as f64 / count as f64) * 100.0
            );

            if let Some((min, avg, max, stddev)) = stats {
                println!(
                    "rtt min/avg/max/stddev = {:.2}/{:.2}/{:.2}/{:.2} ms",
                    min, avg, max, stddev
                );
            }
        }
    }

    // Return error if all pings failed
    if successes == 0 {
        anyhow::bail!("All ping attempts failed");
    }

    Ok(())
}

async fn show_status(ctx: &Context) -> Result<()> {
    ctx.debug(&format!("Using RPC: {}", ctx.rpc_url()));

    let pb = spinner("Fetching network status...");

    let config = RpcConfig {
        endpoints: vec![ctx.rpc_url()],
        ..Default::default()
    };
    let client = RpcClient::new(config)?;

    // Fetch system state
    let system = client.get_system().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch system: {}", e))?;

    // Fetch epoch state
    let epoch = client.get_epoch().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch epoch: {}", e))?;

    // Fetch archive state
    let archive = client.get_archive().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch archive: {}", e))?;

    // Fetch all nodes to count
    let nodes = client.get_all_nodes().await
        .map_err(|e| anyhow::anyhow!("Failed to fetch nodes: {}", e))?;

    // Count active nodes (updated in last 2 epochs)
    let active_nodes = nodes.iter()
        .filter(|(_, node)| {
            let epoch_diff = epoch.id.as_u64().saturating_sub(node.latest_epoch.as_u64());
            epoch_diff <= 2
        })
        .count();

    pb.finish_and_clear();

    match ctx.output {
        OutputFormat::Json => {
            let json = serde_json::json!({
                "epoch": {
                    "current": epoch.id.as_u64(),
                    "state": format!("{:?}", epoch.state),
                    "last_epoch_timestamp": epoch.last_epoch,
                },
                "system": {
                    "version": system.version.as_u64(),
                    "total_nodes": system.total_nodes,
                },
                "archive": {
                    "storage_capacity_mb": archive.storage_capacity.as_u64(),
                    "storage_price": archive.storage_price.as_u64(),
                    "tape_count": archive.tape_count,
                    "recent_usage_mb": archive.recent_usage.as_u64(),
                    "rewards_pool": archive.rewards_pool.as_u64(),
                },
                "nodes": {
                    "total": nodes.len(),
                    "active": active_nodes,
                },
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        _ => {
            println!("Tapedrive Network Status");
            println!("========================");
            println!();

            println!("Epoch Information:");
            println!("  Current Epoch:  {}", epoch.id.as_u64());
            println!("  Epoch State:    {:?}", epoch.state);
            if epoch.last_epoch > 0 {
                let timestamp = chrono::DateTime::from_timestamp(epoch.last_epoch, 0)
                    .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                    .unwrap_or_else(|| epoch.last_epoch.to_string());
                println!("  Last Epoch:     {}", timestamp);
            }
            println!();

            println!("Archive Information:");
            println!("  Storage Capacity:  {} MB", format_number(archive.storage_capacity.as_u64()));
            println!("  Storage Price:     {} flux/MB", archive.storage_price.as_u64());
            println!("  Total Tapes:       {}", format_number(archive.tape_count));
            println!("  Recent Usage:      {} MB", format_number(archive.recent_usage.as_u64()));
            println!();

            println!("Node Information:");
            println!("  Registered Nodes:  {}", nodes.len());
            println!("  Active Nodes:      {}", active_nodes);
            println!();

            println!("RPC Endpoint: {}", ctx.rpc_url());
        }
    }

    Ok(())
}

/// Format a NetworkAddress for display.
fn format_network_address(addr: &NetworkAddress) -> String {
    // NetworkAddress is a compact representation of IP:port
    // Try to convert it to a human-readable form
    let bytes = addr.as_bytes();

    // Check if it's an IPv4 address (bytes 0-3 are the IP, rest might be port)
    if bytes.len() >= 6 {
        // Try IPv4 format: first 4 bytes are IP, next 2 are port (big endian)
        let ip = format!("{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]);
        let port = u16::from_be_bytes([bytes[4], bytes[5]]);

        if port > 0 {
            return format!("{}:{}", ip, port);
        }
        return ip;
    }

    // Fallback to hex representation
    format!("0x{}", hex::encode(bytes))
}
