//! `tape-network stats` — scrape each node's public operator stats API.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::future::join_all;
use reqwest::{Client, StatusCode};
use tape_protocol::api::{NodeStats, routes::NODE_STATS_PATH};

use crate::cloud::{self, Instance};
use crate::settings::Settings;

const NODE_HTTP_PORT: u16 = 3420;
const NODE_HEALTH_PATH: &str = "/v1/health";

pub async fn run(settings: &Settings, timeout_ms: u64, verbose: bool) -> Result<()> {
    let provider = cloud::from_settings(settings)?;
    let mut instances = provider.list_instances().await?;

    if instances.is_empty() {
        println!("no droplets for testbed {}", settings.testbed_id);
        return Ok(());
    }

    instances.sort_by_key(|inst| {
        node_index(settings, &inst.name).unwrap_or(usize::MAX)
    });

    let client = Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .build()
        .context("create stats HTTP client")?;

    let rows = join_all(instances.iter().map(|inst| scrape_node(settings, &client, inst))).await;

    print_rows(&rows, verbose);
    Ok(())
}

#[derive(Debug)]
struct NodeRow {
    idx: Option<usize>,
    name: String,
    address: Option<String>,
    health: HealthState,
    stats: Option<NodeStats>,
    latency_ms: Option<u128>,
    error: Option<String>,
}

#[derive(Debug)]
enum HealthState {
    Up,
    Down(StatusCode),
    StatsError,
    Unreachable,
    NoIp,
}

impl HealthState {
    fn label(&self) -> String {
        match self {
            Self::Up => "up".into(),
            Self::Down(code) => format!("down:{}", code.as_u16()),
            Self::StatsError => "stats-error".into(),
            Self::Unreachable => "unreachable".into(),
            Self::NoIp => "no-ip".into(),
        }
    }
}

async fn scrape_node(settings: &Settings, client: &Client, inst: &Instance) -> NodeRow {
    let idx = node_index(settings, &inst.name);
    let Some(ip) = inst.public_ip.as_deref() else {
        return NodeRow {
            idx,
            name: inst.name.clone(),
            address: None,
            health: HealthState::NoIp,
            stats: None,
            latency_ms: None,
            error: Some("droplet has no public IP".into()),
        };
    };

    let address = format!("{ip}:{NODE_HTTP_PORT}");
    let base = format!("http://{address}");
    let started = Instant::now();

    let health_fut = client.get(format!("{base}{NODE_HEALTH_PATH}")).send();
    let stats_fut = client.get(format!("{base}{NODE_STATS_PATH}")).send();
    let (health_res, stats_res) = tokio::join!(health_fut, stats_fut);
    let latency_ms = Some(started.elapsed().as_millis());

    let health = match &health_res {
        Ok(resp) if resp.status().is_success() => None,
        Ok(resp) => Some(HealthState::Down(resp.status())),
        Err(_) => Some(HealthState::Unreachable),
    };

    let mut error = None;
    let stats = match stats_res {
        Ok(resp) if resp.status().is_success() => match resp.json::<NodeStats>().await {
            Ok(stats) => Some(stats),
            Err(err) => {
                error = Some(format!("stats json: {err}"));
                None
            }
        },
        Ok(resp) => {
            error = Some(format!("stats HTTP {}", resp.status()));
            None
        }
        Err(err) => {
            error = Some(format!("stats request: {err}"));
            None
        }
    };

    let health = match (health, stats.is_some()) {
        (Some(state), _) => state,
        (None, true) => HealthState::Up,
        (None, false) => HealthState::StatsError,
    };

    NodeRow {
        idx,
        name: inst.name.clone(),
        address: Some(address),
        health,
        stats,
        latency_ms,
        error,
    }
}

fn print_rows(rows: &[NodeRow], verbose: bool) {
    if verbose {
        println!(
            "{:>3}  {:<28}  {:<21}  {:<12}  {:>5}  {:>12}  {:>6}  {:>7}  {:>7}  {:>9}  {:>9}  {:>9}  {:>8}  {:>7}  {:>10}  {:>8}  {:>9}  {:>9}  {:>6}  {}",
            "IDX",
            "NODE",
            "ADDRESS",
            "HEALTH",
            "EPOCH",
            "SLOT",
            "SPOOLS",
            "TRACKS",
            "SLICES",
            "PAYLOAD",
            "DISK",
            "FREE",
            "REQS",
            "RECLAIM",
            "BLOCKS",
            "EPOCH_TX",
            "UPLOADED",
            "DOWNLD",
            "LAT",
            "ERROR"
        );
    } else {
        println!(
            "{:>3}  {:<28}  {:<21}  {:<12}  {:>5}  {:>12}  {:>6}  {:>7}  {:>7}  {:>9}  {:>9}  {:>9}  {:>8}  {:>7}",
            "IDX",
            "NODE",
            "ADDRESS",
            "HEALTH",
            "EPOCH",
            "SLOT",
            "SPOOLS",
            "TRACKS",
            "SLICES",
            "PAYLOAD",
            "DISK",
            "FREE",
            "REQS",
            "RECLAIM"
        );
    }

    for row in rows {
        let stats = row.stats.as_ref();
        let base = format!(
            "{:>3}  {:<28}  {:<21}  {:<12}  {:>5}  {:>12}  {:>6}  {:>7}  {:>7}  {:>9}  {:>9}  {:>9}  {:>8}  {:>7}",
            fmt_idx(row.idx),
            fit(&row.name, 28),
            fit(row.address.as_deref().unwrap_or("-"), 21),
            fit(&row.health.label(), 12),
            fmt_stat(stats.map(|s| s.current_epoch)),
            fmt_stat(stats.map(|s| s.last_processed_slot)),
            fmt_stat(stats.map(|s| s.owned_spools)),
            fmt_stat(stats.map(|s| s.tracks_stored)),
            fmt_stat(stats.map(|s| s.slices_stored)),
            fmt_bytes_opt(stats.map(|s| s.slice_payload_bytes)),
            fmt_bytes_opt(stats.map(|s| s.store_disk_bytes)),
            fmt_bytes_opt(stats.and_then(|s| s.free_disk_bytes)),
            fmt_stat(stats.map(|s| s.requests_total)),
            stats
                .map(|s| if s.reclaim_pending { "yes" } else { "-" })
                .unwrap_or("-")
        );

        if verbose {
            println!(
                "{base}  {:>10}  {:>8}  {:>9}  {:>9}  {:>6}  {}",
                fmt_stat(stats.map(|s| s.blocks_processed)),
                fmt_stat(stats.map(|s| s.epoch_transitions)),
                fmt_bytes_opt(stats.map(|s| s.bytes_uploaded)),
                fmt_bytes_opt(stats.map(|s| s.bytes_downloaded)),
                row.latency_ms
                    .map(|ms| ms.to_string())
                    .unwrap_or_else(|| "-".into()),
                row.error.as_deref().unwrap_or("-")
            );
        } else {
            println!("{base}");
        }
    }
}

fn node_index(settings: &Settings, name: &str) -> Option<usize> {
    let prefix = format!("{}-node-", settings.testbed_id);
    name.strip_prefix(&prefix)?.parse().ok()
}

fn fmt_idx(idx: Option<usize>) -> String {
    idx.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

fn fmt_stat(value: Option<u64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
}

fn fmt_bytes_opt(value: Option<u64>) -> String {
    value.map(format_bytes).unwrap_or_else(|| "-".into())
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes}B")
    } else if value >= 10.0 {
        format!("{value:.0}{}", UNITS[unit])
    } else {
        format!("{value:.1}{}", UNITS[unit])
    }
}

fn fit(value: &str, width: usize) -> String {
    if value.len() <= width {
        value.to_string()
    } else if width <= 1 {
        "~".into()
    } else {
        format!("{}~", &value[..width - 1])
    }
}
