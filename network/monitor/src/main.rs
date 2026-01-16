//! Tapedrive Network Monitor - Real-time TUI dashboard.
//!
//! A terminal-based monitor for the Tapedrive decentralized storage network,
//! providing real-time visibility into:
//!
//! - Committee node health and status
//! - Epoch progress and phase transitions
//! - Network statistics (storage, tracks, tapes)
//! - Spool distribution across nodes
//! - Recent network events
//!
//! # Usage
//!
//! ```bash
//! tape-monitor [OPTIONS]
//!
//! Options:
//!     -u, --url <URL>           Solana RPC URL [default: https://api.mainnet-beta.solana.com]
//!     -r, --refresh <SECS>      Refresh interval in seconds [default: 2]
//!     --node-timeout <MS>       Node health check timeout [default: 5000]
//!     -h, --help                Print help
//!     -V, --version             Print version
//! ```

pub mod app;
pub mod data;
pub mod input;
pub mod theme;
pub mod ui;

use std::io;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use rpc_client::RpcConfig;
use tokio::sync::mpsc;

use std::collections::{BTreeMap, HashMap};

use app::{App, EpochPhase, NodeState as AppNodeState, StakeScheduleEntry};
use data::{BlockProcessor, DataCache, DataFetcher, EventWatcher, NodeState as DataNodeState, TapeStats, TapedriveEvent, ToNetworkEvent};
use tape_api::program::tapedrive::EPOCH_DURATION;
use tape_api::prelude::Committee;
use tape_api::state::{Archive, Epoch, Node, System};
use tape_core::spooler::SpoolIndex;
use tape_core::types::coin::TAPE;
use tape_core::types::{EpochNumber, NodeId};

/// Solana cluster/network selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cluster {
    Localnet,
    Mainnet,
    Devnet,
    Testnet,
    Custom(String),
}

impl Cluster {
    pub fn rpc_url(&self) -> String {
        match self {
            Cluster::Localnet => "http://127.0.0.1:8899".to_string(),
            Cluster::Mainnet => "https://api.mainnet-beta.solana.com".to_string(),
            Cluster::Devnet => "https://api.devnet.solana.com".to_string(),
            Cluster::Testnet => "https://api.testnet.solana.com".to_string(),
            Cluster::Custom(url) => url.clone(),
        }
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster::Localnet
    }
}

impl FromStr for Cluster {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "l" | "local" | "localnet" => Ok(Cluster::Localnet),
            "m" | "main" | "mainnet" | "mainnet-beta" => Ok(Cluster::Mainnet),
            "d" | "dev" | "devnet" => Ok(Cluster::Devnet),
            "t" | "test" | "testnet" => Ok(Cluster::Testnet),
            s if s.starts_with("http://") || s.starts_with("https://") => {
                Ok(Cluster::Custom(s.to_string()))
            }
            _ => Err(format!(
                "Invalid cluster: '{}'. Use l/m/d/t or a valid RPC URL",
                s
            )),
        }
    }
}

/// Real-time TUI network monitor for Tapedrive.
#[derive(Parser, Debug)]
#[command(name = "tape-monitor")]
#[command(about = "Real-time TUI network monitor for Tapedrive")]
struct Args {
    /// Cluster: l (localnet), m (mainnet), d (devnet), t (testnet), or URL
    #[arg(short = 'u', long = "cluster", default_value = "l")]
    cluster: Cluster,

    /// Refresh interval in seconds
    #[arg(short = 'r', long = "refresh", default_value = "2")]
    refresh_secs: u64,

    /// Node health check timeout in milliseconds
    #[arg(long = "timeout", default_value = "5000")]
    health_timeout_ms: u64,
}

/// Message from background data fetcher
enum FetchResult {
    /// All data fetched successfully
    Success {
        system: System,
        epoch: Epoch,
        archive: Archive,
        nodes: Vec<DataNodeState>,
        tape_stats: TapeStats,
        slot: u64,
    },
    /// Partial data fetched (some accounts may not exist yet)
    Partial {
        system: Option<System>,
        epoch: Option<Epoch>,
        archive: Option<Archive>,
        nodes: Vec<DataNodeState>,
        tape_stats: TapeStats,
        slot: u64,
        errors: Vec<String>,
    },
    /// Complete fetch failure (e.g., RPC connection lost)
    Error(String),
}

/// Application entry point.
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let rpc_url = args.cluster.rpc_url();
    let refresh_interval = Duration::from_secs(args.refresh_secs);
    let health_timeout_ms = args.health_timeout_ms;

    // Initialize terminal
    enable_raw_mode().context("Failed to enable raw mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)
        .context("Failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("Failed to create terminal")?;
    terminal.clear().context("Failed to clear terminal")?;

    // Create application state
    let mut app = App::new();
    app.refresh_interval = refresh_interval;
    // Don't load demo data - show real state from RPC

    // Channel for receiving fetch results from background task
    let (tx, mut rx) = mpsc::channel::<FetchResult>(1);

    // Channel for receiving block events from block processor
    let (block_tx, mut block_rx) = mpsc::channel::<Vec<TapedriveEvent>>(16);

    // Spawn background data fetcher
    let fetch_trigger = Arc::new(Mutex::new(false));
    let fetch_trigger_clone = fetch_trigger.clone();

    if let Ok(fetcher) = DataFetcher::new(&rpc_url) {
        // Create a separate RPC client for the block processor
        let block_rpc_url = rpc_url.clone();
        tokio::spawn(async move {
            // Create RPC client for block processor
            let rpc_config = RpcConfig {
                endpoints: vec![block_rpc_url],
                ..Default::default()
            };
            let rpc = match rpc_client::RpcClient::new(rpc_config) {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Failed to create block processor RPC client: {}", e);
                    return;
                }
            };

            let mut block_processor = BlockProcessor::new(0);

            // Wait a bit for RPC to connect and get initial slot
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Initialize starting slot
            if let Ok(slot) = rpc.get_slot().await {
                // Start from recent slot (not genesis)
                block_processor.set_last_slot(slot.saturating_sub(10));
            }

            loop {
                // Get latest slot
                if let Ok(latest_slot) = rpc.get_slot().await {
                    // Process up to 50 slots per iteration (catch up faster than node's 100)
                    let (events, _) = block_processor.process_slots(&rpc, latest_slot, 50).await;
                    if !events.is_empty() {
                        let _ = block_tx.send(events).await;
                    }
                }

                // Poll at ~400ms like the node (Solana slot time)
                tokio::time::sleep(Duration::from_millis(400)).await;
            }
        });
        let fetcher = Arc::new(fetcher);
        tokio::spawn(async move {
            loop {
                // Check if a fetch is requested
                let should_fetch = {
                    let mut trigger = fetch_trigger_clone.lock().unwrap();
                    if *trigger {
                        *trigger = false;
                        true
                    } else {
                        false
                    }
                };

                if should_fetch {
                    // Use graceful fetch that handles missing/partial accounts
                    let (system, epoch, archive, nodes, tape_stats, slot, errors) =
                        fetcher.fetch_dashboard_data_graceful(health_timeout_ms).await;

                    // If we got all three main accounts, treat as success
                    if system.is_some() && epoch.is_some() && archive.is_some() {
                        let _ = tx.send(FetchResult::Success {
                            system: system.unwrap(),
                            epoch: epoch.unwrap(),
                            archive: archive.unwrap(),
                            nodes,
                            tape_stats,
                            slot,
                        }).await;
                    } else if system.is_some() || epoch.is_some() || archive.is_some() || !nodes.is_empty() || slot > 0 {
                        // Partial data available - send what we have
                        let _ = tx.send(FetchResult::Partial {
                            system,
                            epoch,
                            archive,
                            nodes,
                            tape_stats,
                            slot,
                            errors,
                        }).await;
                    } else {
                        // Complete failure
                        let error_msg = if errors.is_empty() {
                            "No data available".to_string()
                        } else {
                            errors.join("; ")
                        };
                        let _ = tx.send(FetchResult::Error(error_msg)).await;
                    }
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        // Trigger initial fetch
        *fetch_trigger.lock().unwrap() = true;
    }

    let mut cache = DataCache::new(refresh_interval);
    let mut event_watcher = EventWatcher::new();

    // Run the main loop
    let result = run_app(&mut terminal, &mut app, &mut rx, &mut block_rx, &fetch_trigger, &mut cache, &mut event_watcher).await;

    // Restore terminal
    disable_raw_mode().context("Failed to disable raw mode")?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen
    )
    .context("Failed to leave alternate screen")?;
    terminal.show_cursor().context("Failed to show cursor")?;

    // Return any error from the main loop
    result
}

/// Main application loop.
async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut App,
    rx: &mut mpsc::Receiver<FetchResult>,
    block_rx: &mut mpsc::Receiver<Vec<TapedriveEvent>>,
    fetch_trigger: &Arc<Mutex<bool>>,
    cache: &mut DataCache,
    event_watcher: &mut EventWatcher,
) -> Result<()> {
    loop {
        // Draw the UI
        terminal.draw(|f| {
            ui::draw(f, app);
        })?;

        // Poll for terminal events (synchronous, with short timeout)
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(()),
                    KeyCode::Char('c') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                        return Ok(());
                    }
                    KeyCode::Esc => {
                        if app.current_view == crate::app::View::Dashboard {
                            return Ok(());
                        }
                        input::handle_input(app, key);
                    }
                    KeyCode::Char('r') => {
                        // Trigger a refresh
                        *fetch_trigger.lock().unwrap() = true;
                    }
                    _ => {
                        input::handle_input(app, key);
                    }
                }
            }
        }

        // Check for data from background fetcher (non-blocking)
        match rx.try_recv() {
            Ok(FetchResult::Success { system, epoch, archive, nodes, tape_stats, slot }) => {
                cache.update_all(system.clone(), epoch.clone(), archive.clone(), nodes.clone());
                app.rpc_connected = true;
                app.last_refresh = std::time::Instant::now();
                app.fetch_errors.clear(); // Clear errors on successful fetch
                app.current_slot = slot;

                // Generate events based on state changes
                let events = event_watcher.update_state(
                    &nodes,
                    Some(&epoch),
                    tape_stats.track_count,
                    tape_stats.active_tapes,
                );
                for evt in events {
                    app.add_event(convert_network_event(&evt));
                }

                update_app_from_data(app, &system, &epoch, &archive, &nodes, &tape_stats);
                app.update_color_slots();
            }
            Ok(FetchResult::Partial { system, epoch, archive, nodes, tape_stats, slot, errors }) => {
                // Handle partial data - update what we can
                app.rpc_connected = true; // RPC is working, just missing accounts
                app.last_refresh = std::time::Instant::now();
                app.current_slot = slot;

                // Store errors for display
                app.fetch_errors = errors.clone();

                // Generate events based on state changes
                let events = event_watcher.update_state(
                    &nodes,
                    epoch.as_ref(),
                    tape_stats.track_count,
                    tape_stats.active_tapes,
                );
                for evt in events {
                    app.add_event(convert_network_event(&evt));
                }

                // Log any errors as events (only if new errors)
                for error in &errors {
                    app.add_event(app::NetworkEvent::new(
                        app::EventType::Error,
                        format!("Fetch error: {}", error),
                        "",
                    ));
                }

                update_app_from_partial_data(app, system.as_ref(), epoch.as_ref(), archive.as_ref(), &nodes, &tape_stats);
                app.update_color_slots();
            }
            Ok(FetchResult::Error(err)) => {
                app.rpc_connected = false;
                app.fetch_errors = vec![err.clone()];
                // Reset event watcher state on connection loss
                event_watcher.reset();
                // Log the error as an event (only if this is a new error)
                let error_msg = format!("RPC error: {}", err);
                let already_logged = app.events.iter()
                    .take(3)
                    .any(|e| e.description == error_msg);
                if !already_logged {
                    app.add_event(app::NetworkEvent::new(
                        app::EventType::Error,
                        error_msg,
                        "Retrying...",
                    ));
                }
                // Force immediate retry by invalidating cache
                cache.invalidate();
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                // No data yet, that's fine
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                // Fetcher task died
                app.rpc_connected = false;
            }
        }

        // Check for block events from block processor (non-blocking)
        while let Ok(events) = block_rx.try_recv() {
            for event in events {
                let network_event = event.to_network_event();
                app.add_event(network_event);
            }
        }

        // Trigger periodic refresh if needed
        if cache.needs_refresh() {
            *fetch_trigger.lock().unwrap() = true;
        }
    }
}

/// Update the App state from fetched on-chain data.
fn update_app_from_data(
    app: &mut App,
    system: &System,
    epoch: &Epoch,
    archive: &Archive,
    nodes: &[DataNodeState],
    tape_stats: &TapeStats,
) {
    // Update epoch information
    app.epoch = epoch.id;
    app.phase = if epoch.state.is_syncing() {
        EpochPhase::Syncing
    } else if epoch.state.is_settling() {
        EpochPhase::Settling
    } else if epoch.state.is_active() {
        EpochPhase::Active
    } else {
        EpochPhase::Unknown
    };
    app.epoch_start = epoch.last_epoch;
    app.epoch_duration = EPOCH_DURATION as u64;
    app.epoch_weight = epoch.state.weight;
    app.is_low_quorum = system.is_low_quorum();
    app.committee_next_size = system.committee_next.size();

    // Copy spool assignment arrays
    app.spools_prev = Some(system.spools_prev.0);
    app.spools_current = Some(system.spools.0);

    // Build node lookup: NodeId -> DataNodeState
    let node_lookup: HashMap<NodeId, &DataNodeState> = nodes
        .iter()
        .map(|n| (n.node.id, n))
        .collect();

    // Build PREV committee nodes
    app.committee_prev_nodes = build_committee_nodes(
        &system.committee_prev,
        &system.spools_prev.0,
        &node_lookup,
    );

    // Build CURRENT committee nodes (into app.nodes for compatibility)
    app.nodes = build_committee_nodes(
        &system.committee,
        &system.spools.0,
        &node_lookup,
    );

    // Build NEXT committee nodes (no spool data)
    app.committee_next_nodes = build_committee_nodes_no_spools(
        &system.committee_next,
        &node_lookup,
    );

    // Update network stats from archive and tape stats
    app.stats.storage_capacity = archive.storage_capacity;
    app.stats.storage_used = archive.recent_usage;
    app.stats.tapes_active = tape_stats.active_tapes;
    app.stats.tracks_certified = tape_stats.track_count;
    app.stats.rewards_pool = archive.rewards_pool.0;
    app.stats.rewards_paid = archive.rewards_paid.0;

    // Update spool assignments summary
    app.spool_assignments = app
        .nodes
        .iter()
        .take(10)
        .map(|n| app::SpoolAssignment {
            node_id: n.id,
            name: n.name.clone(),
            count: n.spool_count,
        })
        .collect();

    // Compute throughput and request rates from node stats
    compute_rates(app);
}

/// Compute throughput and request rates from aggregated node stats.
fn compute_rates(app: &mut App) {
    use std::time::Instant;

    let now = Instant::now();
    let elapsed = now.duration_since(app.last_rate_calc).as_secs_f64();

    // Only compute rates if enough time has passed (at least 0.5 seconds)
    if elapsed < 0.5 {
        return;
    }

    // Aggregate stats from all online nodes
    let (total_bytes_up, total_bytes_down, total_requests) = app
        .nodes
        .iter()
        .filter(|n| n.health == app::HealthStatus::Online)
        .filter_map(|n| n.stats.as_ref())
        .fold((0u64, 0u64, 0u64), |(up, down, req), stats| {
            (
                up + stats.bytes_uploaded,
                down + stats.bytes_downloaded,
                req + stats.requests_total,
            )
        });

    // Compute rates from deltas
    if app.prev_bytes_uploaded > 0 || app.prev_bytes_downloaded > 0 || app.prev_requests_total > 0 {
        let bytes_up_delta = total_bytes_up.saturating_sub(app.prev_bytes_uploaded);
        let bytes_down_delta = total_bytes_down.saturating_sub(app.prev_bytes_downloaded);
        let requests_delta = total_requests.saturating_sub(app.prev_requests_total);

        app.stats.upload_throughput = (bytes_up_delta as f64 / elapsed) as u64;
        app.stats.download_throughput = (bytes_down_delta as f64 / elapsed) as u64;
        app.stats.requests_per_sec = (requests_delta as f64 / elapsed) as u32;
    }

    // Update previous values for next calculation
    app.prev_bytes_uploaded = total_bytes_up;
    app.prev_bytes_downloaded = total_bytes_down;
    app.prev_requests_total = total_requests;
    app.last_rate_calc = now;
}

/// Convert data layer HealthStatus to app layer HealthStatus.
fn convert_health_status(status: data::HealthStatus) -> app::HealthStatus {
    match status {
        data::HealthStatus::Online => app::HealthStatus::Online,
        data::HealthStatus::Offline => app::HealthStatus::Offline,
        data::HealthStatus::Syncing => app::HealthStatus::Syncing,
        data::HealthStatus::Unknown => app::HealthStatus::Unknown,
    }
}

/// Extract stake schedule from a node's pool schedule.
/// Returns a BTreeMap of epoch -> (incoming, cancels).
fn extract_stake_schedule(node: &Node) -> BTreeMap<EpochNumber, StakeScheduleEntry> {
    let mut schedule: BTreeMap<EpochNumber, StakeScheduleEntry> = BTreeMap::new();

    // Add incoming tokens by epoch
    let incoming = &node.pool.schedule.incoming_tokens;
    for i in 0..incoming.len() {
        let epoch = incoming.keys[i];
        let amount = incoming.values[i];
        schedule.entry(epoch).or_default().incoming = TAPE(amount);
    }

    // Add outgoing tokens (cancels) by epoch
    let outgoing = &node.pool.schedule.outgoing_tokens;
    for i in 0..outgoing.len() {
        let epoch = outgoing.keys[i];
        let amount = outgoing.values[i];
        schedule.entry(epoch).or_default().cancels = TAPE(amount);
    }

    schedule
}

/// Build NodeState list for a committee with spool assignments.
fn build_committee_nodes(
    committee: &Committee<{ tape_api::program::tapedrive::MEMBER_COUNT }>,
    spools: &[u8; 1024],
    node_lookup: &HashMap<NodeId, &DataNodeState>,
) -> Vec<AppNodeState> {
    use std::time::Instant;
    committee.active_members()
        .iter()
        .enumerate()
        .map(|(member_idx, &node_id)| {
            let data_node = node_lookup.get(&node_id);

            // Calculate assigned spools for this member
            let assigned_spools: Vec<SpoolIndex> = spools
                .iter()
                .enumerate()
                .filter_map(|(spool_idx, &owner)| {
                    if owner as usize == member_idx {
                        Some(spool_idx as SpoolIndex)
                    } else {
                        None
                    }
                })
                .collect();

            // Get stake from committee
            let stake = committee.get_stake(&node_id).unwrap_or(TAPE::zero());

            if let Some(data_node) = data_node {
                let node = &data_node.node;
                AppNodeState {
                    id: node.id,
                    name: data_node.display_name(),
                    authority: data_node.address.to_string(),
                    address: data_node.network_address().unwrap_or_default(),
                    health: convert_health_status(data_node.health),
                    latency_ms: data_node.latency_ms,
                    last_check: data_node.last_check,
                    stake,
                    pool_stake: node.pool.stake,
                    stake_schedule: extract_stake_schedule(node),
                    commission: node.pool.commission_rate,
                    commission_earned: node.pool.commission,
                    rewards_pool: node.pool.rewards,
                    spool_count: assigned_spools.len() as u16,
                    assigned_spools,
                    stats: data_node.stats.clone(),
                }
            } else {
                // Node in committee but not in our node list (shouldn't happen often)
                AppNodeState {
                    id: node_id,
                    name: format!("Node {}", node_id.0),
                    authority: String::new(),
                    address: String::new(),
                    health: app::HealthStatus::Unknown,
                    latency_ms: None,
                    last_check: Instant::now(),
                    stake,
                    pool_stake: TAPE::zero(),
                    stake_schedule: BTreeMap::new(),
                    commission: tape_core::types::BasisPoints(0),
                    commission_earned: TAPE::zero(),
                    rewards_pool: TAPE::zero(),
                    spool_count: assigned_spools.len() as u16,
                    assigned_spools,
                    stats: None,
                }
            }
        })
        .collect()
}

/// Build NodeState list for a committee without spool assignments (committee_next).
fn build_committee_nodes_no_spools(
    committee: &Committee<{ tape_api::program::tapedrive::MEMBER_COUNT }>,
    node_lookup: &HashMap<NodeId, &DataNodeState>,
) -> Vec<AppNodeState> {
    use std::time::Instant;
    committee.active_members()
        .iter()
        .map(|&node_id| {
            let data_node = node_lookup.get(&node_id);

            // Get stake from committee
            let stake = committee.get_stake(&node_id).unwrap_or(TAPE::zero());

            if let Some(data_node) = data_node {
                let node = &data_node.node;
                AppNodeState {
                    id: node.id,
                    name: data_node.display_name(),
                    authority: data_node.address.to_string(),
                    address: data_node.network_address().unwrap_or_default(),
                    health: convert_health_status(data_node.health),
                    latency_ms: data_node.latency_ms,
                    last_check: data_node.last_check,
                    stake,
                    pool_stake: node.pool.stake,
                    stake_schedule: extract_stake_schedule(node),
                    commission: node.pool.commission_rate,
                    commission_earned: node.pool.commission,
                    rewards_pool: node.pool.rewards,
                    spool_count: 0,  // NEXT committee has no spool assignments yet
                    assigned_spools: Vec::new(),
                    stats: data_node.stats.clone(),
                }
            } else {
                AppNodeState {
                    id: node_id,
                    name: format!("Node {}", node_id.0),
                    authority: String::new(),
                    address: String::new(),
                    health: app::HealthStatus::Unknown,
                    latency_ms: None,
                    last_check: Instant::now(),
                    stake,
                    pool_stake: TAPE::zero(),
                    stake_schedule: BTreeMap::new(),
                    commission: tape_core::types::BasisPoints(0),
                    commission_earned: TAPE::zero(),
                    rewards_pool: TAPE::zero(),
                    spool_count: 0,
                    assigned_spools: Vec::new(),
                    stats: None,
                }
            }
        })
        .collect()
}

/// Convert data layer NetworkEvent to app layer NetworkEvent.
fn convert_network_event(event: &data::NetworkEvent) -> app::NetworkEvent {
    let event_type = match event.event_type {
        data::EventType::TrackCertified => app::EventType::TrackCertified,
        data::EventType::NodeOnline => app::EventType::NodeOnline,
        data::EventType::NodeOffline => app::EventType::NodeOffline,
        data::EventType::TapeReserved => app::EventType::TapeReserved,
        data::EventType::TrackRegistered => app::EventType::TrackRegistered,
        data::EventType::DataUploaded => app::EventType::SliceUploaded,
        data::EventType::DataDownloaded => app::EventType::BlobDownloaded,
        data::EventType::EpochTransition => app::EventType::EpochTransition,
        data::EventType::NodeJoined => app::EventType::NodeOnline,
        data::EventType::NodeLeft => app::EventType::NodeOffline,
        data::EventType::Info => app::EventType::TrackCertified, // Map info to something visible
        data::EventType::Warning => app::EventType::Error,
        data::EventType::Error => app::EventType::Error,
    };

    app::NetworkEvent::new(
        event_type,
        &event.description,
        event.details.as_deref().unwrap_or(""),
    )
}

/// Update the App state from partial fetched data (some accounts may be missing).
fn update_app_from_partial_data(
    app: &mut App,
    system: Option<&System>,
    epoch: Option<&Epoch>,
    archive: Option<&Archive>,
    nodes: &[DataNodeState],
    tape_stats: &TapeStats,
) {
    // Detect reset: epoch went backwards or all committees are empty
    let is_reset = if let (Some(epoch_data), Some(system_data)) = (epoch, system) {
        let epoch_went_back = epoch_data.id < app.epoch && app.epoch.0 > 0;
        let all_empty = system_data.committee.size() == 0
            && system_data.committee_prev.size() == 0
            && system_data.committee_next.size() == 0;
        epoch_went_back || (all_empty && !app.nodes.is_empty())
    } else {
        false
    };

    if is_reset {
        // Clear all state
        app.committee_prev_nodes.clear();
        app.nodes.clear();
        app.committee_next_nodes.clear();
        app.spools_prev = None;
        app.spools_current = None;
        app.events.clear();
        app.selected_node_index = None;
        app.stats = app::NetworkStats::default();

        // Log reset event
        app.add_event(app::NetworkEvent::new(
            app::EventType::EpochTransition,
            "System reset detected - cleared state",
            "",
        ));
    }

    // Update epoch information if available
    if let Some(epoch_data) = epoch {
        app.epoch = epoch_data.id;
        app.phase = if epoch_data.state.is_syncing() {
            EpochPhase::Syncing
        } else if epoch_data.state.is_settling() {
            EpochPhase::Settling
        } else if epoch_data.state.is_active() {
            EpochPhase::Active
        } else {
            EpochPhase::Unknown
        };
        app.epoch_start = epoch_data.last_epoch;
        app.epoch_duration = EPOCH_DURATION as u64;
        app.epoch_weight = epoch_data.state.weight;
    }

    // Update archive stats if available
    if let Some(archive) = archive {
        app.stats.storage_capacity = archive.storage_capacity;
        app.stats.storage_used = archive.recent_usage;
        app.stats.rewards_pool = archive.rewards_pool.0;
        app.stats.rewards_paid = archive.rewards_paid.0;
    }

    // Update tape stats (from tape fetching, independent of archive)
    app.stats.tapes_active = tape_stats.active_tapes;
    app.stats.tracks_certified = tape_stats.track_count;

    // Update system-derived fields and convert nodes
    if let Some(system) = system {
        app.is_low_quorum = system.is_low_quorum();
        app.committee_next_size = system.committee_next.size();

        // Copy spool assignment arrays
        app.spools_prev = Some(system.spools_prev.0);
        app.spools_current = Some(system.spools.0);

        // Build node lookup: NodeId -> DataNodeState
        let node_lookup: HashMap<NodeId, &DataNodeState> = nodes
            .iter()
            .map(|n| (n.node.id, n))
            .collect();

        // Build PREV committee nodes
        app.committee_prev_nodes = build_committee_nodes(
            &system.committee_prev,
            &system.spools_prev.0,
            &node_lookup,
        );

        // Build CURRENT committee nodes (into app.nodes for compatibility)
        app.nodes = build_committee_nodes(
            &system.committee,
            &system.spools.0,
            &node_lookup,
        );

        // Build NEXT committee nodes (no spool data)
        app.committee_next_nodes = build_committee_nodes_no_spools(
            &system.committee_next,
            &node_lookup,
        );

        // Update spool assignments summary
        app.spool_assignments = app
            .nodes
            .iter()
            .take(10)
            .map(|n| app::SpoolAssignment {
                node_id: n.id,
                name: n.name.clone(),
                count: n.spool_count,
            })
            .collect();
    } else if !nodes.is_empty() {
        // No system data, but we have nodes - show them without spool info
        // Use pool stake as committee stake (no committee data to get actual value)
        app.nodes = nodes
            .iter()
            .map(|data_node| {
                let node = &data_node.node;
                AppNodeState {
                    id: node.id,
                    name: data_node.display_name(),
                    authority: data_node.address.to_string(),
                    address: data_node.network_address().unwrap_or_default(),
                    health: convert_health_status(data_node.health),
                    latency_ms: data_node.latency_ms,
                    last_check: data_node.last_check,
                    stake: node.pool.stake,
                    pool_stake: node.pool.stake,
                    stake_schedule: extract_stake_schedule(node),
                    commission: node.pool.commission_rate,
                    commission_earned: node.pool.commission,
                    rewards_pool: node.pool.rewards,
                    spool_count: 0,
                    assigned_spools: Vec::new(),
                    stats: data_node.stats.clone(),
                }
            })
            .collect();
        // Clear prev and next committees when no system data
        app.committee_prev_nodes.clear();
        app.committee_next_nodes.clear();
        app.spools_prev = None;
        app.spools_current = None;
    }

    // Compute throughput and request rates from node stats
    compute_rates(app);
}
