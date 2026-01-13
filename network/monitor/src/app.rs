//! Application state for the Tapedrive Network Monitor.
//!
//! Defines the core data structures used by the UI to render network state.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tape_core::spooler::SpoolIndex;
use tape_core::types::{BasisPoints, EpochNumber, NodeId, StorageUnits};

/// Maximum number of events to keep in the event log.
pub const MAX_EVENTS: usize = 1000;

/// Total number of spools in the network.
pub const TOTAL_SPOOLS: u16 = 1024;

// ============================================================================
// Core Types
// ============================================================================

/// Health status of a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthStatus {
    /// Node is online and responding to health checks.
    Online,
    /// Node is offline or not responding.
    Offline,
    /// Node is syncing data.
    Syncing,
    /// Status unknown (not yet polled).
    #[default]
    Unknown,
}

/// Epoch phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EpochPhase {
    /// Unknown phase.
    #[default]
    Unknown,
    /// Nodes are attesting they have synced their spool data.
    Syncing,
    /// Previous committee members are settling rewards.
    Settling,
    /// Main operational phase - committee is active.
    Active,
}

impl EpochPhase {
    /// Get display name for the phase.
    pub fn as_str(&self) -> &'static str {
        match self {
            EpochPhase::Unknown => "Unknown",
            EpochPhase::Syncing => "Syncing",
            EpochPhase::Settling => "Settling",
            EpochPhase::Active => "Active",
        }
    }
}

/// Current view being displayed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum View {
    /// Main dashboard view.
    #[default]
    Dashboard,
    /// Node list view with sorting/filtering.
    NodeList,
    /// Node detail popup.
    NodeDetail(usize),
    /// Epoch history view.
    EpochHistory,
    /// Search view.
    Search(String),
    /// Help screen.
    Help,
}

// ============================================================================
// Node State
// ============================================================================

/// State of a single committee node.
#[derive(Debug, Clone)]
pub struct NodeState {
    /// Node ID.
    pub id: NodeId,
    /// Node name (truncated to 32 chars).
    pub name: String,
    /// Authority pubkey (base58 encoded).
    pub authority: String,
    /// Network address (IP:port).
    pub address: String,
    /// Health status.
    pub health: HealthStatus,
    /// Latency in milliseconds (if online).
    pub latency_ms: Option<u32>,
    /// Last health check timestamp.
    pub last_check: Instant,
    /// Stake amount in TAPE flux units.
    pub stake: u64,
    /// Commission in basis points.
    pub commission: BasisPoints,
    /// Number of spools assigned.
    pub spool_count: u16,
    /// List of assigned spool indices.
    pub assigned_spools: Vec<SpoolIndex>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            id: NodeId(0),
            name: String::new(),
            authority: String::new(),
            address: String::new(),
            health: HealthStatus::Unknown,
            latency_ms: None,
            last_check: Instant::now(),
            stake: 0,
            commission: BasisPoints(0),
            spool_count: 0,
            assigned_spools: Vec::new(),
        }
    }
}

impl NodeState {
    /// Get stake amount formatted with K/M suffix.
    pub fn stake_display(&self) -> String {
        if self.stake >= 1_000_000_000_000 {
            format!("{:.1}M", self.stake as f64 / 1_000_000_000_000.0)
        } else if self.stake >= 1_000_000_000 {
            format!("{:.0}K", self.stake as f64 / 1_000_000_000.0)
        } else {
            format!("{}", self.stake / 1_000_000)
        }
    }

    /// Get commission as percentage string.
    pub fn commission_display(&self) -> String {
        format!("{:.1}%", self.commission.0 as f64 / 100.0)
    }

    /// Get latency display string.
    pub fn latency_display(&self) -> String {
        match self.latency_ms {
            Some(ms) => format!("{}ms", ms),
            None => "TIMEOUT".to_string(),
        }
    }
}

// ============================================================================
// Network Events
// ============================================================================

/// Type of network event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// Track was certified.
    TrackCertified,
    /// Tape was reserved.
    TapeReserved,
    /// Track was registered.
    TrackRegistered,
    /// Node came online.
    NodeOnline,
    /// Node went offline.
    NodeOffline,
    /// Slice was uploaded.
    SliceUploaded,
    /// Blob was downloaded.
    BlobDownloaded,
    /// Epoch transitioned.
    EpochTransition,
    /// Error occurred.
    Error,
}

impl EventType {
    /// Get the icon for this event type.
    pub fn icon(&self) -> &'static str {
        match self {
            EventType::TrackCertified => "✓",
            EventType::TapeReserved | EventType::TrackRegistered => "+",
            EventType::NodeOnline => "●",
            EventType::NodeOffline => "⚠",
            EventType::SliceUploaded => "↑",
            EventType::BlobDownloaded => "↓",
            EventType::EpochTransition => "→",
            EventType::Error => "✗",
        }
    }
}

/// A network event to display in the event log.
#[derive(Debug, Clone)]
pub struct NetworkEvent {
    /// When the event occurred.
    pub timestamp: Instant,
    /// Type of event.
    pub event_type: EventType,
    /// Event description.
    pub description: String,
    /// Associated actors (nodes, pubkeys, etc.).
    pub actors: String,
}

impl NetworkEvent {
    /// Create a new network event.
    pub fn new(event_type: EventType, description: impl Into<String>, actors: impl Into<String>) -> Self {
        Self {
            timestamp: Instant::now(),
            event_type,
            description: description.into(),
            actors: actors.into(),
        }
    }

    /// Get the timestamp formatted as HH:MM:SS.
    pub fn timestamp_display(&self, app_start: Instant) -> String {
        let elapsed = self.timestamp.duration_since(app_start);
        let secs = elapsed.as_secs();
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;
        format!("{:02}:{:02}:{:02}", hours, mins, secs)
    }
}

// ============================================================================
// Network Stats
// ============================================================================

/// Aggregated network statistics.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total storage capacity across all nodes.
    pub storage_capacity: StorageUnits,
    /// Total storage used.
    pub storage_used: StorageUnits,
    /// Number of certified tracks.
    pub tracks_certified: u64,
    /// Number of active tapes.
    pub tapes_active: u64,
    /// Rewards pool balance.
    pub rewards_pool: u64,
    /// Total rewards paid out.
    pub rewards_paid: u64,
    /// Upload throughput in bytes/sec.
    pub upload_throughput: u64,
    /// Download throughput in bytes/sec.
    pub download_throughput: u64,
    /// Requests per second.
    pub requests_per_sec: u32,
}

impl NetworkStats {
    /// Get storage usage as percentage.
    pub fn storage_percentage(&self) -> u8 {
        if self.storage_capacity.0 == 0 {
            0
        } else {
            ((self.storage_used.0 * 100) / self.storage_capacity.0) as u8
        }
    }

    /// Format storage for display (e.g., "1.2 TB / 10 TB").
    pub fn storage_display(&self) -> String {
        let used = format_storage(self.storage_used.0);
        let capacity = format_storage(self.storage_capacity.0);
        format!("{} / {}", used, capacity)
    }

    /// Format throughput for display.
    pub fn throughput_display(&self) -> String {
        let up = format_bytes_per_sec(self.upload_throughput);
        let down = format_bytes_per_sec(self.download_throughput);
        format!("^ {} v {}", up, down)
    }
}

/// Format storage units (MB) for display.
fn format_storage(mb: u64) -> String {
    if mb >= 1_000_000 {
        format!("{:.1} TB", mb as f64 / 1_000_000.0)
    } else if mb >= 1_000 {
        format!("{:.1} GB", mb as f64 / 1_000.0)
    } else {
        format!("{} MB", mb)
    }
}

/// Format bytes per second for display.
fn format_bytes_per_sec(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.1} GB/s", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.1} MB/s", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1} KB/s", bytes as f64 / 1_000.0)
    } else {
        format!("{} B/s", bytes)
    }
}

// ============================================================================
// Spool Assignment
// ============================================================================

/// Spool assignment for a single node (used for visualization).
#[derive(Debug, Clone)]
pub struct SpoolAssignment {
    /// Node ID.
    pub node_id: NodeId,
    /// Node name.
    pub name: String,
    /// Number of spools assigned.
    pub count: u16,
}

// ============================================================================
// Main Application State
// ============================================================================

/// Main application state.
#[derive(Debug)]
pub struct App {
    /// Current view being displayed.
    pub current_view: View,
    /// Selected node index in the committee (for navigation).
    pub selected_node: Option<usize>,
    /// Scroll offset for lists.
    pub scroll_offset: usize,
    /// Event log scroll offset.
    pub event_scroll: usize,
    /// Auto-scroll event log.
    pub event_auto_scroll: bool,

    // Epoch data
    /// Current epoch number.
    pub epoch: EpochNumber,
    /// Current epoch phase.
    pub phase: EpochPhase,
    /// Timestamp of last epoch start (Unix seconds).
    pub epoch_start: i64,
    /// Epoch duration in seconds.
    pub epoch_duration: u64,
    /// Current slot number.
    pub current_slot: u64,

    // Committee data
    /// Current committee nodes.
    pub nodes: Vec<NodeState>,
    /// Spool assignments by node.
    pub spool_assignments: Vec<SpoolAssignment>,

    // Network stats
    /// Aggregated network statistics.
    pub stats: NetworkStats,

    // Event log
    /// Recent network events.
    pub events: VecDeque<NetworkEvent>,

    // Refresh timing
    /// Application start time (for relative timestamps).
    pub app_start: Instant,
    /// Last data refresh timestamp.
    pub last_refresh: Instant,
    /// Refresh interval.
    pub refresh_interval: Duration,

    // Connection state
    /// Whether RPC is connected.
    pub rpc_connected: bool,
    /// Recent fetch errors (cleared on successful fetch).
    pub fetch_errors: Vec<String>,

    // Node list view state
    /// Current sort order for node list.
    pub node_sort: NodeSortOrder,
    /// Current filter for node list.
    pub node_filter: NodeFilter,
}

/// Sort order for the node list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeSortOrder {
    #[default]
    Stake,
    Name,
    Latency,
    Commission,
    Spools,
}

/// Filter for the node list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeFilter {
    #[default]
    All,
    Online,
    Offline,
}

impl Default for App {
    fn default() -> Self {
        Self::new()
    }
}

impl App {
    /// Create a new application state with defaults.
    pub fn new() -> Self {
        let now = Instant::now();
        Self {
            current_view: View::Dashboard,
            selected_node: None,
            scroll_offset: 0,
            event_scroll: 0,
            event_auto_scroll: true,

            epoch: EpochNumber(0),
            phase: EpochPhase::Unknown,
            epoch_start: 0,
            epoch_duration: 604_800, // 1 week
            current_slot: 0,

            nodes: Vec::new(),
            spool_assignments: Vec::new(),

            stats: NetworkStats::default(),

            events: VecDeque::with_capacity(MAX_EVENTS),

            app_start: now,
            last_refresh: now,
            refresh_interval: Duration::from_secs(2),

            rpc_connected: false,
            fetch_errors: Vec::new(),

            node_sort: NodeSortOrder::default(),
            node_filter: NodeFilter::default(),
        }
    }

    /// Get count of online nodes.
    pub fn online_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.health == HealthStatus::Online).count()
    }

    /// Get count of offline nodes.
    pub fn offline_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.health == HealthStatus::Offline).count()
    }

    /// Get count of syncing nodes.
    pub fn syncing_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.health == HealthStatus::Syncing).count()
    }

    /// Get count of unknown nodes.
    pub fn unknown_count(&self) -> usize {
        self.nodes.iter().filter(|n| n.health == HealthStatus::Unknown).count()
    }

    /// Get total committee size.
    pub fn committee_size(&self) -> usize {
        self.nodes.len()
    }

    /// Calculate epoch progress as percentage (0-100).
    pub fn epoch_progress(&self) -> u8 {
        if self.epoch_duration == 0 {
            return 0;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let elapsed = (now - self.epoch_start).max(0) as u64;
        ((elapsed * 100) / self.epoch_duration).min(100) as u8
    }

    /// Calculate time remaining in epoch.
    pub fn time_remaining(&self) -> Duration {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let elapsed = (now - self.epoch_start).max(0) as u64;
        Duration::from_secs(self.epoch_duration.saturating_sub(elapsed))
    }

    /// Format time remaining as "Xd HH:MM:SS".
    pub fn time_remaining_display(&self) -> String {
        let remaining = self.time_remaining();
        let secs = remaining.as_secs();
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        let mins = (secs % 3600) / 60;
        let secs = secs % 60;
        if days > 0 {
            format!("{}d {:02}:{:02}:{:02}", days, hours, mins, secs)
        } else {
            format!("{:02}:{:02}:{:02}", hours, mins, secs)
        }
    }

    /// Get time since last refresh.
    pub fn last_refresh_ago(&self) -> Duration {
        self.last_refresh.elapsed()
    }

    /// Format last refresh time.
    pub fn last_refresh_display(&self) -> String {
        let ago = self.last_refresh_ago();
        if ago.as_secs() >= 60 {
            format!("{}m ago", ago.as_secs() / 60)
        } else {
            format!("{:.1}s ago", ago.as_secs_f32())
        }
    }

    /// Add a network event.
    pub fn add_event(&mut self, event: NetworkEvent) {
        if self.events.len() >= MAX_EVENTS {
            self.events.pop_front();
        }
        self.events.push_back(event);
        if self.event_auto_scroll {
            self.event_scroll = self.events.len().saturating_sub(1);
        }
    }

    /// Get the selected node (if any).
    pub fn selected_node_state(&self) -> Option<&NodeState> {
        self.selected_node.and_then(|idx| self.nodes.get(idx))
    }

    /// Move selection up.
    pub fn select_prev(&mut self) {
        if let Some(idx) = self.selected_node {
            if idx > 0 {
                self.selected_node = Some(idx - 1);
            }
        } else if !self.nodes.is_empty() {
            self.selected_node = Some(self.nodes.len() - 1);
        }
    }

    /// Move selection down.
    pub fn select_next(&mut self) {
        if let Some(idx) = self.selected_node {
            if idx + 1 < self.nodes.len() {
                self.selected_node = Some(idx + 1);
            }
        } else if !self.nodes.is_empty() {
            self.selected_node = Some(0);
        }
    }

    /// Clear selection.
    pub fn clear_selection(&mut self) {
        self.selected_node = None;
    }
}

// ============================================================================
// Demo/Test Data
// ============================================================================

impl App {
    /// Load demo data for testing UI layout.
    pub fn load_demo_data(&mut self) {
        // Set epoch data
        self.epoch = EpochNumber(47);
        self.phase = EpochPhase::Active;
        self.epoch_start = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0) - 200_000; // ~2.3 days ago
        self.epoch_duration = 604_800;
        self.current_slot = 298_472_109;
        self.rpc_connected = true;
        self.last_refresh = Instant::now();

        // Create demo nodes
        let demo_nodes = [
            ("validator1", HealthStatus::Online, 500_000, 122),
            ("speedy-node", HealthStatus::Online, 420_000, 102),
            ("tape-storage", HealthStatus::Online, 380_000, 93),
            ("archiver01", HealthStatus::Online, 350_000, 85),
            ("node-eu-1", HealthStatus::Offline, 300_000, 73),
            ("storage-us", HealthStatus::Online, 280_000, 68),
            ("asia-archive", HealthStatus::Syncing, 260_000, 63),
            ("backup-main", HealthStatus::Online, 240_000, 58),
        ];

        self.nodes = demo_nodes
            .iter()
            .enumerate()
            .map(|(i, (name, health, stake, spools))| NodeState {
                id: NodeId(i as u64 + 1),
                name: name.to_string(),
                authority: format!("7xKp...{}mQr", i),
                address: format!("192.168.1.{}:8080", 100 + i),
                health: *health,
                latency_ms: match health {
                    HealthStatus::Online => Some(45 + (i as u32 * 7)),
                    HealthStatus::Syncing => Some(150),
                    _ => None,
                },
                last_check: Instant::now(),
                stake: *stake * 1_000_000, // Convert to flux units
                commission: BasisPoints(200),
                spool_count: *spools,
                assigned_spools: (0..*spools).collect(),
            })
            .collect();

        // Pad to 87 nodes to match design mockup
        for i in 8..87 {
            let health = if i < 84 { HealthStatus::Online } else { HealthStatus::Offline };
            self.nodes.push(NodeState {
                id: NodeId(i as u64 + 1),
                name: format!("node-{}", i),
                authority: format!("{}...{}", i, i + 100),
                address: format!("10.0.0.{}:8080", i),
                health,
                latency_ms: if health == HealthStatus::Online { Some(50 + i as u32) } else { None },
                last_check: Instant::now(),
                stake: (200_000 - i as u64 * 1000) * 1_000_000,
                commission: BasisPoints(200 + i as u64 * 10),
                spool_count: (50 - i as u16 / 2).max(5),
                assigned_spools: Vec::new(),
            });
        }

        // Create spool assignments summary
        self.spool_assignments = self.nodes.iter()
            .take(10)
            .map(|n| SpoolAssignment {
                node_id: n.id,
                name: n.name.clone(),
                count: n.spool_count,
            })
            .collect();

        // Set network stats
        self.stats = NetworkStats {
            storage_capacity: StorageUnits(10_000_000), // 10 TB
            storage_used: StorageUnits(1_200_000),      // 1.2 TB
            tracks_certified: 48_291,
            tapes_active: 892,
            rewards_pool: 125_420_000_000,   // 125,420 TAPE
            rewards_paid: 118_290_000_000,   // 118,290 TAPE
            upload_throughput: 12_400_000,   // 12.4 MB/s
            download_throughput: 45_200_000, // 45.2 MB/s
            requests_per_sec: 1247,
        };

        // Add demo events
        let event_data = [
            (EventType::TrackCertified, "Track #48291 certified", "validator1, speedy-node"),
            (EventType::SliceUploaded, "Slice uploaded to spool 742", "archiver01"),
            (EventType::NodeOnline, "Node #143 came online", "node-asia-2"),
            (EventType::TrackCertified, "Track #48290 certified", "tape-stor, node-eu-1"),
            (EventType::NodeOffline, "Node #87 went offline", "backup-store"),
            (EventType::BlobDownloaded, "Blob downloaded (48 MB)", "client request"),
            (EventType::TapeReserved, "Tape #893 reserved (1 GB, 52 epochs)", "7xKp...3mQr"),
        ];

        for (event_type, desc, actors) in event_data {
            self.add_event(NetworkEvent::new(event_type, desc, actors));
        }
    }
}
