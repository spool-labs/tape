//! Data fetching and caching layer for the Tapedrive Network Monitor.
//!
//! This module provides:
//! - [`DataFetcher`] - Async data fetching from Solana RPC and storage nodes
//! - [`DataCache`] - Caching layer with configurable refresh intervals
//! - [`EventWatcher`] - Event stream processing for network activity
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
//! │ DataFetcher │────>│  DataCache  │────>│     TUI     │
//! └─────────────┘     └─────────────┘     └─────────────┘
//!       │
//!       ├── Solana RPC (System, Epoch, Archive, Nodes)
//!       ├── Node Health Checks (HTTP /v1/health)
//!       └── Event Stream (transaction subscription)
//! ```

mod block_processor;
mod cache;
mod events;
mod fetcher;

pub use block_processor::{BlockProcessor, TapedriveEvent, ToNetworkEvent};
pub use cache::DataCache;
pub use events::EventWatcher;
pub use fetcher::{DataFetcher, TapeStats};

use solana_sdk::pubkey::Pubkey;
use std::time::Instant;
use tape_api::state::Node;
use tape_core::types::NodeId;
use tape_node_api::NodeStats;

// ============================================================================
// Shared Types
// ============================================================================

/// Health status of a storage node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HealthStatus {
    /// Node is online and responding to health checks.
    Online,
    /// Node is offline or not responding.
    Offline,
    /// Node is online but currently syncing data.
    Syncing,
    /// Node health status has not been checked yet.
    #[default]
    Unknown,
}

impl HealthStatus {
    /// Returns true if the node is considered healthy (Online or Syncing).
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Online | HealthStatus::Syncing)
    }

    /// Returns a display symbol for the status.
    pub fn symbol(&self) -> &'static str {
        match self {
            HealthStatus::Online => "●",
            HealthStatus::Offline => "○",
            HealthStatus::Syncing => "◐",
            HealthStatus::Unknown => "◌",
        }
    }

    /// Returns a display label for the status.
    pub fn label(&self) -> &'static str {
        match self {
            HealthStatus::Online => "Online",
            HealthStatus::Offline => "Offline",
            HealthStatus::Syncing => "Syncing",
            HealthStatus::Unknown => "Unknown",
        }
    }
}

/// Complete state information for a storage node.
#[derive(Debug, Clone)]
pub struct NodeState {
    /// The on-chain Node account data.
    pub node: Node,
    /// The public key address of the Node account.
    pub address: Pubkey,
    /// Current health status from the last check.
    pub health: HealthStatus,
    /// Latency in milliseconds from the last health check.
    /// None if the node is offline or hasn't been checked.
    pub latency_ms: Option<u32>,
    /// Timestamp of the last health check.
    pub last_check: Instant,
    /// Block processor stats from /v1/stats endpoint.
    /// None if the node is offline or stats couldn't be fetched.
    pub stats: Option<NodeStats>,
}

impl NodeState {
    /// Create a new NodeState with unknown health status.
    pub fn new(address: Pubkey, node: Node) -> Self {
        Self {
            node,
            address,
            health: HealthStatus::Unknown,
            latency_ms: None,
            last_check: Instant::now(),
            stats: None,
        }
    }

    /// Get the node's unique ID.
    pub fn id(&self) -> NodeId {
        self.node.id
    }

    /// Get the node's display name, or a truncated address if no name is set.
    pub fn display_name(&self) -> String {
        let name_bytes = &self.node.metadata.name;
        // Find the first null byte to determine actual string length
        let len = name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
        if len == 0 {
            // No name set, use truncated address
            let addr_str = self.address.to_string();
            format!("{}...{}", &addr_str[..4], &addr_str[addr_str.len() - 4..])
        } else {
            String::from_utf8_lossy(&name_bytes[..len]).to_string()
        }
    }

    /// Get the node's network address as a string.
    pub fn network_address(&self) -> Option<String> {
        let addr = &self.node.metadata.network_address;
        addr.to_socket_addr().ok().map(|sa| sa.to_string())
    }
}

/// Type of network event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventType {
    /// A track was certified by the network.
    TrackCertified,
    /// A storage node came online.
    NodeOnline,
    /// A storage node went offline.
    NodeOffline,
    /// A new tape was reserved.
    TapeReserved,
    /// A new track was registered.
    TrackRegistered,
    /// Data was uploaded to the network.
    DataUploaded,
    /// Data was downloaded from the network.
    DataDownloaded,
    /// Epoch transitioned to a new phase.
    EpochTransition,
    /// A node joined the committee.
    NodeJoined,
    /// A node left the committee.
    NodeLeft,
    /// Generic info event.
    Info,
    /// Warning event.
    Warning,
    /// Error event.
    Error,
}

impl EventType {
    /// Returns a display icon for the event type.
    pub fn icon(&self) -> &'static str {
        match self {
            EventType::TrackCertified => "✓",
            EventType::NodeOnline => "●",
            EventType::NodeOffline => "⚠",
            EventType::TapeReserved => "+",
            EventType::TrackRegistered => "+",
            EventType::DataUploaded => "↑",
            EventType::DataDownloaded => "↓",
            EventType::EpochTransition => "→",
            EventType::NodeJoined => "+",
            EventType::NodeLeft => "-",
            EventType::Info => "i",
            EventType::Warning => "⚠",
            EventType::Error => "✗",
        }
    }
}

/// A network event for display in the event log.
#[derive(Debug, Clone)]
pub struct NetworkEvent {
    /// When the event occurred.
    pub timestamp: Instant,
    /// The type of event.
    pub event_type: EventType,
    /// Short description of the event.
    pub description: String,
    /// Optional additional details (e.g., node names, transaction signature).
    pub details: Option<String>,
}

impl NetworkEvent {
    /// Create a new network event.
    pub fn new(event_type: EventType, description: impl Into<String>) -> Self {
        Self {
            timestamp: Instant::now(),
            event_type,
            description: description.into(),
            details: None,
        }
    }

    /// Create a new network event with details.
    pub fn with_details(
        event_type: EventType,
        description: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self {
            timestamp: Instant::now(),
            event_type,
            description: description.into(),
            details: Some(details.into()),
        }
    }

    /// Create a track certified event.
    pub fn track_certified(track_number: u64, certifiers: &str) -> Self {
        Self::with_details(
            EventType::TrackCertified,
            format!("Track #{} certified", track_number),
            certifiers,
        )
    }

    /// Create a node online event.
    pub fn node_online(node_id: NodeId, name: &str) -> Self {
        Self::with_details(
            EventType::NodeOnline,
            format!("Node #{} came online", node_id),
            name,
        )
    }

    /// Create a node offline event.
    pub fn node_offline(node_id: NodeId, name: &str) -> Self {
        Self::with_details(
            EventType::NodeOffline,
            format!("Node #{} went offline", node_id),
            name,
        )
    }

    /// Create a tape reserved event.
    pub fn tape_reserved(tape_number: u64, size_mb: u64, epochs: u64, authority: &str) -> Self {
        Self::with_details(
            EventType::TapeReserved,
            format!("Tape #{} reserved ({} MB, {} epochs)", tape_number, size_mb, epochs),
            authority,
        )
    }

    /// Create an epoch transition event.
    pub fn epoch_transition(epoch: u64, phase: &str) -> Self {
        Self::new(
            EventType::EpochTransition,
            format!("Epoch {} entered {} phase", epoch, phase),
        )
    }

    /// Create an info event.
    pub fn info(message: impl Into<String>) -> Self {
        Self::new(EventType::Info, message)
    }

    /// Create a warning event.
    pub fn warning(message: impl Into<String>) -> Self {
        Self::new(EventType::Warning, message)
    }

    /// Create an error event.
    pub fn error(message: impl Into<String>) -> Self {
        Self::new(EventType::Error, message)
    }
}
