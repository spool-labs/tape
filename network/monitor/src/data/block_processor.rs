//! Block processor for real-time event streaming.
//!
//! Uses tape-blocks for parsing and provides UI-specific functionality.

use rpc_client::{Rpc, RpcClient};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiConfirmedBlock;

// Re-export event type from shared crate
pub use tape_blocks::TapedriveEvent;

use crate::app::{EventType, NetworkEvent};

// ============================================================================
// UI Conversion (monitor-specific)
// ============================================================================

/// Extension trait for converting events to UI display format.
pub trait ToNetworkEvent {
    fn to_network_event(&self) -> NetworkEvent;
}

impl ToNetworkEvent for TapedriveEvent {
    /// Convert to a NetworkEvent for display in the UI.
    fn to_network_event(&self) -> NetworkEvent {
        match self {
            TapedriveEvent::EpochAdvanced(e) => NetworkEvent::new(
                EventType::EpochTransition,
                format!(
                    "Epoch E{} -> E{} (committee: {})",
                    e.old_epoch.0,
                    e.new_epoch.0,
                    u64::from_le_bytes(e.committee_size)
                ),
                "",
            ),

            TapedriveEvent::TrackRegistered(e) => NetworkEvent::new(
                EventType::TrackRegistered,
                format!("Track registered ({})", format_size(e.size)),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TrackCertified(e) => NetworkEvent::new(
                EventType::TrackCertified,
                format!("Track certified (E{})", e.epoch.0),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TrackDeleted(e) => NetworkEvent::new(
                EventType::TrackRegistered, // Reuse type, icon is appropriate
                format!("Track deleted ({})", format_size(e.size)),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TrackInvalidated(e) => NetworkEvent::new(
                EventType::TrackRegistered,
                format!("Track invalidated (E{})", e.epoch.0),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TapeReserved(e) => NetworkEvent::new(
                EventType::TapeReserved,
                format!(
                    "Tape reserved ({}MB, epochs {}-{})",
                    e.capacity.0, e.active_epoch.0, e.expiry_epoch.0
                ),
                truncate_pubkey(&e.tape),
            ),

            TapedriveEvent::TapeDestroyed(e) => NetworkEvent::new(
                EventType::TapeReserved, // Reuse type
                "Tape destroyed",
                truncate_pubkey(&e.authority),
            ),

            TapedriveEvent::NodeRegistered(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!("Node #{} registered", e.id.0),
                truncate_pubkey(&e.node),
            ),

            TapedriveEvent::NodeJoinedCommittee(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!(
                    "Node joined committee (E{}, stake: {} TAPE)",
                    e.activation_epoch.0,
                    u64::from_le_bytes(e.stake) / 1_000_000
                ),
                "",
            ),

            TapedriveEvent::NodeSynced(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!("Node #{} synced (E{})", e.id.0, e.epoch.0),
                truncate_pubkey(&e.node),
            ),
        }
    }
}

/// Truncate a pubkey for display.
fn truncate_pubkey(pubkey: &Pubkey) -> String {
    let s = pubkey.to_string();
    if s.len() > 12 {
        format!("{}...{}", &s[..4], &s[s.len() - 4..])
    } else {
        s
    }
}

/// Format StorageUnits (MB) for human-readable display.
fn format_size(size: tape_core::types::StorageUnits) -> String {
    let mb = size.0;
    if mb >= 1_000 {
        format!("{:.1} GB", mb as f64 / 1_000.0)
    } else {
        format!("{} MB", mb)
    }
}

// ============================================================================
// Block Parsing (using shared crate)
// ============================================================================

/// Result of parsing a single block (monitor-specific view).
#[derive(Debug, Default)]
pub struct ParsedBlock {
    /// Parsed events from successful transactions.
    pub events: Vec<TapedriveEvent>,
    /// Number of transactions processed.
    pub tx_count: usize,
}

/// Parse a confirmed block for tapedrive events (monitor interface).
pub fn parse_block(block: &UiConfirmedBlock) -> ParsedBlock {
    match tape_blocks::parse(block) {
        Ok(raw) => ParsedBlock {
            events: raw.events,
            tx_count: raw.tx_count,
        },
        Err(_) => ParsedBlock::default(),
    }
}

// ============================================================================
// Block Processor State
// ============================================================================

/// Block processor that tracks slot position and fetches blocks.
#[derive(Debug)]
pub struct BlockProcessor {
    /// Last processed slot.
    last_slot: u64,
}

impl BlockProcessor {
    /// Create a new block processor starting from a given slot.
    pub fn new(start_slot: u64) -> Self {
        Self {
            last_slot: start_slot,
        }
    }

    /// Get the last processed slot.
    pub fn last_slot(&self) -> u64 {
        self.last_slot
    }

    /// Update the last processed slot.
    pub fn set_last_slot(&mut self, slot: u64) {
        self.last_slot = slot;
    }

    /// Process a range of slots and return events.
    /// Returns (events, new_last_slot).
    pub async fn process_slots<R: Rpc>(
        &mut self,
        rpc: &RpcClient<R>,
        latest_slot: u64,
        max_slots: u64,
    ) -> (Vec<TapedriveEvent>, u64) {
        let mut all_events = Vec::new();

        if latest_slot <= self.last_slot {
            return (all_events, self.last_slot);
        }

        let start_slot = self.last_slot + 1;
        let end_slot = latest_slot.min(start_slot + max_slots - 1);

        for slot in start_slot..=end_slot {
            match rpc.get_block(slot).await {
                Ok(block) => {
                    let parsed = parse_block(&block);
                    all_events.extend(parsed.events);
                }
                Err(e) => {
                    // SlotSkipped errors are normal
                    let err_str = format!("{}", e);
                    if !err_str.contains("SlotSkipped") && !err_str.contains("was skipped") {
                        tracing::debug!(slot = slot, error = %e, "Failed to fetch block");
                    }
                }
            }
            self.last_slot = slot;
        }

        (all_events, self.last_slot)
    }
}
