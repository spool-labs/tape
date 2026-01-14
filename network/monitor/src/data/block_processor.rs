//! Block processor for real-time event streaming.
//!
//! Parses Solana blocks to extract tapedrive-related events directly from
//! transaction logs, providing real-time visibility into network activity.
//!
//! This is adapted from the tape-node block parser to provide actual on-chain
//! events rather than polling-based change detection.

use base64::Engine;
use rpc_client::{Rpc, RpcClient};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiConfirmedBlock, UiMessage, UiTransactionStatusMeta,
};
use tape_api::event::{
    EpochAdvanced, EventType as TapeEventType, NodeJoinedCommittee, NodeRegistered, NodeSynced,
    TapeDestroyed, TapeReserved, TrackCertified, TrackDeleted, TrackRegistered,
};

use crate::app::{EventType, NetworkEvent};

// ============================================================================
// Tapedrive Event Types (from on-chain program logs)
// ============================================================================

/// Parsed tapedrive event from transaction logs.
#[derive(Debug, Clone)]
pub enum TapedriveEvent {
    EpochAdvanced(EpochAdvanced),
    TrackRegistered(TrackRegistered),
    TrackCertified(TrackCertified),
    TrackDeleted(TrackDeleted),
    TapeReserved(TapeReserved),
    TapeDestroyed(TapeDestroyed),
    NodeRegistered(NodeRegistered),
    NodeJoinedCommittee(NodeJoinedCommittee),
    NodeSynced(NodeSynced),
}

impl TapedriveEvent {
    /// Convert to a NetworkEvent for display in the UI.
    pub fn to_network_event(&self) -> NetworkEvent {
        match self {
            TapedriveEvent::EpochAdvanced(e) => NetworkEvent::new(
                EventType::EpochTransition,
                format!(
                    "Epoch {} -> {} (committee: {})",
                    e.old_epoch,
                    e.new_epoch,
                    u64::from_le_bytes(e.committee_size)
                ),
                "",
            ),

            TapedriveEvent::TrackRegistered(e) => NetworkEvent::new(
                EventType::TrackRegistered,
                format!("Track registered ({}MB)", e.size.0),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TrackCertified(e) => NetworkEvent::new(
                EventType::TrackCertified,
                format!("Track certified (epoch {})", e.epoch),
                truncate_pubkey(&e.track),
            ),

            TapedriveEvent::TrackDeleted(e) => NetworkEvent::new(
                EventType::TrackRegistered, // Reuse type, icon is appropriate
                format!("Track deleted ({}MB)", e.size.0),
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
                "Tape destroyed".to_string(),
                truncate_pubkey(&e.authority),
            ),

            TapedriveEvent::NodeRegistered(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!("Node #{} registered", e.id),
                truncate_pubkey(&e.node),
            ),

            TapedriveEvent::NodeJoinedCommittee(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!(
                    "Node joined committee (epoch {}, stake: {})",
                    e.activation_epoch,
                    u64::from_le_bytes(e.stake) / 1_000_000
                ),
                "",
            ),

            TapedriveEvent::NodeSynced(e) => NetworkEvent::new(
                EventType::NodeOnline,
                format!("Node #{} synced (epoch {})", e.id, e.epoch),
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

// ============================================================================
// Block Parsing
// ============================================================================

/// Result of parsing a single block.
#[derive(Debug, Default)]
pub struct ParsedBlock {
    /// Parsed events from successful transactions.
    pub events: Vec<TapedriveEvent>,
    /// Number of transactions processed.
    pub tx_count: usize,
}

/// Parse a confirmed block for tapedrive events.
pub fn parse_block(block: &UiConfirmedBlock) -> ParsedBlock {
    let mut result = ParsedBlock::default();

    let Some(transactions) = &block.transactions else {
        return result;
    };

    for tx in transactions {
        if is_failed_transaction(tx) {
            continue;
        }

        result.tx_count += 1;

        // Parse events from this transaction
        if let Ok(events) = parse_transaction_events(tx) {
            result.events.extend(events);
        }
    }

    result
}

/// Parse events from a single transaction's log messages.
fn parse_transaction_events(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<Vec<TapedriveEvent>, ()> {
    let EncodedTransaction::Json(ui_tx) = &tx.transaction else {
        return Ok(Vec::new());
    };

    let UiMessage::Raw(_) = &ui_tx.message else {
        return Ok(Vec::new());
    };

    // Extract events from log messages
    let Some(meta) = &tx.meta else {
        return Ok(Vec::new());
    };

    parse_log_messages(meta)
}

/// Parse events from transaction log messages.
fn parse_log_messages(meta: &UiTransactionStatusMeta) -> Result<Vec<TapedriveEvent>, ()> {
    let mut events = Vec::new();

    let OptionSerializer::Some(log_messages) = &meta.log_messages else {
        return Ok(events);
    };

    let mut program_stack: Vec<Pubkey> = Vec::new();

    for log in log_messages {
        if is_program_invoke(log) {
            if let Some(program_id) = get_program_id(log) {
                program_stack.push(program_id);
            }
        } else if is_program_success(log) || is_program_failure(log) {
            program_stack.pop();
        }

        // Only parse events from tapedrive program
        let is_tapedrive = program_stack.last() == Some(&tape_api::program::tapedrive::ID);

        if is_tapedrive && is_program_data(log) {
            if let Some(event) = parse_event_data(log) {
                events.push(event);
            }
        }
    }

    Ok(events)
}

/// Parse event data from a "Program data:" log line.
fn parse_event_data(log: &str) -> Option<TapedriveEvent> {
    let encoded_data = log.strip_prefix("Program data: ")?;

    let data = base64::engine::general_purpose::STANDARD
        .decode(encoded_data)
        .ok()?;

    if data.len() < 8 {
        return None;
    }

    // First byte of discriminator is the EventType
    let discriminator = data[0];
    let event_type = TapeEventType::try_from(discriminator).ok()?;

    // Event data starts after 8-byte discriminator
    let event_data = &data[8..];

    match event_type {
        TapeEventType::EpochAdvanced => {
            let event = bytemuck::try_from_bytes::<EpochAdvanced>(event_data).ok()?;
            Some(TapedriveEvent::EpochAdvanced(*event))
        }
        TapeEventType::TrackRegistered => {
            let event = bytemuck::try_from_bytes::<TrackRegistered>(event_data).ok()?;
            Some(TapedriveEvent::TrackRegistered(*event))
        }
        TapeEventType::TrackCertified => {
            let event = bytemuck::try_from_bytes::<TrackCertified>(event_data).ok()?;
            Some(TapedriveEvent::TrackCertified(*event))
        }
        TapeEventType::TrackDeleted => {
            let event = bytemuck::try_from_bytes::<TrackDeleted>(event_data).ok()?;
            Some(TapedriveEvent::TrackDeleted(*event))
        }
        TapeEventType::TapeReserved => {
            let event = bytemuck::try_from_bytes::<TapeReserved>(event_data).ok()?;
            Some(TapedriveEvent::TapeReserved(*event))
        }
        TapeEventType::TapeDestroyed => {
            let event = bytemuck::try_from_bytes::<TapeDestroyed>(event_data).ok()?;
            Some(TapedriveEvent::TapeDestroyed(*event))
        }
        TapeEventType::NodeRegistered => {
            let event = bytemuck::try_from_bytes::<NodeRegistered>(event_data).ok()?;
            Some(TapedriveEvent::NodeRegistered(*event))
        }
        TapeEventType::NodeJoinedCommittee => {
            let event = bytemuck::try_from_bytes::<NodeJoinedCommittee>(event_data).ok()?;
            Some(TapedriveEvent::NodeJoinedCommittee(*event))
        }
        TapeEventType::NodeSynced => {
            let event = bytemuck::try_from_bytes::<NodeSynced>(event_data).ok()?;
            Some(TapedriveEvent::NodeSynced(*event))
        }
        // Events we don't need to display
        _ => None,
    }
}

/// Check if a transaction failed.
fn is_failed_transaction(tx: &EncodedTransactionWithStatusMeta) -> bool {
    tx.meta
        .as_ref()
        .map(|meta| meta.status.is_err())
        .unwrap_or(true)
}

/// Check if log indicates program invoke.
fn is_program_invoke(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" invoke ")
}

/// Check if log indicates program success.
fn is_program_success(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" success")
}

/// Check if log indicates program failure.
fn is_program_failure(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" failed")
}

/// Check if log contains program data (event).
fn is_program_data(log: &str) -> bool {
    log.starts_with("Program data: ")
}

/// Extract program ID from invoke log.
fn get_program_id(log: &str) -> Option<Pubkey> {
    let parts: Vec<&str> = log.split_whitespace().collect();
    if parts.len() >= 3 {
        return parts[1].parse::<Pubkey>().ok();
    }
    None
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
