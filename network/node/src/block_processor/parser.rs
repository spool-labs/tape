//! Transaction and block parsing for tapedrive instructions.
//!
//! Parses Solana blocks to extract tapedrive-related instructions
//! and their corresponding events that affect node state.
//!
//! Events are emitted via `sol_log_data` and contain execution-time state,
//! which allows correct processing during historical catch-up.

use base64::Engine;
use bytemuck;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiCompiledInstruction, UiConfirmedBlock, UiInstruction, UiMessage, UiTransactionStatusMeta,
};
use tape_api::event::{
    EpochAdvanced, EventType, NodeJoinedCommittee, NodeRegistered, TapeDestroyed, TrackCertified,
    TrackDeleted, TrackInvalidated, TrackRegistered,
};
use tape_api::instruction::{self as ix, TapeInstruction};
use tape_core::prelude::*;
use tape_crypto::Hash;

/// Error type for block/transaction parsing.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("invalid instruction data")]
    InvalidData,

    #[error("invalid public key")]
    InvalidPubkey,

    #[error("missing account: {0}")]
    MissingAccount(&'static str),

    #[error("deserialization failed: {0}")]
    Deserialization(String),

    #[error("event/instruction mismatch: {0}")]
    EventMismatch(&'static str),

    #[error("invalid event data")]
    InvalidEvent,
}

/// Parsed tapedrive event from transaction logs.
#[derive(Debug, Clone)]
pub enum TapedriveEvent {
    EpochAdvanced(EpochAdvanced),
    TrackRegistered(TrackRegistered),
    TrackCertified(TrackCertified),
    TrackDeleted(TrackDeleted),
    TrackInvalidated(TrackInvalidated),
    TapeDestroyed(TapeDestroyed),
    NodeRegistered(NodeRegistered),
    NodeJoinedCommittee(NodeJoinedCommittee),
}

/// Parsed tapedrive instruction with associated event data.
///
/// Instructions now include event data that contains execution-time state,
/// eliminating the need to query current RPC state during catch-up.
#[derive(Debug, Clone)]
pub enum ParsedInstruction {
    // Epoch management
    AdvanceEpoch {
        /// Event contains old_epoch, new_epoch, committee info
        event: EpochAdvanced,
    },
    SyncEpoch {
        node: Pubkey,
        epoch: EpochNumber,
        spools_hash: Hash,
    },

    // Track management
    RegisterTrack {
        owner: Pubkey,
        track: Pubkey,
        key: Hash,
        root: Hash,
        commitment: Hash,
        size: StorageUnits,
        /// Event contains registration epoch
        event: Option<TrackRegistered>,
    },
    DeleteTrack {
        owner: Pubkey,
        track: Pubkey,
        /// Event contains track/tape info
        event: Option<TrackDeleted>,
    },
    CertifyTrack {
        track: Pubkey,
        /// Event contains certification epoch (fixes TODO!)
        event: TrackCertified,
    },
    InvalidateTrack {
        track: Pubkey,
        /// Event contains invalidation epoch
        event: Option<TrackInvalidated>,
    },

    // Tape management
    ReserveTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    DestroyTape {
        owner: Pubkey,
        tape: Pubkey,
        /// Event contains tape/authority info
        event: Option<TapeDestroyed>,
    },

    // Node management
    RegisterNode {
        authority: Pubkey,
        node: Pubkey,
        /// Event contains node ID, registration epoch
        event: Option<NodeRegistered>,
    },
    JoinNetwork {
        node: Pubkey,
        /// Event contains activation epoch, stake
        event: Option<NodeJoinedCommittee>,
    },
}

/// Result of parsing a single block.
#[derive(Debug, Default)]
pub struct ParsedBlock {
    /// Parsed instructions from successful transactions.
    pub instructions: Vec<ParsedInstruction>,
    /// Number of transactions processed.
    pub tx_count: usize,
    /// Number of failed transactions skipped.
    pub failed_tx_count: usize,
}

/// Parse a confirmed block for tapedrive instructions.
pub fn parse_block(block: &UiConfirmedBlock) -> Result<ParsedBlock, ParseError> {
    let mut result = ParsedBlock::default();

    let Some(transactions) = &block.transactions else {
        return Ok(result);
    };

    for tx in transactions {
        if is_failed_transaction(tx) {
            result.failed_tx_count += 1;
            continue;
        }

        result.tx_count += 1;

        // Parse instructions and events from this transaction
        let parsed = parse_transaction(tx)?;
        result.instructions.extend(parsed);
    }

    Ok(result)
}

/// Parse a single transaction for tapedrive instructions and events.
fn parse_transaction(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let EncodedTransaction::Json(ui_tx) = &tx.transaction else {
        return Ok(Vec::new());
    };

    let UiMessage::Raw(raw_message) = &ui_tx.message else {
        return Ok(Vec::new());
    };

    let account_keys = &raw_message.account_keys;

    // First, extract events from log messages
    let events = if let Some(meta) = &tx.meta {
        parse_log_messages(meta)?
    } else {
        Vec::new()
    };

    // Then parse instructions
    let mut raw_instructions = Vec::new();

    // Process top-level instructions
    for ix in &raw_message.instructions {
        if let Some(parsed) = parse_raw_instruction(ix, account_keys)? {
            raw_instructions.push(parsed);
        }
    }

    // Process inner instructions (CPIs)
    if let Some(meta) = &tx.meta {
        let inner = parse_inner_instructions(account_keys, meta)?;
        raw_instructions.extend(inner);
    }

    // Match instructions with events
    merge_instructions_and_events(raw_instructions, events)
}

/// Raw instruction before event matching (internal use).
#[derive(Debug)]
enum RawInstruction {
    AdvanceEpoch,
    SyncEpoch {
        node: Pubkey,
        epoch: EpochNumber,
        spools_hash: Hash,
    },
    RegisterTrack {
        owner: Pubkey,
        track: Pubkey,
        key: Hash,
        root: Hash,
        commitment: Hash,
        size: StorageUnits,
    },
    DeleteTrack {
        owner: Pubkey,
        track: Pubkey,
    },
    CertifyTrack {
        track: Pubkey,
    },
    InvalidateTrack {
        track: Pubkey,
    },
    ReserveTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    DestroyTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    RegisterNode {
        authority: Pubkey,
        node: Pubkey,
    },
    JoinNetwork {
        node: Pubkey,
    },
}

/// Parse events from transaction log messages.
fn parse_log_messages(meta: &UiTransactionStatusMeta) -> Result<Vec<TapedriveEvent>, ParseError> {
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
            if let Some(event) = parse_event_data(log)? {
                events.push(event);
            }
        }
    }

    Ok(events)
}

/// Parse event data from a "Program data:" log line.
fn parse_event_data(log: &str) -> Result<Option<TapedriveEvent>, ParseError> {
    let Some(encoded_data) = log.strip_prefix("Program data: ") else {
        return Ok(None);
    };

    let data = base64::engine::general_purpose::STANDARD
        .decode(encoded_data)
        .map_err(|_| ParseError::InvalidEvent)?;

    if data.len() < 8 {
        return Ok(None);
    }

    // First byte of discriminator is the EventType
    let discriminator = data[0];
    let event_type = EventType::try_from(discriminator).ok();

    let Some(event_type) = event_type else {
        return Ok(None);
    };

    // Event data starts after 8-byte discriminator
    let event_data = &data[8..];

    match event_type {
        EventType::EpochAdvanced => {
            let event = bytemuck::try_from_bytes::<EpochAdvanced>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::EpochAdvanced(*event)))
        }
        EventType::TrackRegistered => {
            let event = bytemuck::try_from_bytes::<TrackRegistered>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackRegistered(*event)))
        }
        EventType::TrackCertified => {
            let event = bytemuck::try_from_bytes::<TrackCertified>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackCertified(*event)))
        }
        EventType::TrackDeleted => {
            let event = bytemuck::try_from_bytes::<TrackDeleted>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackDeleted(*event)))
        }
        EventType::TrackInvalidated => {
            let event = bytemuck::try_from_bytes::<TrackInvalidated>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TrackInvalidated(*event)))
        }
        EventType::TapeDestroyed => {
            let event = bytemuck::try_from_bytes::<TapeDestroyed>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::TapeDestroyed(*event)))
        }
        EventType::NodeRegistered => {
            let event = bytemuck::try_from_bytes::<NodeRegistered>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::NodeRegistered(*event)))
        }
        EventType::NodeJoinedCommittee => {
            let event = bytemuck::try_from_bytes::<NodeJoinedCommittee>(event_data)
                .map_err(|_| ParseError::InvalidEvent)?;
            Ok(Some(TapedriveEvent::NodeJoinedCommittee(*event)))
        }
        // Events we don't need to track
        _ => Ok(None),
    }
}

/// Merge raw instructions with their corresponding events.
fn merge_instructions_and_events(
    instructions: Vec<RawInstruction>,
    mut events: Vec<TapedriveEvent>,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut result = Vec::new();
    let mut event_iter = events.drain(..);

    for ix in instructions {
        let parsed = match ix {
            RawInstruction::AdvanceEpoch => {
                // AdvanceEpoch always has an event
                let event = match event_iter.next() {
                    Some(TapedriveEvent::EpochAdvanced(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected EpochAdvanced event")),
                };
                ParsedInstruction::AdvanceEpoch { event }
            }

            RawInstruction::SyncEpoch {
                node,
                epoch,
                spools_hash,
            } => {
                // SyncEpoch emits NodeSynced but we get epoch from instruction
                // Skip matching event, we have the data we need
                ParsedInstruction::SyncEpoch {
                    node,
                    epoch,
                    spools_hash,
                }
            }

            RawInstruction::RegisterTrack {
                owner,
                track,
                key,
                root,
                commitment,
                size,
            } => {
                // Try to get matching event
                let event = match event_iter.next() {
                    Some(TapedriveEvent::TrackRegistered(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::RegisterTrack {
                    owner,
                    track,
                    key,
                    root,
                    commitment,
                    size,
                    event,
                }
            }

            RawInstruction::CertifyTrack { track } => {
                // CertifyTrack always has an event with the epoch
                let event = match event_iter.next() {
                    Some(TapedriveEvent::TrackCertified(e)) => e,
                    _ => return Err(ParseError::EventMismatch("expected TrackCertified event")),
                };
                ParsedInstruction::CertifyTrack { track, event }
            }

            RawInstruction::DeleteTrack { owner, track } => {
                let event = match event_iter.next() {
                    Some(TapedriveEvent::TrackDeleted(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::DeleteTrack {
                    owner,
                    track,
                    event,
                }
            }

            RawInstruction::InvalidateTrack { track } => {
                let event = match event_iter.next() {
                    Some(TapedriveEvent::TrackInvalidated(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::InvalidateTrack { track, event }
            }

            RawInstruction::ReserveTape { owner, tape } => {
                // TapeReserved event exists but we don't need to track it
                ParsedInstruction::ReserveTape { owner, tape }
            }

            RawInstruction::DestroyTape { owner, tape } => {
                let event = match event_iter.next() {
                    Some(TapedriveEvent::TapeDestroyed(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::DestroyTape { owner, tape, event }
            }

            RawInstruction::RegisterNode { authority, node } => {
                let event = match event_iter.next() {
                    Some(TapedriveEvent::NodeRegistered(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::RegisterNode {
                    authority,
                    node,
                    event,
                }
            }

            RawInstruction::JoinNetwork { node } => {
                let event = match event_iter.next() {
                    Some(TapedriveEvent::NodeJoinedCommittee(e)) => Some(e),
                    _ => None,
                };
                ParsedInstruction::JoinNetwork { node, event }
            }
        };

        result.push(parsed);
    }

    Ok(result)
}

/// Parse inner instructions from transaction metadata.
fn parse_inner_instructions(
    account_keys: &[String],
    meta: &UiTransactionStatusMeta,
) -> Result<Vec<RawInstruction>, ParseError> {
    let mut instructions = Vec::new();

    let OptionSerializer::Some(inner_instructions) = &meta.inner_instructions else {
        return Ok(instructions);
    };

    for inner_ix_set in inner_instructions {
        for inner_ix in &inner_ix_set.instructions {
            if let UiInstruction::Compiled(compiled_ix) = inner_ix {
                if let Some(parsed) = parse_raw_instruction(compiled_ix, account_keys)? {
                    instructions.push(parsed);
                }
            }
        }
    }

    Ok(instructions)
}

/// Parse a single compiled instruction into a RawInstruction.
fn parse_raw_instruction(
    ix: &UiCompiledInstruction,
    account_keys: &[String],
) -> Result<Option<RawInstruction>, ParseError> {
    // Get the program ID
    let program_id_index = ix.program_id_index as usize;
    if program_id_index >= account_keys.len() {
        return Ok(None);
    }

    let program_id: Pubkey = account_keys[program_id_index]
        .parse()
        .map_err(|_| ParseError::InvalidPubkey)?;

    // Only process tapedrive program instructions
    if program_id != tape_api::program::tapedrive::ID {
        return Ok(None);
    }

    // Decode instruction data
    let ix_data = bs58::decode(&ix.data)
        .into_vec()
        .map_err(|_| ParseError::InvalidData)?;

    if ix_data.is_empty() {
        return Ok(None);
    }

    // Parse based on discriminator
    let discriminator = ix_data[0];
    let ix_type = TapeInstruction::try_from(discriminator).ok();

    let Some(ix_type) = ix_type else {
        return Ok(None);
    };

    // Helper to get account pubkey at index
    let get_account = |idx: usize| -> Result<Pubkey, ParseError> {
        let account_idx =
            *ix.accounts.get(idx).ok_or(ParseError::MissingAccount("account"))? as usize;
        if account_idx >= account_keys.len() {
            return Err(ParseError::MissingAccount("account index out of bounds"));
        }
        account_keys[account_idx]
            .parse()
            .map_err(|_| ParseError::InvalidPubkey)
    };

    match ix_type {
        TapeInstruction::AdvanceEpoch => Ok(Some(RawInstruction::AdvanceEpoch)),

        TapeInstruction::SyncEpoch => {
            let node = get_account(3)?;
            let args = ix::SyncEpoch::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            Ok(Some(RawInstruction::SyncEpoch {
                node,
                epoch: EpochNumber::unpack(args.epoch),
                spools_hash: args.spools,
            }))
        }

        TapeInstruction::RegisterTrack => {
            let owner = get_account(0)?;
            let track = get_account(3)?;
            let args = ix::RegisterTrack::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            Ok(Some(RawInstruction::RegisterTrack {
                owner,
                track,
                key: args.key,
                root: args.root,
                commitment: args.commitment,
                size: StorageUnits::unpack(args.size),
            }))
        }

        TapeInstruction::DeleteTrack => {
            let owner = get_account(0)?;
            let track = get_account(2)?;
            Ok(Some(RawInstruction::DeleteTrack { owner, track }))
        }

        TapeInstruction::CertifyTrack => {
            let track = get_account(4)?;
            Ok(Some(RawInstruction::CertifyTrack { track }))
        }

        TapeInstruction::InvalidateTrack => {
            let track = get_account(2)?;
            Ok(Some(RawInstruction::InvalidateTrack { track }))
        }

        TapeInstruction::ReserveTape => {
            let owner = get_account(0)?;
            let tape = get_account(2)?;
            Ok(Some(RawInstruction::ReserveTape { owner, tape }))
        }

        TapeInstruction::DestroyTape => {
            let owner = get_account(0)?;
            let tape = get_account(1)?;
            Ok(Some(RawInstruction::DestroyTape { owner, tape }))
        }

        TapeInstruction::RegisterNode => {
            let authority = get_account(0)?;
            let node = get_account(4)?;
            Ok(Some(RawInstruction::RegisterNode { authority, node }))
        }

        TapeInstruction::JoinNetwork => {
            let node = get_account(3)?;
            Ok(Some(RawInstruction::JoinNetwork { node }))
        }

        // Instructions we don't need to track for node operation
        _ => Ok(None),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_block() {
        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: None,
            signatures: None,
            rewards: None,
            block_time: None,
            block_height: None,
            num_reward_partitions: None,
        };

        let result = parse_block(&block).unwrap();
        assert!(result.instructions.is_empty());
        assert_eq!(result.tx_count, 0);
    }

    #[test]
    fn test_is_program_invoke() {
        assert!(is_program_invoke(
            "Program 11111111111111111111111111111111 invoke [1]"
        ));
        assert!(!is_program_invoke("Program log: Hello"));
    }

    #[test]
    fn test_is_program_data() {
        assert!(is_program_data("Program data: SGVsbG8gV29ybGQ="));
        assert!(!is_program_data("Program log: Hello"));
    }

    #[test]
    fn test_get_program_id() {
        let log = "Program 11111111111111111111111111111111 invoke [1]";
        let pubkey = get_program_id(log).unwrap();
        assert_eq!(
            pubkey.to_string(),
            "11111111111111111111111111111111"
        );
    }
}
