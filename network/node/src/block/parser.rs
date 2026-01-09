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
    use super::super::test_utils::{encode_event, TestTransaction};
    use base64::Engine;
    use solana_transaction_status::{UiRawMessage, UiTransaction};
    use tape_api::event::EventType;
    use tape_api::instruction::TapeInstruction;

    // -------------------------------------------------------------------------
    // parse_transaction tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_parse_transaction_advance_epoch() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        // AdvanceEpoch has specific account layout, but we only need program ID
        let tx = TestTransaction::new()
            .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
            .with_event(EventType::EpochAdvanced, &event)
            .build();

        let result = parse_transaction(&tx).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.old_epoch, EpochNumber(5));
                assert_eq!(event.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }
    }

    #[test]
    fn test_parse_transaction_certify_track() {
        let track = Pubkey::new_unique();

        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        // CertifyTrack: account[4] is the track
        let tx = TestTransaction::new()
            .with_account(Pubkey::new_unique()) // 0: authority
            .with_account(Pubkey::new_unique()) // 1: system
            .with_account(Pubkey::new_unique()) // 2: epoch
            .with_account(Pubkey::new_unique()) // 3: node
            .with_account(track)                // 4: track
            .with_instruction(
                TapeInstruction::CertifyTrack,
                vec![0, 1, 2, 3, 4],
                vec![], // CertifyTrack has no additional data
            )
            .with_event(EventType::TrackCertified, &event)
            .build();

        let result = parse_transaction(&tx).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            ParsedInstruction::CertifyTrack {
                track: t,
                event: e,
            } => {
                assert_eq!(*t, track);
                assert_eq!(e.epoch, EpochNumber(10));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_parse_transaction_multiple_instructions() {
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let epoch_event = EpochAdvanced {
            old_epoch: EpochNumber(1),
            new_epoch: EpochNumber(2),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        let certify_event = TrackCertified {
            track,
            epoch: EpochNumber(2),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let tx = TestTransaction::new()
            .with_account(owner)                // 0
            .with_account(Pubkey::new_unique()) // 1
            .with_account(Pubkey::new_unique()) // 2
            .with_account(Pubkey::new_unique()) // 3
            .with_account(track)                // 4
            // First instruction: AdvanceEpoch
            .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
            .with_event(EventType::EpochAdvanced, &epoch_event)
            // Second instruction: CertifyTrack
            .with_instruction(TapeInstruction::CertifyTrack, vec![0, 1, 2, 3, 4], vec![])
            .with_event(EventType::TrackCertified, &certify_event)
            .build();

        let result = parse_transaction(&tx).unwrap();

        assert_eq!(result.len(), 2);

        match &result[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.new_epoch, EpochNumber(2));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }

        match &result[1] {
            ParsedInstruction::CertifyTrack { track: t, event } => {
                assert_eq!(*t, track);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_parse_transaction_failed_tx_skipped() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        let tx = TestTransaction::new()
            .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
            .with_event(EventType::EpochAdvanced, &event)
            .as_failed() // Mark as failed
            .build();

        // Failed transactions should be detected
        assert!(is_failed_transaction(&tx));

        // parse_transaction still parses it (filtering happens in parse_block)
        // But let's verify is_failed_transaction works
    }

    #[test]
    fn test_parse_transaction_delete_track_optional_event() {
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        // DeleteTrack without event (event is optional)
        let tx = TestTransaction::new()
            .with_account(owner)                // 0: owner
            .with_account(Pubkey::new_unique()) // 1: tape
            .with_account(track)                // 2: track
            .with_instruction(TapeInstruction::DeleteTrack, vec![0, 1, 2], vec![])
            // No event - it's optional
            .build();

        let result = parse_transaction(&tx).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            ParsedInstruction::DeleteTrack {
                owner: o,
                track: t,
                event,
            } => {
                assert_eq!(*o, owner);
                assert_eq!(*t, track);
                assert!(event.is_none());
            }
            _ => panic!("Expected DeleteTrack"),
        }
    }

    #[test]
    fn test_parse_transaction_non_tapedrive_instruction_ignored() {
        // Create a transaction with a non-tapedrive program
        let other_program = Pubkey::new_unique();

        let raw_message = UiRawMessage {
            header: solana_sdk::message::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![other_program.to_string()],
            recent_blockhash: "11111111111111111111111111111111".to_string(),
            instructions: vec![UiCompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: bs58::encode(&[0u8]).into_string(),
                stack_height: None,
            }],
            address_table_lookups: None,
        };

        let ui_tx = UiTransaction {
            signatures: vec!["sig".to_string()],
            message: UiMessage::Raw(raw_message),
        };

        let meta = UiTransactionStatusMeta {
            err: None,
            status: Ok(()),
            fee: 5000,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: OptionSerializer::None,
            log_messages: OptionSerializer::Some(vec![]),
            pre_token_balances: OptionSerializer::None,
            post_token_balances: OptionSerializer::None,
            rewards: OptionSerializer::None,
            loaded_addresses: OptionSerializer::None,
            return_data: OptionSerializer::None,
            compute_units_consumed: OptionSerializer::None,
            cost_units: OptionSerializer::None,
        };

        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Json(ui_tx),
            meta: Some(meta),
            version: None,
        };

        let result = parse_transaction(&tx).unwrap();
        assert!(result.is_empty()); // Non-tapedrive instructions ignored
    }

    // -------------------------------------------------------------------------
    // Original unit tests
    // -------------------------------------------------------------------------

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
        assert_eq!(pubkey.to_string(), "11111111111111111111111111111111");
    }

    // -------------------------------------------------------------------------
    // Event parsing tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_parse_epoch_advanced_event() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [10, 0, 0, 0, 0, 0, 0, 0],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(1000),
        };

        let log = encode_event(EventType::EpochAdvanced, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::EpochAdvanced(e) => {
                assert_eq!(e.old_epoch, EpochNumber(5));
                assert_eq!(e.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected EpochAdvanced event"),
        }
    }

    #[test]
    fn test_parse_track_certified_event() {
        let track = Pubkey::new_unique();
        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [5, 0, 0, 0, 0, 0, 0, 0],
            signer_weight: [100, 0, 0, 0, 0, 0, 0, 0],
        };

        let log = encode_event(EventType::TrackCertified, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::TrackCertified(e) => {
                assert_eq!(e.track, track);
                assert_eq!(e.epoch, EpochNumber(10));
            }
            _ => panic!("Expected TrackCertified event"),
        }
    }

    #[test]
    fn test_parse_track_registered_event() {
        let track = Pubkey::new_unique();
        let tape = Pubkey::new_unique();
        let event = TrackRegistered {
            track,
            tape,
            key: Hash::default(),
            size: StorageUnits(500),
            commitment: Hash::default(),
            epoch: EpochNumber(3),
        };

        let log = encode_event(EventType::TrackRegistered, &event);
        let parsed = parse_event_data(&log).unwrap().unwrap();

        match parsed {
            TapedriveEvent::TrackRegistered(e) => {
                assert_eq!(e.track, track);
                assert_eq!(e.tape, tape);
                assert_eq!(e.epoch, EpochNumber(3));
                assert_eq!(e.size, StorageUnits(500));
            }
            _ => panic!("Expected TrackRegistered event"),
        }
    }

    #[test]
    fn test_parse_invalid_event_data() {
        // Too short
        let result = parse_event_data("Program data: AAAA").unwrap();
        assert!(result.is_none());

        // Invalid base64
        let result = parse_event_data("Program data: !!!invalid!!!");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_event_type() {
        // Create data with unknown discriminator (0xFF)
        let mut data = vec![0xFFu8; 8];
        data.extend_from_slice(&[0u8; 32]); // Some padding
        let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
        let log = format!("Program data: {}", encoded);

        let result = parse_event_data(&log).unwrap();
        assert!(result.is_none()); // Unknown events are skipped, not errors
    }

    // -------------------------------------------------------------------------
    // Event/instruction merge tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_merge_advance_epoch() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(5),
            new_epoch: EpochNumber(6),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![TapedriveEvent::EpochAdvanced(event)];

        let merged = merge_instructions_and_events(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.old_epoch, EpochNumber(5));
                assert_eq!(event.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }
    }

    #[test]
    fn test_merge_certify_track() {
        let track = Pubkey::new_unique();
        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let instructions = vec![RawInstruction::CertifyTrack { track }];
        let events = vec![TapedriveEvent::TrackCertified(event)];

        let merged = merge_instructions_and_events(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::CertifyTrack { track: t, event } => {
                assert_eq!(*t, track);
                assert_eq!(event.epoch, EpochNumber(10));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_merge_advance_epoch_missing_event() {
        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![]; // No events!

        let result = merge_instructions_and_events(instructions, events);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
        }
    }

    #[test]
    fn test_merge_certify_track_missing_event() {
        let track = Pubkey::new_unique();
        let instructions = vec![RawInstruction::CertifyTrack { track }];
        let events = vec![]; // No events!

        let result = merge_instructions_and_events(instructions, events);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
        }
    }

    #[test]
    fn test_merge_multiple_instructions() {
        let track1 = Pubkey::new_unique();
        let track2 = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        // Create events
        let epoch_event = EpochAdvanced {
            old_epoch: EpochNumber(1),
            new_epoch: EpochNumber(2),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        let register_event = TrackRegistered {
            track: track1,
            tape: Pubkey::new_unique(),
            key: Hash::default(),
            size: StorageUnits(100),
            commitment: Hash::default(),
            epoch: EpochNumber(2),
        };

        let certify_event = TrackCertified {
            track: track2,
            epoch: EpochNumber(2),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        let instructions = vec![
            RawInstruction::AdvanceEpoch,
            RawInstruction::RegisterTrack {
                owner,
                track: track1,
                key: Hash::default(),
                root: Hash::default(),
                commitment: Hash::default(),
                size: StorageUnits(100),
            },
            RawInstruction::CertifyTrack { track: track2 },
        ];

        let events = vec![
            TapedriveEvent::EpochAdvanced(epoch_event),
            TapedriveEvent::TrackRegistered(register_event),
            TapedriveEvent::TrackCertified(certify_event),
        ];

        let merged = merge_instructions_and_events(instructions, events).unwrap();

        assert_eq!(merged.len(), 3);

        // Check AdvanceEpoch
        match &merged[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.new_epoch, EpochNumber(2));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }

        // Check RegisterTrack
        match &merged[1] {
            ParsedInstruction::RegisterTrack { track, event, .. } => {
                assert_eq!(*track, track1);
                assert!(event.is_some());
                assert_eq!(event.as_ref().unwrap().epoch, EpochNumber(2));
            }
            _ => panic!("Expected RegisterTrack"),
        }

        // Check CertifyTrack
        match &merged[2] {
            ParsedInstruction::CertifyTrack { track, event } => {
                assert_eq!(*track, track2);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_merge_optional_events() {
        // DeleteTrack, InvalidateTrack, etc. have optional events
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let instructions = vec![RawInstruction::DeleteTrack { owner, track }];
        let events = vec![]; // No event (optional)

        let merged = merge_instructions_and_events(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::DeleteTrack {
                track: t,
                event,
                ..
            } => {
                assert_eq!(*t, track);
                assert!(event.is_none()); // Event is optional
            }
            _ => panic!("Expected DeleteTrack"),
        }
    }

    #[test]
    fn test_merge_sync_epoch_no_event_needed() {
        // SyncEpoch gets data from instruction, not event
        let node = Pubkey::new_unique();
        let instructions = vec![RawInstruction::SyncEpoch {
            node,
            epoch: EpochNumber(5),
            spools_hash: Hash::default(),
        }];
        let events = vec![]; // SyncEpoch doesn't need event

        let merged = merge_instructions_and_events(instructions, events).unwrap();

        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::SyncEpoch {
                node: n,
                epoch,
                ..
            } => {
                assert_eq!(*n, node);
                assert_eq!(*epoch, EpochNumber(5));
            }
            _ => panic!("Expected SyncEpoch"),
        }
    }

    #[test]
    fn test_merge_wrong_event_type() {
        // AdvanceEpoch instruction with TrackCertified event should fail
        let instructions = vec![RawInstruction::AdvanceEpoch];
        let events = vec![TapedriveEvent::TrackCertified(TrackCertified {
            track: Pubkey::new_unique(),
            epoch: EpochNumber(1),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        })];

        let result = merge_instructions_and_events(instructions, events);
        assert!(result.is_err());
    }
}
