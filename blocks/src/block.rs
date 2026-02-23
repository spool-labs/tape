//! Block-level parsing.

use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiConfirmedBlock, UiInstruction, UiMessage, UiTransactionStatusMeta,
};

use crate::error::ParseError;
use crate::event::{parse_event_data, TapedriveEvent};
use crate::helpers::{
    get_program_id, is_program_data, is_program_failure, is_program_invoke, is_program_success,
};
use crate::instruction::{parse_raw_instruction, ParsedInstruction, RawInstruction};
use crate::merge::merge;

/// Result of parsing a single block.
///
/// Contains BOTH raw instructions AND events, kept separate.
/// Use `merge()` if you need merged output.
#[derive(Debug, Default)]
pub struct ParsedTransaction {
    /// Raw instructions for this transaction.
    pub raw_instructions: Vec<RawInstruction>,
    /// Parsed events for this transaction.
    pub events: Vec<TapedriveEvent>,
}

/// Result of parsing a single block.
///
/// Contains BOTH flattened and per-transaction instruction/event streams.
#[derive(Debug, Default)]
pub struct ParsedBlock {
    /// Raw instructions (before event matching).
    pub raw_instructions: Vec<RawInstruction>,
    /// All parsed events.
    pub events: Vec<TapedriveEvent>,
    /// Parsed instruction/event streams grouped per transaction.
    pub transactions: Vec<ParsedTransaction>,
    /// Number of transactions processed.
    pub tx_count: usize,
    /// Number of failed transactions skipped.
    pub failed_tx_count: usize,
}

/// Parse a confirmed block for tapedrive instructions and events.
///
/// Returns instructions and events separately. Consumers can:
/// - Use `parsed.events` directly for event-only processing (monitoring)
/// - Use `parse_and_merge(block)` for node-safe, per-transaction merged
///   instructions
///
/// # Example
/// ```ignore
/// let parsed = tape_blocks::parse(&block)?;
///
/// // Event-only usage (monitor):
/// for event in parsed.events {
///     println!("{:?}", event);
/// }
///
/// // Merged usage (node):
/// let merged = tape_blocks::merge(parsed.raw_instructions, parsed.events)?;
/// ```
pub fn parse(block: &UiConfirmedBlock) -> Result<ParsedBlock, ParseError> {
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
        let (instructions, events) = parse_transaction(tx)?;
        result.transactions.push(ParsedTransaction {
            raw_instructions: instructions.clone(),
            events: events.clone(),
        });
        result.raw_instructions.extend(instructions);
        result.events.extend(events);
    }

    Ok(result)
}

/// Parse a confirmed block and merge parsed instructions for each transaction.
///
/// Instruction/event alignment is performed on a per-transaction basis so
/// same-type instructions or events in one block cannot cross-map across
/// different transactions.
pub fn parse_and_merge(block: &UiConfirmedBlock) -> Result<Vec<ParsedInstruction>, ParseError> {
    let parsed = parse(block)?;
    merge_transactions(&parsed.transactions)
}

fn merge_transactions(
    transactions: &[ParsedTransaction],
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut instructions = Vec::new();
    for tx in transactions {
        instructions.extend(merge(tx.raw_instructions.clone(), tx.events.clone())?);
    }
    Ok(instructions)
}

/// Parse a single transaction for tapedrive instructions and events.
fn parse_transaction(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<(Vec<RawInstruction>, Vec<TapedriveEvent>), ParseError> {
    let EncodedTransaction::Json(ui_tx) = &tx.transaction else {
        return Ok((Vec::new(), Vec::new()));
    };

    let UiMessage::Raw(raw_message) = &ui_tx.message else {
        return Ok((Vec::new(), Vec::new()));
    };

    let account_keys = &raw_message.account_keys;

    // Extract events from log messages
    let events = if let Some(meta) = &tx.meta {
        parse_log_messages(meta)?
    } else {
        Vec::new()
    };

    // Parse instructions
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

    Ok((raw_instructions, events))
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

/// Check if a transaction failed.
fn is_failed_transaction(tx: &EncodedTransactionWithStatusMeta) -> bool {
    tx.meta
        .as_ref()
        .map(|meta| meta.status.is_err())
        .unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use solana_transaction_status::UiConfirmedBlock;
    use tape_api::event::{EpochAdvanced, NodeSynced};

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

        let result = parse(&block).unwrap();
        assert!(result.raw_instructions.is_empty());
        assert!(result.events.is_empty());
        assert_eq!(result.tx_count, 0);
    }

    #[test]
    fn test_merge_transactions_keeps_transaction_boundaries_for_optional_events() {
        use tape_core::erasure::SPOOL_GROUP_SIZE;
        use crate::event::TapedriveEvent;
        use tape_api::event::TrackRegistered;

        let tx1_track = Pubkey::new_unique();
        let tx2_track = Pubkey::new_unique();

        let tx1 = ParsedTransaction {
            raw_instructions: vec![RawInstruction::RegisterTrack {
                owner: Pubkey::new_unique(),
                track: tx1_track,
                key: tape_crypto::Hash::default(),
                root: tape_crypto::Hash::default(),
                commitment: tape_crypto::Hash::default(),
                size: 1_024u64.into(),
            }],
            events: vec![],
        };

        let track_event = TrackRegistered {
            track: tx2_track,
            tape: Pubkey::new_unique(),
            key: tape_crypto::Hash::default(),
            size: 1_024u64.into(),
            commitment: tape_crypto::Hash::default(),
            epoch: 2.into(),
            profile: tape_core::encoding::EncodingProfile::clay_default(),
            spool_group: 0u64.to_le_bytes(),
            stripe_size: 0u64.to_le_bytes(),
            stripe_count: 0u64.to_le_bytes(),
            leaves: [tape_crypto::Hash::default(); SPOOL_GROUP_SIZE],
        };

        let tx2 = ParsedTransaction {
            raw_instructions: vec![RawInstruction::RegisterTrack {
                owner: Pubkey::new_unique(),
                track: tx2_track,
                key: tape_crypto::Hash::default(),
                root: tape_crypto::Hash::default(),
                commitment: tape_crypto::Hash::default(),
                size: 1_024u64.into(),
            }],
            events: vec![TapedriveEvent::TrackRegistered(track_event)],
        };

        let parsed = ParsedBlock {
            transactions: vec![tx1, tx2],
            ..ParsedBlock::default()
        };

        let merged = merge_transactions(&parsed.transactions).unwrap();
        assert_eq!(merged.len(), 2);
        match &merged[0] {
            ParsedInstruction::RegisterTrack {
                event: first_event,
                ..
            } => assert!(first_event.is_none()),
            _ => panic!("expected first instruction to be register track"),
        }
        match &merged[1] {
            ParsedInstruction::RegisterTrack {
                event: second_event,
                ..
            } => {
                assert!(second_event.is_some());
                assert_eq!(second_event.as_ref().unwrap().track, tx2_track);
            }
            _ => panic!("expected second instruction to be register track"),
        }
    }

    #[test]
    fn test_merge_transactions_preserves_instruction_order() {
        let tx1 = ParsedTransaction {
            raw_instructions: vec![
                RawInstruction::AdvanceEpoch,
                RawInstruction::SyncEpoch,
            ],
            events: vec![
                TapedriveEvent::EpochAdvanced(EpochAdvanced {
                    old_epoch: 1u64.into(),
                    new_epoch: 2u64.into(),
                    timestamp: [0; 8],
                    committee_size: [0; 8],
                    total_stake: [0; 8],
                    storage_price: [0; 8],
                    storage_capacity: 1.into(),
                    nonce: tape_crypto::Hash::default(),
                    phase: 1, // Syncing
                }),
                TapedriveEvent::NodeSynced(NodeSynced {
                    node: Pubkey::new_unique(),
                    id: tape_core::types::NodeId::new(1),
                    epoch: 1u64.into(),
                    spools_hash: tape_crypto::Hash::default(),
                    phase: 1, // Syncing
                }),
            ],
        };
        let tx2 = ParsedTransaction {
            raw_instructions: vec![RawInstruction::AdvanceEpoch],
            events: vec![TapedriveEvent::EpochAdvanced(EpochAdvanced {
                old_epoch: 2u64.into(),
                new_epoch: 3u64.into(),
                timestamp: [0; 8],
                committee_size: [0; 8],
                total_stake: [0; 8],
                storage_price: [0; 8],
                storage_capacity: 1.into(),
                nonce: tape_crypto::Hash::default(),
                phase: 1, // Syncing
            })],
        };

        let parsed = ParsedBlock {
            transactions: vec![tx1, tx2],
            ..ParsedBlock::default()
        };

        let merged = merge_transactions(&parsed.transactions).unwrap();
        assert_eq!(merged.len(), 3);
        assert!(matches!(
            &merged[0],
            ParsedInstruction::AdvanceEpoch { .. }
        ));
        assert!(matches!(&merged[1], ParsedInstruction::SyncEpoch { .. }));
        assert!(matches!(&merged[2], ParsedInstruction::AdvanceEpoch { .. }));
    }
}
