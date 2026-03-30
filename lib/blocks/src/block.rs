//! Block-level parsing.

use std::collections::BTreeMap;

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
    let mut inner_by_outer_index = if let Some(meta) = &tx.meta {
        parse_inner_instructions(account_keys, meta)?
    } else {
        BTreeMap::new()
    };

    // Process each top-level instruction followed immediately by its inner instructions.
    for (outer_index, ix) in raw_message.instructions.iter().enumerate() {
        if let Some(parsed) = parse_raw_instruction(ix, account_keys)? {
            raw_instructions.push(parsed);
        }

        if let Some(inner) = inner_by_outer_index.remove(&(outer_index as u8)) {
            raw_instructions.extend(inner);
        }
    }

    // Preserve any unmatched inner-instruction sets deterministically.
    for (_, inner) in inner_by_outer_index {
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
) -> Result<BTreeMap<u8, Vec<RawInstruction>>, ParseError> {
    let mut instructions = BTreeMap::new();

    let OptionSerializer::Some(inner_instructions) = &meta.inner_instructions else {
        return Ok(instructions);
    };

    for inner_ix_set in inner_instructions {
        let parsed_for_index = instructions.entry(inner_ix_set.index).or_default();
        for inner_ix in &inner_ix_set.instructions {
            if let UiInstruction::Compiled(compiled_ix) = inner_ix {
                if let Some(parsed) = parse_raw_instruction(compiled_ix, account_keys)? {
                    parsed_for_index.push(parsed);
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
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::MessageHeader;
    use solana_sdk::pubkey::Pubkey;
    use solana_transaction_status::{
        EncodedTransaction, EncodedTransactionWithStatusMeta, UiCompiledInstruction,
        UiInnerInstructions, UiRawMessage, UiTransaction, UiTransactionStatusMeta,
    };
    use tape_api::event::{EpochAdvanced, EventType, NodeSynced, TrackWritten};
    use tape_api::instruction::build_track_write_raw_ix;
    use tape_api::program::tapedrive::{epoch_pda, tape_pda};
    use tape_core::types::EpochNumber;
    use tape_core::track::data::TrackData;
    use tape_crypto::Hash;

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
        use crate::event::TapedriveEvent;
        use tape_api::event::TrackWritten;

        let tx2_track = Pubkey::new_unique();

        let tx1 = ParsedTransaction {
            raw_instructions: vec![RawInstruction::TrackWrite {
                authority: Pubkey::new_unique(),
                key: tape_crypto::Hash::default(),
                value: tape_core::track::data::TrackData::Blob(tape_core::track::blob::BlobInfo {
                    size: 1_024u64.into(),
                    root: tape_crypto::Hash::default(),
                    commitment: tape_crypto::Hash::default(),
                    profile: tape_core::encoding::EncodingProfile::default(),
                    stripe_size: 64,
                    stripe_count: 1,
                    leaves: [tape_crypto::Hash::default(); tape_core::erasure::SPOOL_GROUP_SIZE],
                }),
            }],
            events: vec![],
        };

        let track_event = TrackWritten {
            epoch: 2.into(),
            track: tx2_track,
            tape: Pubkey::new_unique(),
            spool_group: 0u64.to_le_bytes(),
            track_number: 0u64.into(),
            track_hash: tape_crypto::Hash::default(),
        };

        let tx2 = ParsedTransaction {
            raw_instructions: vec![RawInstruction::TrackWrite {
                authority: Pubkey::new_unique(),
                key: tape_crypto::Hash::default(),
                value: tape_core::track::data::TrackData::Blob(tape_core::track::blob::BlobInfo {
                    size: 1_024u64.into(),
                    root: tape_crypto::Hash::default(),
                    commitment: tape_crypto::Hash::default(),
                    profile: tape_core::encoding::EncodingProfile::default(),
                    stripe_size: 64,
                    stripe_count: 1,
                    leaves: [tape_crypto::Hash::default(); tape_core::erasure::SPOOL_GROUP_SIZE],
                }),
            }],
            events: vec![TapedriveEvent::TrackWritten(track_event)],
        };

        let parsed = ParsedBlock {
            transactions: vec![tx1, tx2],
            ..ParsedBlock::default()
        };

        let result = merge_transactions(&parsed.transactions);
        assert!(result.is_err());
        match result {
            Err(ParseError::EventMismatch(_)) => {}
            _ => panic!("Expected EventMismatch error"),
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

    fn encode_event<T: bytemuck::Pod>(event_type: EventType, event: &T) -> String {
        let mut data = vec![event_type as u8];
        data.extend_from_slice(&[0u8; 7]);
        data.extend_from_slice(bytemuck::bytes_of(event));
        format!("Program data: {}", base64::encode(&data))
    }

    fn compile_ui_instruction(ix: &Instruction, account_keys: &[Pubkey]) -> UiCompiledInstruction {
        let program_id_index = account_keys
            .iter()
            .position(|key| *key == ix.program_id)
            .expect("program id in account keys") as u8;

        let accounts = ix
            .accounts
            .iter()
            .map(|meta| {
                account_keys
                    .iter()
                    .position(|key| *key == meta.pubkey)
                    .expect("account in account keys") as u8
            })
            .collect();

        UiCompiledInstruction {
            program_id_index,
            accounts,
            data: bs58::encode(&ix.data).into_string(),
            stack_height: None,
        }
    }

    fn raw_message(account_keys: &[Pubkey], instructions: Vec<UiCompiledInstruction>) -> UiRawMessage {
        UiRawMessage {
            header: MessageHeader {
                num_required_signatures: 0,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: account_keys.iter().map(ToString::to_string).collect(),
            recent_blockhash: solana_sdk::hash::Hash::new_unique().to_string(),
            instructions,
            address_table_lookups: None,
        }
    }

    #[test]
    fn test_parse_transaction_interleaves_inner_instructions_by_outer_index() {
        let fee_payer = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let epoch = epoch_pda().0;
        let tape = tape_pda(authority).0;
        let slot_hashes = solana_sdk::sysvar::slot_hashes::ID;
        let other_program = Pubkey::new_unique();
        let tapedrive_program = tape_api::program::tapedrive::ID;
        let account_keys = vec![
            other_program,
            fee_payer,
            authority,
            epoch,
            tape,
            slot_hashes,
            tapedrive_program,
        ];

        let inner_key = Hash::new_unique();
        let outer_key = Hash::new_unique();
        let inner_ix = build_track_write_raw_ix(fee_payer, authority, inner_key, b"inner raw");
        let outer_ix = build_track_write_raw_ix(fee_payer, authority, outer_key, b"outer raw");

        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Json(UiTransaction {
                signatures: vec![],
                message: UiMessage::Raw(raw_message(
                    &account_keys,
                    vec![
                        UiCompiledInstruction {
                            program_id_index: 0,
                            accounts: vec![],
                            data: String::new(),
                            stack_height: None,
                        },
                        compile_ui_instruction(&outer_ix, &account_keys),
                    ],
                )),
            }),
            meta: Some(UiTransactionStatusMeta {
                err: None,
                status: Ok(()),
                fee: 0,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: OptionSerializer::Some(vec![UiInnerInstructions {
                    index: 0,
                    instructions: vec![UiInstruction::Compiled(compile_ui_instruction(
                        &inner_ix,
                        &account_keys,
                    ))],
                }]),
                log_messages: OptionSerializer::Some(vec![
                    format!("Program {} invoke [1]", other_program),
                    format!("Program {} invoke [2]", tapedrive_program),
                    encode_event(
                        EventType::TrackWritten,
                        &TrackWritten {
                            epoch: EpochNumber(3),
                            track: Pubkey::new_unique(),
                            tape,
                            spool_group: 1u64.to_le_bytes(),
                            track_number: 7u64.into(),
                            track_hash: Hash::new_unique(),
                        },
                    ),
                    format!("Program {} success", tapedrive_program),
                    format!("Program {} success", other_program),
                    format!("Program {} invoke [1]", tapedrive_program),
                    encode_event(
                        EventType::TrackWritten,
                        &TrackWritten {
                            epoch: EpochNumber(3),
                            track: Pubkey::new_unique(),
                            tape,
                            spool_group: 2u64.to_le_bytes(),
                            track_number: 8u64.into(),
                            track_hash: Hash::new_unique(),
                        },
                    ),
                    format!("Program {} success", tapedrive_program),
                ]),
                pre_token_balances: OptionSerializer::None,
                post_token_balances: OptionSerializer::None,
                rewards: OptionSerializer::None,
                loaded_addresses: OptionSerializer::Skip,
                return_data: OptionSerializer::Skip,
                compute_units_consumed: OptionSerializer::Skip,
                cost_units: OptionSerializer::Skip,
            }),
            version: None,
        };

        let (instructions, events) = parse_transaction(&tx).unwrap();

        assert_eq!(instructions.len(), 2);
        match &instructions[0] {
            RawInstruction::TrackWrite { key, value, .. } => {
                assert_eq!(*key, inner_key);
                assert!(matches!(value, TrackData::Raw(bytes) if bytes == b"inner raw"));
            }
            _ => panic!("expected CPI TrackWrite first"),
        }
        match &instructions[1] {
            RawInstruction::TrackWrite { key, value, .. } => {
                assert_eq!(*key, outer_key);
                assert!(matches!(value, TrackData::Raw(bytes) if bytes == b"outer raw"));
            }
            _ => panic!("expected top-level TrackWrite second"),
        }

        let merged = merge(instructions, events).unwrap();
        assert_eq!(merged.len(), 2);

        match &merged[0] {
            ParsedInstruction::TrackWrite { key, event, .. } => {
                assert_eq!(*key, inner_key);
                assert_eq!(event.track_number, 7u64.into());
            }
            _ => panic!("expected merged CPI TrackWrite first"),
        }
        match &merged[1] {
            ParsedInstruction::TrackWrite { key, event, .. } => {
                assert_eq!(*key, outer_key);
                assert_eq!(event.track_number, 8u64.into());
            }
            _ => panic!("expected merged top-level TrackWrite second"),
        }
    }
}
