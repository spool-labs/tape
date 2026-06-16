use std::collections::BTreeMap;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiConfirmedBlock, UiInstruction, UiMessage, UiTransactionStatusMeta,
};
use tape_api::program::tapedrive::ID as TAPE_PROGRAM_ID;
use tape_crypto::address::Address;

use crate::error::ParseError;
use crate::event::{parse_event_data, TapedriveEvent};
use crate::helpers::{
    get_program_id, is_program_data, is_program_failure, is_program_invoke, is_program_success,
};
use crate::instruction::{parse_raw_instruction, ParsedInstruction, RawInstruction};
use crate::merge::merge;
use tape_crypto::tx::Txid;

/// Result of parsing a single block.
#[derive(Debug, Default)]
pub struct ParsedTransaction {
    /// Canonical transaction id, when available.
    pub tx_id: Option<Txid>,
    /// Raw instructions for this transaction.
    pub raw_instructions: Vec<RawInstruction>,
    /// Parsed events for this transaction.
    pub events: Vec<TapedriveEvent>,
}

/// Merged instruction with the transaction id that produced it.
#[derive(Debug, Clone)]
pub struct ParsedInstructionWithSource {
    pub tx_id: Txid,
    pub instruction: ParsedInstruction,
}

/// Result of parsing a single block.
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
        let tx_id = transaction_id(tx)?;
        let (instructions, events) = parse_transaction(tx)?;
        result.transactions.push(ParsedTransaction {
            tx_id,
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

/// Parse a confirmed block and merge parsed instructions while retaining the
/// source transaction id for each merged instruction.
pub fn parse_and_merge_with_sources(
    block: &UiConfirmedBlock,
) -> Result<Vec<ParsedInstructionWithSource>, ParseError> {
    let parsed = parse(block)?;
    merge_transactions_with_sources(&parsed.transactions)
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

fn merge_transactions_with_sources(
    transactions: &[ParsedTransaction],
) -> Result<Vec<ParsedInstructionWithSource>, ParseError> {
    let mut instructions = Vec::new();
    for tx in transactions {
        let tx_id = tx.tx_id.ok_or(ParseError::InvalidTxId)?;
        instructions.extend(
            merge(tx.raw_instructions.clone(), tx.events.clone())?
                .into_iter()
                .map(|instruction| ParsedInstructionWithSource {
                    tx_id,
                    instruction,
                }),
        );
    }
    Ok(instructions)
}

fn transaction_id(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<Option<Txid>, ParseError> {
    let EncodedTransaction::Json(ui_tx) = &tx.transaction else {
        return Ok(None);
    };

    let Some(encoded) = ui_tx.signatures.first() else {
        return Ok(None);
    };

    let bytes = bs58::decode(encoded)
        .into_vec()
        .map_err(|_| ParseError::InvalidTxId)?;
    let bytes: [u8; 64] = bytes
        .try_into()
        .map_err(|_| ParseError::InvalidTxId)?;

    Ok(Some(Txid::from(bytes)))
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

    let Some(meta) = &tx.meta else {
        return Ok((Vec::new(), Vec::new()));
    };

    // Solana resolves compiled-instruction indices against static keys, then
    // ALT-loaded writable, then ALT-loaded readonly. Order is load-bearing.
    let mut resolved_keys: Vec<String> = raw_message.account_keys.clone();
    if let OptionSerializer::Some(loaded) = &meta.loaded_addresses {
        resolved_keys.extend(loaded.writable.iter().cloned());
        resolved_keys.extend(loaded.readonly.iter().cloned());
    }

    let events = parse_log_messages(meta)?;
    let mut raw_instructions = Vec::new();
    let mut inner_by_outer_index = parse_inner_instructions(&resolved_keys, meta)?;

    // Process each top-level instruction followed immediately by its inner instructions.
    for (outer_index, ix) in raw_message.instructions.iter().enumerate() {
        if let Some(parsed) = parse_raw_instruction(ix, &resolved_keys)? {
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
    let tapedrive_program_id = Address::from(TAPE_PROGRAM_ID);

    let OptionSerializer::Some(log_messages) = &meta.log_messages else {
        return Ok(events);
    };

    let mut program_stack: Vec<Address> = Vec::new();

    for log in log_messages {
        if is_program_invoke(log) {
            if let Some(program_id) = get_program_id(log) {
                program_stack.push(program_id);
            }
        } else if is_program_success(log) || is_program_failure(log) {
            program_stack.pop();
        }

        // Only parse events from tapedrive program
        let is_tapedrive = program_stack.last() == Some(&tapedrive_program_id);

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
    use base64::encode as base64_encode;
    use bs58::encode as bs58_encode;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::MessageHeader;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::hash::Hash as SolanaHash;
    use solana_sdk::signature::{Keypair, Signer};
    use solana_sdk::sysvar;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{EpochPhase, NodePreferences};
    use bytemuck::Zeroable;
    use solana_transaction_status::{
        EncodedTransaction, EncodedTransactionWithStatusMeta, UiCompiledInstruction,
        UiInnerInstructions, UiLoadedAddresses, UiRawMessage, UiTransaction,
        UiTransactionStatusMeta,
    };
    use tape_api::event::{EpochAdvanced, EventType, SpoolSynced, TrackWritten};
    use tape_api::instruction::build_track_write_ix;
    use tape_api::program::tapedrive::{self, system_pda, tape_pda};
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::types::coin::TAPE;
    use tape_core::track::data::{BlobData, BlobInfo};
    use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits, StripeCount};
    use tape_crypto::address::Address;
    use tape_crypto::hash::hash;
    use tape_crypto::tx::Txid;
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
    fn test_parse_preserves_transaction_id() {
        let keypair = Keypair::new();
        let signature = keypair.sign_message(b"tapedrive txid");
        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Json(UiTransaction {
                signatures: vec![signature.to_string()],
                message: UiMessage::Raw(raw_message(&[], Vec::new())),
            }),
            meta: Some(UiTransactionStatusMeta {
                err: None,
                status: Ok(()),
                fee: 0,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: OptionSerializer::None,
                log_messages: OptionSerializer::None,
                pre_token_balances: OptionSerializer::None,
                post_token_balances: OptionSerializer::None,
                rewards: OptionSerializer::None,
                loaded_addresses: OptionSerializer::None,
                return_data: OptionSerializer::None,
                compute_units_consumed: OptionSerializer::None,
                cost_units: OptionSerializer::None,
            }),
            version: None,
        };
        let block = UiConfirmedBlock {
            previous_blockhash: String::new(),
            blockhash: String::new(),
            parent_slot: 0,
            transactions: Some(vec![tx]),
            signatures: None,
            rewards: None,
            block_time: None,
            block_height: None,
            num_reward_partitions: None,
        };

        let parsed = parse(&block).unwrap();

        assert_eq!(parsed.transactions.len(), 1);
        assert_eq!(parsed.transactions[0].tx_id, Some(Txid::from(signature)));
    }

    #[test]
    fn test_merge_transactions_keeps_transaction_boundaries_for_optional_events() {
        use crate::event::TapedriveEvent;
        use tape_api::event::TrackWritten;

        let tx2_track = Address::new_unique();

        let tx1 = ParsedTransaction {
            tx_id: None,
            raw_instructions: vec![RawInstruction::TrackWrite {
                authority: Address::new_unique(),
                key: Hash::default(),
                object: None,
                value: BlobData::Coded(BlobEncoding {
                    size: 1_024u64.into(),
                    commitment: Hash::default(),
                    profile: EncodingProfile::default(),
                    stripe_size: StorageUnits::from_bytes(64),
                    stripe_count: StripeCount(1),
                    leaves: [Hash::default(); GROUP_SIZE],
                }),
            }],
            events: vec![],
        };

        let track_event = TrackWritten {
            epoch: 2.into(),
            track: tx2_track,
            tape: Address::new_unique(),
            group: GroupIndex(0),
            track_number: 0u64.into(),
            track_hash: Hash::default(),
        };

        let tx2 = ParsedTransaction {
            tx_id: None,
            raw_instructions: vec![RawInstruction::TrackWrite {
                authority: Address::new_unique(),
                key: Hash::default(),
                object: None,
                value: BlobData::Coded(BlobEncoding {
                    size: 1_024u64.into(),
                    commitment: Hash::default(),
                    profile: EncodingProfile::default(),
                    stripe_size: StorageUnits::from_bytes(64),
                    stripe_count: StripeCount(1),
                    leaves: [Hash::default(); GROUP_SIZE],
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
        let sync_node = Address::new_unique();
        let tx1 = ParsedTransaction {
            tx_id: None,
            raw_instructions: vec![
                RawInstruction::AdvanceEpoch,
                RawInstruction::SyncSpool { node: sync_node, spool: 3 },
            ],
            events: vec![
                TapedriveEvent::EpochAdvanced(EpochAdvanced {
                    old_epoch: 1u64.into(),
                    new_epoch: 2u64.into(),
                    timestamp: 0,
                    total_stake: TAPE(0),
                    committee_count: 0,
                    preferences: NodePreferences::zeroed(),
                    subsidy: TAPE(0),
                    nonce: Hash::default(),
                }),
                TapedriveEvent::SpoolSynced(SpoolSynced {
                    node: sync_node,
                    epoch: 1u64.into(),
                    group: GroupIndex(7),
                    spool: SpoolIndex(3),
                    phase: EpochPhase::Sync as u64,
                }),
            ],
        };
        let tx2 = ParsedTransaction {
            tx_id: None,
            raw_instructions: vec![RawInstruction::AdvanceEpoch],
            events: vec![TapedriveEvent::EpochAdvanced(EpochAdvanced {
                old_epoch: 2u64.into(),
                new_epoch: 3u64.into(),
                timestamp: 0,
                total_stake: TAPE(0),
                committee_count: 0,
                preferences: NodePreferences::zeroed(),
                subsidy: TAPE(0),
                nonce: Hash::default(),
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
        assert!(matches!(&merged[1], ParsedInstruction::SyncSpool { .. }));
        assert!(matches!(&merged[2], ParsedInstruction::AdvanceEpoch { .. }));
    }

    fn encode_event<T: bytemuck::Pod>(event_type: EventType, event: &T) -> String {
        let mut data = vec![event_type as u8];
        data.extend_from_slice(&[0u8; 7]);
        data.extend_from_slice(bytemuck::bytes_of(event));
        format!("Program data: {}", base64_encode(&data))
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
            data: bs58_encode(&ix.data).into_string(),
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
            recent_blockhash: SolanaHash::new_unique().to_string(),
            instructions,
            address_table_lookups: None,
        }
    }

    #[test]
    fn test_parse_transaction_interleaves_inner_instructions_by_outer_index() {
        let fee_payer = Address::new_unique();
        let authority = Address::new_unique();
        let system = system_pda().0;
        let tape = tape_pda(authority).0;
        let slot_hashes = sysvar::slot_hashes::ID;
        let other_program = Pubkey::new_unique();
        let tapedrive_program = tapedrive::ID;
        let account_keys = vec![
            other_program,
            fee_payer.into(),
            authority.into(),
            system.into(),
            tape.into(),
            slot_hashes,
            tapedrive_program,
        ];

        let inner_data = b"inner raw";
        let outer_data = b"outer raw";
        let inner_key = hash(inner_data);
        let outer_key = hash(outer_data);
        let inner_ix = build_track_write_ix(
            fee_payer,
            authority,
            BlobInfo {
                object: None,
                data: BlobData::Inline(inner_data.to_vec()),
            },
        )
        .expect("valid inner track write instruction");
        let outer_ix = build_track_write_ix(
            fee_payer,
            authority,
            BlobInfo {
                object: None,
                data: BlobData::Inline(outer_data.to_vec()),
            },
        )
        .expect("valid outer track write instruction");

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
                            track: Address::new_unique(),
                            tape,
                            group: GroupIndex(1),
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
                            track: Address::new_unique(),
                            tape,
                            group: GroupIndex(2),
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
                assert!(matches!(value, BlobData::Inline(bytes) if bytes == b"inner raw"));
            }
            _ => panic!("expected CPI TrackWrite first"),
        }
        match &instructions[1] {
            RawInstruction::TrackWrite { key, value, .. } => {
                assert_eq!(*key, outer_key);
                assert!(matches!(value, BlobData::Inline(bytes) if bytes == b"outer raw"));
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

    fn loaded_addresses(writable: &[Pubkey], readonly: &[Pubkey]) -> UiLoadedAddresses {
        UiLoadedAddresses {
            writable: writable.iter().map(ToString::to_string).collect(),
            readonly: readonly.iter().map(ToString::to_string).collect(),
        }
    }

    /// Outer instruction in static keys, inner CPI to a program loaded from an ALT.
    #[test]
    fn resolves_alt_inner_cpi() {
        let fee_payer = Address::new_unique();
        let authority = Address::new_unique();
        let system = system_pda().0;
        let tape = tape_pda(authority).0;
        let slot_hashes = sysvar::slot_hashes::ID;
        let outer_program = Pubkey::new_unique();
        let tapedrive_program = tapedrive::ID;

        let static_keys = vec![
            outer_program,
            fee_payer.into(),
            authority.into(),
            system.into(),
            tape.into(),
            slot_hashes,
        ];
        let writable: Vec<Pubkey> = vec![tapedrive_program];
        let readonly: Vec<Pubkey> = vec![];

        let mut combined_keys = static_keys.clone();
        combined_keys.extend(writable.iter().copied());
        combined_keys.extend(readonly.iter().copied());

        let inner_data = b"inner alt";
        let inner_key = hash(inner_data);
        let inner_ix = build_track_write_ix(
            fee_payer,
            authority,
            BlobInfo {
                object: None,
                data: BlobData::Inline(inner_data.to_vec()),
            },
        )
        .expect("valid inner track write instruction");

        let tx = EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Json(UiTransaction {
                signatures: vec![],
                message: UiMessage::Raw(raw_message(
                    &static_keys,
                    vec![UiCompiledInstruction {
                        program_id_index: 0,
                        accounts: vec![],
                        data: String::new(),
                        stack_height: None,
                    }],
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
                        &combined_keys,
                    ))],
                }]),
                log_messages: OptionSerializer::Some(vec![
                    format!("Program {} invoke [1]", outer_program),
                    format!("Program {} invoke [2]", tapedrive_program),
                    encode_event(
                        EventType::TrackWritten,
                        &TrackWritten {
                            epoch: EpochNumber(3),
                            track: Address::new_unique(),
                            tape,
                            group: GroupIndex(1),
                            track_number: 21u64.into(),
                            track_hash: Hash::new_unique(),
                        },
                    ),
                    format!("Program {} success", tapedrive_program),
                    format!("Program {} success", outer_program),
                ]),
                pre_token_balances: OptionSerializer::None,
                post_token_balances: OptionSerializer::None,
                rewards: OptionSerializer::None,
                loaded_addresses: OptionSerializer::Some(loaded_addresses(&writable, &readonly)),
                return_data: OptionSerializer::Skip,
                compute_units_consumed: OptionSerializer::Skip,
                cost_units: OptionSerializer::Skip,
            }),
            version: None,
        };

        let (instructions, events) = parse_transaction(&tx).unwrap();

        assert_eq!(instructions.len(), 1);
        match &instructions[0] {
            RawInstruction::TrackWrite { key, value, .. } => {
                assert_eq!(*key, inner_key);
                assert!(matches!(value, BlobData::Inline(bytes) if bytes == b"inner alt"));
            }
            other => panic!("expected inner TrackWrite, got {other:?}"),
        }

        let merged = merge(instructions, events).expect("merge inner CPI with event");
        assert_eq!(merged.len(), 1);
        match &merged[0] {
            ParsedInstruction::TrackWrite { key, event, .. } => {
                assert_eq!(*key, inner_key);
                assert_eq!(event.track_number, 21u64.into());
            }
            _ => panic!("expected merged inner TrackWrite"),
        }
    }
}
