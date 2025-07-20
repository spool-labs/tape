use base64;
use thiserror::Error;
use std::collections::HashMap;
use solana_sdk::{
    pubkey::Pubkey, 
    bs58, 
};
use solana_transaction_status::{
    option_serializer::OptionSerializer,
    EncodedTransaction,
    EncodedTransactionWithStatusMeta,
    UiCompiledInstruction,
    UiInstruction,
    UiMessage,
    UiTransactionStatusMeta,
    UiConfirmedBlock
};
use tape_api::prelude::{
    SEGMENT_SIZE,
    PROOF_LEN,
    WriteEvent,
    UpdateEvent,
    FinalizeEvent,
    InstructionType,
    EventType,
    Update,
};

#[derive(Error, Debug)]
pub enum BlockError {
    #[error("No transactions found in block")]
    NoTransactions,
    #[error("Mismatch between counts: {0}")]
    CountMismatch(&'static str),
    #[error("Invalid data: {0}")]
    InvalidData(&'static str),
    #[error("Deserialization failed: {0}")]
    Deserialization(String),
    #[error("Invalid public key")]
    InvalidPubkey,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SegmentKey {
    pub address: Pubkey,
    pub segment_number: u64,
    pub prev_slot: u64,
}

// Pulled out of logs
#[derive(Debug)]
pub enum TapeEvent {
    Write(WriteEvent),
    Update(UpdateEvent),
    Finalize(FinalizeEvent),
}

// Pulled out of instruction data
#[derive(Debug)]
pub enum TapeInstruction {
    Write { address: Pubkey, data: Vec<u8> },
    Update {
      address: Pubkey,
      segment_number: u64,
      old_data: [u8; SEGMENT_SIZE],
      new_data: [u8; SEGMENT_SIZE],
      proof: [[u8;32]; PROOF_LEN],
    },
    Finalize { address: Pubkey },
}

#[derive(Debug, Default)]
pub struct TapeBlock {
    pub events: Vec<TapeEvent>,
    pub instructions: Vec<TapeInstruction>,
}

#[derive(Debug, Default)]
pub struct ProcessedBlock {
    pub slot: u64,
    pub finalized_tapes: HashMap<Pubkey, u64>,
    pub segment_writes: HashMap<SegmentKey, Vec<u8>>,
}

#[derive(Debug, Default)]
struct MergedTapeData {
    finalized_tapes: HashMap<Pubkey, u64>,
    segment_writes: HashMap<SegmentKey, Vec<u8>>,
}

pub fn process_block(block: UiConfirmedBlock, slot: u64) -> Result<ProcessedBlock, BlockError> {
    let transactions = block.transactions.ok_or(BlockError::NoTransactions)?;
    let mut tape_block = TapeBlock::default();

    for tx in transactions {
        process_transaction(&tx, &mut tape_block)?;
    }

    verify_counts(&tape_block)?;
    let merged = merge_events_and_instructions(&tape_block)?;

    Ok(ProcessedBlock {
        slot,
        finalized_tapes: merged.finalized_tapes,
        segment_writes: merged.segment_writes,
    })
}

fn verify_counts(tape_block: &TapeBlock) -> Result<(), BlockError> {
    let (mut write_events, mut update_events, mut finalize_events) = (0, 0, 0);
    for event in &tape_block.events {
        match event {
            TapeEvent::Write(_) => write_events += 1,
            TapeEvent::Update(_) => update_events += 1,
            TapeEvent::Finalize(_) => finalize_events += 1,
        }
    }

    let (mut write_ix, mut update_ix, mut finalize_ix) = (0, 0, 0);
    for ix in &tape_block.instructions {
        match ix {
            TapeInstruction::Write { .. } => write_ix += 1,
            TapeInstruction::Update { .. } => update_ix += 1,
            TapeInstruction::Finalize { .. } => finalize_ix += 1,
        }
    }

    if tape_block.events.len() != tape_block.instructions.len() {
        return Err(BlockError::CountMismatch(
            "Events and Instructions",
        ));
    }

    if write_ix != write_events {
        return Err(BlockError::CountMismatch(
            "Write instructions and events",
        ));
    }

    if update_ix != update_events {
        return Err(BlockError::CountMismatch(
            "Update instructions and events",
        ));
    }

    if finalize_ix != finalize_events {
        return Err(BlockError::CountMismatch(
            "Finalize instructions and events",
        ));
    }

    Ok(())
}

fn merge_events_and_instructions(
    tape_block: &TapeBlock,
) -> Result<MergedTapeData, BlockError> {
    let mut merged = MergedTapeData::default();

    // Iterate over events and instructions in parallel
    for (event, instruction) in tape_block.events.iter().zip(&tape_block.instructions) {
        match (event, instruction) {
            (TapeEvent::Write(write_event), TapeInstruction::Write { address, data }) => {
                merge_write(write_event, address, data, &mut merged)?;
            }

            (TapeEvent::Update(update_event), TapeInstruction::Update { address, segment_number, new_data, .. }) => {
                merge_update(update_event, address, *segment_number, new_data, &mut merged)?;
            }

            (TapeEvent::Finalize(finalize_event), TapeInstruction::Finalize { address }) => {
                merge_finalize(finalize_event, address, &mut merged)?;
            }

            _ => return Err(BlockError::InvalidData("Event/instruction type mismatch")),
        }
    }

    Ok(merged)
}

fn merge_write(
    write_event: &WriteEvent,
    address: &Pubkey,
    data: &[u8],
    merged: &mut MergedTapeData,
) -> Result<(), BlockError> {
    if write_event.address != address.to_bytes() {
        return Err(BlockError::InvalidData("Write event and instruction address mismatch"));
    }

    let base = write_event
        .num_total
        .saturating_sub(write_event.num_added);

    // A single write instruction can contain multiple segments
    let segments: Vec<&[u8]> = data.chunks(SEGMENT_SIZE).collect();

    // Sanity check: number of chunks must match num_added
    if segments.len() as u64 != write_event.num_added {
        return Err(BlockError::InvalidData("Segment count does not match num_added"));
    }

    for (i, segment) in segments.into_iter().enumerate() {
        let segment_number = base + i as u64;

        let key = SegmentKey {
            address: *address,
            segment_number,
            prev_slot: write_event.prev_slot,
        };
        merged.segment_writes.insert(key, segment.to_vec());
    }

    Ok(())
}

fn merge_update(
    update_event: &UpdateEvent,
    address: &Pubkey,
    segment_number: u64,
    new_data: &[u8; SEGMENT_SIZE],
    merged: &mut MergedTapeData,
) -> Result<(), BlockError> {
    if update_event.address != address.to_bytes() {
       return Err(BlockError::InvalidData("Update event/address mismatch"));
    }

    if update_event.segment_number != segment_number {
        return Err(BlockError::InvalidData("Update event segment number mismatch"));
    }

    let key = SegmentKey {
        address: *address,
        segment_number,
        prev_slot: update_event.prev_slot,
    };

    // Record the “new_data”, effectively overwriting that segment
    merged.segment_writes.insert(key, new_data.to_vec());

    // (optional) verify old_data + proof

    Ok(())
}

fn merge_finalize(
    finalize_event: &FinalizeEvent,
    address: &Pubkey,
    merged: &mut MergedTapeData,
) -> Result<(), BlockError> {
    if finalize_event.address != address.to_bytes() {
        return Err(BlockError::InvalidData("Finalize event and instruction address mismatch"));
    }

    merged.finalized_tapes.insert(*address, finalize_event.tape);

    Ok(())
}

fn process_transaction(
    tx: &EncodedTransactionWithStatusMeta,
    tape_block: &mut TapeBlock,
) -> Result<(), BlockError> {
    if is_failed_transaction(tx) {
        return Ok(());
    }

    let encoded_tx = &tx.transaction;
    let ui_transaction = match encoded_tx {
        EncodedTransaction::Json(ui_tx) => ui_tx,
        _ => {
            return Ok(());
        }
    };

    match &ui_transaction.message {
        UiMessage::Raw(raw_message) => {
            if let Some(meta) = &tx.meta {
                if let OptionSerializer::Some(log_messages) = &meta.log_messages {
                    process_log_messages(log_messages, tape_block)?;
                }
            }

            process_top_level_instructions(
                &raw_message.account_keys,
                &raw_message.instructions,
                tape_block,
            )?;
            process_inner_instructions(&raw_message.account_keys, &tx.meta, tape_block)?;
            Ok(())
        }
        _ => {
            Ok(())
        }
    }
}

fn process_log_messages(
    log_messages: &[String],
    tape_block: &mut TapeBlock,
) -> Result<(), BlockError> {
    let events = &mut tape_block.events;
    let mut program_stack: Vec<Pubkey> = Vec::new();

    for log in log_messages {
        if is_program_invoke(log) {
            if let Some(program_id) = get_program_id(log) {
                program_stack.push(program_id);
            }
        } else if is_program_success(log) || is_program_failure(log) {
            program_stack.pop();
        }

        let is_tape_program = program_stack.last() == Some(&tape_api::ID);

        if is_tape_program && is_program_data(log) {
            let event_data =
                get_event_data(log).ok_or(BlockError::InvalidData("Invalid log format"))?;

            let event_type = EventType::try_from(event_data[0])
                .map_err(|_| BlockError::InvalidData("Failed to parse event type"))?;

            match event_type {
                EventType::WriteEvent => {
                    let event = WriteEvent::try_from_bytes(&event_data)
                        .map_err(|e| BlockError::Deserialization(e.to_string()))?;
                    events.push(TapeEvent::Write(*event));
                }
                EventType::UpdateEvent => {
                  let event = UpdateEvent::try_from_bytes(&event_data)
                      .map_err(|e| BlockError::Deserialization(e.to_string()))?;
                  events.push(TapeEvent::Update(*event));
                }
                EventType::FinalizeEvent => {
                    let event = FinalizeEvent::try_from_bytes(&event_data)
                        .map_err(|e| BlockError::Deserialization(e.to_string()))?;
                    events.push(TapeEvent::Finalize(*event));
                }
                _ => {}
            }
        }
    }

    Ok(())
}

fn process_top_level_instructions(
    account_keys: &[String],
    instructions: &[UiCompiledInstruction],
    tape_block: &mut TapeBlock,
) -> Result<(), BlockError> {
    for ix in instructions {
        let program_id_index = ix.program_id_index as usize;
        if program_id_index >= account_keys.len() {
            continue;
        }

        let program_id = account_keys[program_id_index]
            .parse::<Pubkey>()
            .map_err(|_| BlockError::InvalidPubkey)?;
        if program_id == tape_api::ID {
            let tape_ix = process_instruction(ix, account_keys)?;
            if let Some(ix) = tape_ix {
                tape_block.instructions.push(ix);
            }
        }
    }

    Ok(())
}

fn process_inner_instructions(
    account_keys: &[String],
    meta: &Option<UiTransactionStatusMeta>,
    tape_block: &mut TapeBlock,
) -> Result<(), BlockError> {
    let Some(meta) = meta else {
        return Ok(());
    };

    let OptionSerializer::Some(inner_instructions) = &meta.inner_instructions else {
        return Ok(());
    };

    for inner_ix_set in inner_instructions {
        for inner_ix in &inner_ix_set.instructions {
            if let UiInstruction::Compiled(compiled_ix) = inner_ix {
                let program_id_index = compiled_ix.program_id_index as usize;
                if program_id_index >= account_keys.len() {
                    continue;
                }

                let program_id = account_keys[program_id_index]
                    .parse::<Pubkey>()
                    .map_err(|_| BlockError::InvalidPubkey)?;
                if program_id == tape_api::ID {
                    let tape_ix = process_instruction(compiled_ix, account_keys)?;
                    if let Some(ix) = tape_ix {
                        tape_block.instructions.push(ix);
                    }
                }
            }
        }
    }

    Ok(())
}

fn process_instruction(
    ix: &UiCompiledInstruction,
    account_keys: &[String],
) -> Result<Option<TapeInstruction>, BlockError> {
    let tape_index = *ix
        .accounts
        .get(1)
        .ok_or(BlockError::InvalidData("Missing tape account"))? as usize;

    if tape_index >= account_keys.len() {
        return Err(BlockError::InvalidData("Invalid tape account index"));
    }

    let tape_address = account_keys[tape_index]
        .parse::<Pubkey>()
        .map_err(|_| BlockError::InvalidPubkey)?;

    let ix_data = bs58::decode(&ix.data)
        .into_vec()
        .map_err(|_| BlockError::InvalidData("Invalid instruction data"))?;

    if ix_data.is_empty() {
        return Ok(None);
    }

    let ix_type = InstructionType::try_from(ix_data[0])
        .map_err(|_| BlockError::InvalidData("Invalid instruction type"))?;

    match ix_type {
        InstructionType::Write => Ok(Some(TapeInstruction::Write {
            address: tape_address,
            data: ix_data[1..].to_vec(),
        })),
        InstructionType::Update => {
            Update::try_from_bytes(&ix_data[1..])
                .map_err(|e| BlockError::Deserialization(e.to_string()))
                .map(|update| Some(TapeInstruction::Update {
                    address: tape_address,
                    segment_number: u64::from_le_bytes(update.segment_number),
                    old_data: update.old_data,
                    new_data: update.new_data,
                    proof: update.proof,
                }))
        }
        InstructionType::Finalize => Ok(Some(TapeInstruction::Finalize {
            address: tape_address,
        })),
        _ => Ok(None),
    }
}

fn is_failed_transaction(tx: &EncodedTransactionWithStatusMeta) -> bool {
    if let Some(meta) = &tx.meta {
        if let solana_sdk::transaction::Result::Err(_) = meta.status {
            return true;
        }
    }
    false
}

fn is_program_invoke(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" invoke ")
}

fn is_program_success(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" success")
}

fn is_program_failure(log: &str) -> bool {
    log.starts_with("Program ") && log.contains(" failed")
}

fn is_program_data(log: &str) -> bool {
    log.starts_with("Program data: ")
}

fn get_program_id(log: &str) -> Option<Pubkey> {
    let parts: Vec<&str> = log.split_whitespace().collect();
    if parts.len() >= 3 {
        return parts[1].parse::<Pubkey>().ok();
    }
    None
}

fn get_event_data(log: &str) -> Option<Vec<u8>> {
    let encoded_data = log.strip_prefix("Program data: ")?;
    base64::decode(encoded_data).ok()
}
