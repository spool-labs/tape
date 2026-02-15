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
use crate::instruction::{parse_raw_instruction, RawInstruction};

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
/// - Use `parsed.events` directly for event-only processing (monitor)
/// - Call `merge(parsed.raw_instructions, parsed.events)` to get merged
///   instructions (node)
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
    use solana_transaction_status::UiConfirmedBlock;

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
}
