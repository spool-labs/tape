//! Transaction and block parsing for tapedrive instructions.
//!
//! Parses Solana blocks to extract tapedrive-related instructions
//! that affect node state.

use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiCompiledInstruction, UiConfirmedBlock, UiInstruction, UiMessage, UiTransactionStatusMeta,
};
use tape_api::instruction::{
    self as ix, TapeInstruction,
};
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
}

/// Parsed tapedrive instruction relevant to node operation.
#[derive(Debug, Clone)]
pub enum ParsedInstruction {
    // Epoch management
    AdvanceEpoch,
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
    },
    DeleteTrack {
        owner: Pubkey,
        track: Pubkey,
    },
    CertifyTrack {
        track: Pubkey,
        epoch: EpochNumber,
    },
    InvalidateTrack {
        track: Pubkey,
    },

    // Tape management
    ReserveTape {
        owner: Pubkey,
        tape: Pubkey,
    },
    DestroyTape {
        owner: Pubkey,
        tape: Pubkey,
    },

    // Node management
    RegisterNode {
        authority: Pubkey,
        node: Pubkey,
    },
    JoinNetwork {
        node: Pubkey,
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

        // Parse instructions from this transaction
        let parsed = parse_transaction(tx)?;
        result.instructions.extend(parsed);
    }

    Ok(result)
}

/// Parse a single transaction for tapedrive instructions.
fn parse_transaction(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut instructions = Vec::new();

    let EncodedTransaction::Json(ui_tx) = &tx.transaction else {
        return Ok(instructions);
    };

    let UiMessage::Raw(raw_message) = &ui_tx.message else {
        return Ok(instructions);
    };

    let account_keys = &raw_message.account_keys;

    // Process top-level instructions
    for ix in &raw_message.instructions {
        if let Some(parsed) = parse_instruction(ix, account_keys)? {
            instructions.push(parsed);
        }
    }

    // Process inner instructions (CPIs)
    if let Some(meta) = &tx.meta {
        let inner = parse_inner_instructions(account_keys, meta)?;
        instructions.extend(inner);
    }

    Ok(instructions)
}

/// Parse inner instructions from transaction metadata.
fn parse_inner_instructions(
    account_keys: &[String],
    meta: &UiTransactionStatusMeta,
) -> Result<Vec<ParsedInstruction>, ParseError> {
    let mut instructions = Vec::new();

    let OptionSerializer::Some(inner_instructions) = &meta.inner_instructions else {
        return Ok(instructions);
    };

    for inner_ix_set in inner_instructions {
        for inner_ix in &inner_ix_set.instructions {
            if let UiInstruction::Compiled(compiled_ix) = inner_ix {
                if let Some(parsed) = parse_instruction(compiled_ix, account_keys)? {
                    instructions.push(parsed);
                }
            }
        }
    }

    Ok(instructions)
}

/// Parse a single compiled instruction.
fn parse_instruction(
    ix: &UiCompiledInstruction,
    account_keys: &[String],
) -> Result<Option<ParsedInstruction>, ParseError> {
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
        let account_idx = *ix.accounts.get(idx).ok_or(ParseError::MissingAccount("account"))? as usize;
        if account_idx >= account_keys.len() {
            return Err(ParseError::MissingAccount("account index out of bounds"));
        }
        account_keys[account_idx]
            .parse()
            .map_err(|_| ParseError::InvalidPubkey)
    };

    match ix_type {
        TapeInstruction::AdvanceEpoch => Ok(Some(ParsedInstruction::AdvanceEpoch)),

        TapeInstruction::SyncEpoch => {
            let node = get_account(3)?;
            let args = ix::SyncEpoch::try_from_bytes(&ix_data[1..])
                .map_err(|e| ParseError::Deserialization(e.to_string()))?;
            Ok(Some(ParsedInstruction::SyncEpoch {
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
            Ok(Some(ParsedInstruction::RegisterTrack {
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
            Ok(Some(ParsedInstruction::DeleteTrack { owner, track }))
        }

        TapeInstruction::CertifyTrack => {
            let track = get_account(4)?;
            // TODO: Parse epoch from instruction data or derive from current state
            Ok(Some(ParsedInstruction::CertifyTrack {
                track,
                epoch: EpochNumber(0), // Placeholder
            }))
        }

        TapeInstruction::InvalidateTrack => {
            let track = get_account(2)?;
            Ok(Some(ParsedInstruction::InvalidateTrack { track }))
        }

        TapeInstruction::ReserveTape => {
            let owner = get_account(0)?;
            let tape = get_account(2)?;
            Ok(Some(ParsedInstruction::ReserveTape { owner, tape }))
        }

        TapeInstruction::DestroyTape => {
            let owner = get_account(0)?;
            let tape = get_account(1)?;
            Ok(Some(ParsedInstruction::DestroyTape { owner, tape }))
        }

        TapeInstruction::RegisterNode => {
            let authority = get_account(0)?;
            let node = get_account(4)?;
            Ok(Some(ParsedInstruction::RegisterNode { authority, node }))
        }

        TapeInstruction::JoinNetwork => {
            let node = get_account(3)?;
            Ok(Some(ParsedInstruction::JoinNetwork { node }))
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
        };

        let result = parse_block(&block).unwrap();
        assert!(result.instructions.is_empty());
        assert_eq!(result.tx_count, 0);
    }
}
