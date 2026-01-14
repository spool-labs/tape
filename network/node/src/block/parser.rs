//! Transaction and block parsing for tapedrive instructions.
//!
//! This module provides a thin wrapper around `tape_blocks` that returns
//! merged instructions (with events embedded) suitable for node operation.
//!
//! Events are emitted via `sol_log_data` and contain execution-time state,
//! which allows correct processing during historical catch-up.

use solana_transaction_status::UiConfirmedBlock;

// Re-export types from the shared crate
pub use tape_blocks::{ParseError, ParsedInstruction, TapedriveEvent};

/// Result of parsing a single block.
///
/// This is the node-specific view that contains merged instructions
/// (instructions with their corresponding events embedded).
#[derive(Debug, Default)]
pub struct ParsedBlock {
    /// Parsed instructions with events merged.
    pub instructions: Vec<ParsedInstruction>,
    /// Number of transactions processed.
    pub tx_count: usize,
    /// Number of failed transactions skipped.
    pub failed_tx_count: usize,
}

/// Parse a confirmed block for tapedrive instructions.
///
/// This function parses the block and automatically merges events with
/// their corresponding instructions. For event-only processing, use
/// `tape_blocks::parse()` directly.
pub fn parse_block(block: &UiConfirmedBlock) -> Result<ParsedBlock, ParseError> {
    let raw = tape_blocks::parse(block)?;

    let instructions = tape_blocks::merge(raw.raw_instructions, raw.events)?;

    Ok(ParsedBlock {
        instructions,
        tx_count: raw.tx_count,
        failed_tx_count: raw.failed_tx_count,
    })
}

#[cfg(test)]
mod tests {
    use super::super::test_utils::TestTransaction;
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use solana_transaction_status::UiConfirmedBlock;
    use tape_api::event::{EpochAdvanced, EventType, TrackCertified};
    use tape_api::instruction::TapeInstruction;
    use tape_core::prelude::*;

    // -------------------------------------------------------------------------
    // parse_block tests
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
    fn test_parse_block_advance_epoch() {
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
            .build();

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

        let result = parse_block(&block).unwrap();

        assert_eq!(result.instructions.len(), 1);
        assert_eq!(result.tx_count, 1);

        match &result.instructions[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.old_epoch, EpochNumber(5));
                assert_eq!(event.new_epoch, EpochNumber(6));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }
    }

    #[test]
    fn test_parse_block_certify_track() {
        let track = Pubkey::new_unique();

        let event = TrackCertified {
            track,
            epoch: EpochNumber(10),
            signer_count: [0; 8],
            signer_weight: [0; 8],
        };

        // CertifyTrack: account[5] is the track
        // Account order: 0=fee_payer, 1=authority, 2=system, 3=epoch, 4=tape, 5=track
        let tx = TestTransaction::new()
            .with_account(Pubkey::new_unique()) // 0: fee_payer
            .with_account(Pubkey::new_unique()) // 1: authority
            .with_account(Pubkey::new_unique()) // 2: system
            .with_account(Pubkey::new_unique()) // 3: epoch
            .with_account(Pubkey::new_unique()) // 4: tape
            .with_account(track)                // 5: track
            .with_instruction(
                TapeInstruction::CertifyTrack,
                vec![0, 1, 2, 3, 4, 5],
                vec![], // CertifyTrack has no additional data
            )
            .with_event(EventType::TrackCertified, &event)
            .build();

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

        let result = parse_block(&block).unwrap();

        assert_eq!(result.instructions.len(), 1);
        match &result.instructions[0] {
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
    fn test_parse_block_multiple_instructions() {
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

        // Account order for CertifyTrack: 0=fee_payer, 1=authority, 2=system, 3=epoch, 4=tape, 5=track
        let tx = TestTransaction::new()
            .with_account(owner)                // 0: fee_payer
            .with_account(Pubkey::new_unique()) // 1: authority
            .with_account(Pubkey::new_unique()) // 2: system
            .with_account(Pubkey::new_unique()) // 3: epoch
            .with_account(Pubkey::new_unique()) // 4: tape
            .with_account(track)                // 5: track
            // First instruction: AdvanceEpoch
            .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
            .with_event(EventType::EpochAdvanced, &epoch_event)
            // Second instruction: CertifyTrack
            .with_instruction(TapeInstruction::CertifyTrack, vec![0, 1, 2, 3, 4, 5], vec![])
            .with_event(EventType::TrackCertified, &certify_event)
            .build();

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

        let result = parse_block(&block).unwrap();

        assert_eq!(result.instructions.len(), 2);

        match &result.instructions[0] {
            ParsedInstruction::AdvanceEpoch { event } => {
                assert_eq!(event.new_epoch, EpochNumber(2));
            }
            _ => panic!("Expected AdvanceEpoch"),
        }

        match &result.instructions[1] {
            ParsedInstruction::CertifyTrack { track: t, event } => {
                assert_eq!(*t, track);
                assert_eq!(event.epoch, EpochNumber(2));
            }
            _ => panic!("Expected CertifyTrack"),
        }
    }

    #[test]
    fn test_parse_block_failed_tx_skipped() {
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

        let result = parse_block(&block).unwrap();

        // Failed transactions should be skipped
        assert!(result.instructions.is_empty());
        assert_eq!(result.tx_count, 0);
        assert_eq!(result.failed_tx_count, 1);
    }

    #[test]
    fn test_parse_block_delete_track_optional_event() {
        let track = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        // DeleteTrack without event (event is optional)
        // Account order: 0=fee_payer, 1=authority, 2=tape, 3=track, 4=system, 5=rent
        let tx = TestTransaction::new()
            .with_account(owner)                // 0: fee_payer
            .with_account(Pubkey::new_unique()) // 1: authority
            .with_account(Pubkey::new_unique()) // 2: tape
            .with_account(track)                // 3: track
            .with_instruction(TapeInstruction::DeleteTrack, vec![0, 1, 2, 3], vec![])
            // No event - it's optional
            .build();

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

        let result = parse_block(&block).unwrap();

        assert_eq!(result.instructions.len(), 1);
        match &result.instructions[0] {
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
}
