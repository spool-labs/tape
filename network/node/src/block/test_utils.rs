//! Test utilities for block processor tests.
//!
//! This module provides helpers for constructing mock Solana transactions
//! with tapedrive instructions and events for testing the parser.

use base64::Engine;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, EncodedTransactionWithStatusMeta,
    UiCompiledInstruction, UiMessage, UiRawMessage, UiTransaction, UiTransactionStatusMeta,
};
use tape_api::event::EventType;
use tape_api::instruction::TapeInstruction;

/// Encode an event as a "Program data:" log line.
///
/// Events are serialized with an 8-byte discriminator prefix (first byte is EventType,
/// rest are zeros) followed by the event struct bytes.
pub fn encode_event<T: bytemuck::Pod>(event_type: EventType, event: &T) -> String {
    let mut data = vec![0u8; 8];
    data[0] = event_type as u8;
    data.extend_from_slice(bytemuck::bytes_of(event));
    let encoded = base64::engine::general_purpose::STANDARD.encode(&data);
    format!("Program data: {}", encoded)
}

/// Builder for constructing test transactions with tapedrive instructions and events.
///
/// Hides the complexity of Solana's transaction types behind a fluent API,
/// making it easy to create mock transactions for parser tests.
///
/// # Example
///
/// ```ignore
/// use tape_node::block::test_utils::TestTransaction;
/// use tape_api::instruction::TapeInstruction;
/// use tape_api::event::{EventType, EpochAdvanced};
///
/// let epoch_event = EpochAdvanced { /* ... */ };
///
/// let tx = TestTransaction::new()
///     .with_account(owner)
///     .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
///     .with_event(EventType::EpochAdvanced, &epoch_event)
///     .build();
///
/// let parsed = parse_transaction(&tx).unwrap();
/// ```
pub struct TestTransaction {
    /// Account keys referenced by instructions
    accounts: Vec<Pubkey>,
    /// Instructions to include: (type, account_indices, additional_data)
    instructions: Vec<(TapeInstruction, Vec<u8>, Vec<u8>)>,
    /// Events to include in logs (already encoded as "Program data:" lines)
    events: Vec<String>,
    /// Whether transaction should be marked as failed
    failed: bool,
}

impl TestTransaction {
    /// Create a new empty test transaction builder.
    pub fn new() -> Self {
        Self {
            accounts: vec![],
            instructions: vec![],
            events: vec![],
            failed: false,
        }
    }

    /// Add an account to the transaction's account list.
    ///
    /// Accounts are deduplicated - adding the same pubkey twice will not
    /// create a duplicate entry.
    pub fn with_account(mut self, pubkey: Pubkey) -> Self {
        if !self.accounts.contains(&pubkey) {
            self.accounts.push(pubkey);
        }
        self
    }

    /// Add a tapedrive instruction to the transaction.
    ///
    /// # Arguments
    /// * `ix_type` - The instruction discriminator (e.g., `TapeInstruction::AdvanceEpoch`)
    /// * `account_indices` - Indices into the account list for this instruction
    /// * `data` - Additional instruction data (after the discriminator byte)
    pub fn with_instruction(
        mut self,
        ix_type: TapeInstruction,
        account_indices: Vec<u8>,
        data: Vec<u8>,
    ) -> Self {
        self.instructions.push((ix_type, account_indices, data));
        self
    }

    /// Add an event to the transaction's log messages.
    ///
    /// The event will be serialized and encoded as a "Program data:" log line.
    pub fn with_event<T: bytemuck::Pod>(mut self, event_type: EventType, event: &T) -> Self {
        self.events.push(encode_event(event_type, event));
        self
    }

    /// Mark the transaction as failed.
    ///
    /// Failed transactions have their status set to an error, which causes
    /// the parser to skip them.
    pub fn as_failed(mut self) -> Self {
        self.failed = true;
        self
    }

    /// Build the final `EncodedTransactionWithStatusMeta`.
    ///
    /// This constructs all the nested Solana types required for a complete
    /// transaction representation that can be passed to `parse_transaction`.
    pub fn build(self) -> EncodedTransactionWithStatusMeta {
        // Ensure tapedrive program is in accounts
        let mut accounts = self.accounts;
        let program_id = tape_api::program::tapedrive::ID;
        let program_idx = if let Some(idx) = accounts.iter().position(|a| *a == program_id) {
            idx
        } else {
            accounts.push(program_id);
            accounts.len() - 1
        };

        // Build compiled instructions
        let compiled_instructions: Vec<UiCompiledInstruction> = self
            .instructions
            .iter()
            .map(|(ix_type, account_indices, data)| {
                // Build instruction data: discriminator + additional data
                let mut ix_data = vec![*ix_type as u8];
                ix_data.extend(data);

                UiCompiledInstruction {
                    program_id_index: program_idx as u8,
                    accounts: account_indices.clone(),
                    data: bs58::encode(&ix_data).into_string(),
                    stack_height: None,
                }
            })
            .collect();

        // Build log messages with program invoke/success wrapper
        let mut log_messages = vec![format!("Program {} invoke [1]", program_id)];
        log_messages.extend(self.events);
        log_messages.push(format!("Program {} success", program_id));

        // Build the transaction message
        let raw_message = UiRawMessage {
            header: solana_sdk::message::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: accounts.iter().map(|p| p.to_string()).collect(),
            recent_blockhash: "11111111111111111111111111111111".to_string(),
            instructions: compiled_instructions,
            address_table_lookups: None,
        };

        let ui_tx = UiTransaction {
            signatures: vec!["signature".to_string()],
            message: UiMessage::Raw(raw_message),
        };

        // Build transaction metadata
        let meta = UiTransactionStatusMeta {
            err: if self.failed {
                Some(solana_sdk::transaction::TransactionError::AccountNotFound.into())
            } else {
                None
            },
            status: if self.failed {
                Err(solana_sdk::transaction::TransactionError::AccountNotFound.into())
            } else {
                Ok(())
            },
            fee: 5000,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: OptionSerializer::None,
            log_messages: OptionSerializer::Some(log_messages),
            pre_token_balances: OptionSerializer::None,
            post_token_balances: OptionSerializer::None,
            rewards: OptionSerializer::None,
            loaded_addresses: OptionSerializer::None,
            return_data: OptionSerializer::None,
            compute_units_consumed: OptionSerializer::None,
            cost_units: OptionSerializer::None,
        };

        EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Json(ui_tx),
            meta: Some(meta),
            version: None,
        }
    }
}

impl Default for TestTransaction {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_api::event::EpochAdvanced;
    use tape_core::types::{EpochNumber, StorageUnits};

    #[test]
    fn test_encode_event() {
        let event = EpochAdvanced {
            old_epoch: EpochNumber(1),
            new_epoch: EpochNumber(2),
            timestamp: [0; 8],
            committee_size: [0; 8],
            total_stake: [0; 8],
            storage_price: [0; 8],
            storage_capacity: StorageUnits(0),
        };

        let encoded = encode_event(EventType::EpochAdvanced, &event);
        assert!(encoded.starts_with("Program data: "));

        // Decode and verify
        let data_part = encoded.strip_prefix("Program data: ").unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(data_part)
            .unwrap();

        // First byte should be EventType::EpochAdvanced (0x40)
        assert_eq!(decoded[0], 0x40);
    }

    #[test]
    fn test_transaction_builder_adds_program() {
        let tx = TestTransaction::new()
            .with_instruction(TapeInstruction::AdvanceEpoch, vec![], vec![])
            .build();

        // Verify program ID was added to accounts
        if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
            if let UiMessage::Raw(msg) = &ui_tx.message {
                assert!(msg
                    .account_keys
                    .contains(&tape_api::program::tapedrive::ID.to_string()));
            }
        }
    }

    #[test]
    fn test_transaction_builder_failed() {
        let tx = TestTransaction::new().as_failed().build();

        assert!(tx.meta.as_ref().unwrap().status.is_err());
        assert!(tx.meta.as_ref().unwrap().err.is_some());
    }

    #[test]
    fn test_transaction_builder_deduplicates_accounts() {
        let pubkey = Pubkey::new_unique();
        let tx = TestTransaction::new()
            .with_account(pubkey)
            .with_account(pubkey) // Duplicate
            .with_account(pubkey) // Another duplicate
            .build();

        if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
            if let UiMessage::Raw(msg) = &ui_tx.message {
                // Should only have the pubkey once, plus the program ID
                let count = msg
                    .account_keys
                    .iter()
                    .filter(|k| *k == &pubkey.to_string())
                    .count();
                assert_eq!(count, 1);
            }
        }
    }
}
