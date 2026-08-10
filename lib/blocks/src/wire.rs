//! Minimal deserialisation of a `getBlock` response.
//!
//! The typed solana client walks every response byte into a `serde_json::Value`
//! tree and back out through `from_value`, and its `EncodedTransaction`,
//! `UiMessage` and `UiInstruction` are `#[serde(untagged)]`, so each one buffers
//! into a second intermediate tree just to pick a variant.
//!
//! We pin the request to `encoding: json` and `transactionDetails: full`, so the
//! response shape is fixed and none of that is needed. Only the fields the
//! parser reads are declared here, and serde drops the rest without building
//! them. Balances alone are 40% of a mainnet block and are never read.

use serde::Deserialize;
use serde::de::IgnoredAny;

/// A confirmed block, carrying only what the parser consumes.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Block {
    pub previous_blockhash: String,
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub transactions: Option<Vec<Transaction>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct Transaction {
    pub transaction: TransactionBody,
    pub meta: Option<Meta>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TransactionBody {
    pub signatures: Vec<String>,
    pub message: Message,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Message {
    pub account_keys: Vec<String>,
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Meta {
    /// Null on success. Held as `IgnoredAny` because only its presence is read,
    /// so a failed transaction never builds its error tree.
    pub err: Option<IgnoredAny>,
    pub log_messages: Option<Vec<String>>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub loaded_addresses: Option<LoadedAddresses>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct InnerInstructions {
    pub index: u8,
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct LoadedAddresses {
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
}

impl Transaction {
    /// A transaction with no meta is treated as failed, matching the typed path.
    pub fn is_failed(&self) -> bool {
        self.meta.as_ref().is_none_or(|meta| meta.err.is_some())
    }
}

/// Build the wire shape from the typed one.
///
/// The hot path deserialises straight into these types. litesvm has no JSON at
/// all, it builds `UiConfirmedBlock` structurally, so it converts rather than
/// growing a second construction path. The parser tests do the same.
mod from_typed {
        use solana_transaction_status::{
        EncodedTransaction, EncodedTransactionWithStatusMeta, UiCompiledInstruction,
        UiConfirmedBlock, UiInstruction, UiMessage,
    };

    use super::{
        Block, CompiledInstruction, IgnoredAny, InnerInstructions, LoadedAddresses, Message, Meta,
        Transaction, TransactionBody,
    };

    impl From<UiConfirmedBlock> for Block {
        fn from(block: UiConfirmedBlock) -> Self {
            Self {
                previous_blockhash: block.previous_blockhash,
                blockhash: block.blockhash,
                parent_slot: block.parent_slot,
                block_time: block.block_time,
                transactions: block
                    .transactions
                    .map(|txs| txs.into_iter().map(Transaction::from).collect()),
            }
        }
    }

    impl From<UiCompiledInstruction> for CompiledInstruction {
        fn from(ix: UiCompiledInstruction) -> Self {
            Self {
                program_id_index: ix.program_id_index,
                accounts: ix.accounts,
                data: ix.data,
            }
        }
    }

    impl From<EncodedTransactionWithStatusMeta> for Transaction {
        fn from(tx: EncodedTransactionWithStatusMeta) -> Self {
            let body = match tx.transaction {
                EncodedTransaction::Json(ui_tx) => TransactionBody {
                    signatures: ui_tx.signatures,
                    message: match ui_tx.message {
                        UiMessage::Raw(raw) => Message {
                            account_keys: raw.account_keys,
                            instructions: raw
                                .instructions
                                .into_iter()
                                .map(CompiledInstruction::from)
                                .collect(),
                        },
                        // The parser skips parsed messages, so an empty one is
                        // the same outcome without carrying the variant across.
                        UiMessage::Parsed(_) => Message::default(),
                    },
                },
                _ => TransactionBody::default(),
            };

            let meta = tx.meta.map(|meta| Meta {
                err: meta.status.is_err().then_some(IgnoredAny),
                log_messages: meta.log_messages.into(),
                inner_instructions: meta.inner_instructions.map(|sets| {
                    sets.into_iter()
                        .map(|set| InnerInstructions {
                            index: set.index,
                            instructions: set
                                .instructions
                                .into_iter()
                                .filter_map(|ix| match ix {
                                    UiInstruction::Compiled(ix) => Some(ix.into()),
                                    UiInstruction::Parsed(_) => None,
                                })
                                .collect(),
                        })
                        .collect()
                }),
                loaded_addresses: meta.loaded_addresses.map(|loaded| LoadedAddresses {
                    writable: loaded.writable,
                    readonly: loaded.readonly,
                }),
            });

            Self {
                transaction: body,
                meta,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Shaped like a real getBlock response, including the fields we drop, so a
    // rename that misses camelCase shows up as an empty field rather than as a
    // deserialisation error.
    const RESPONSE: &str = r#"{
      "blockhash": "9zc1abc",
      "previousBlockhash": "8yb0def",
      "parentSlot": 430,
      "blockTime": 1700000000,
      "blockHeight": 400,
      "rewards": [],
      "numRewardPartitions": null,
      "transactions": [
        {
          "transaction": {
            "signatures": ["5sig"],
            "message": {
              "header": {
                "numRequiredSignatures": 1,
                "numReadonlySignedAccounts": 0,
                "numReadonlyUnsignedAccounts": 1
              },
              "accountKeys": ["AccountOne", "ProgramOne"],
              "recentBlockhash": "7xa9ghi",
              "instructions": [
                {"programIdIndex": 1, "accounts": [0], "data": "3Bxs", "stackHeight": null}
              ],
              "addressTableLookups": []
            }
          },
          "meta": {
            "err": null,
            "status": {"Ok": null},
            "fee": 5000,
            "preBalances": [100, 200],
            "postBalances": [95, 200],
            "preTokenBalances": [],
            "postTokenBalances": [],
            "rewards": [],
            "computeUnitsConsumed": 11965,
            "innerInstructions": [
              {"index": 0, "instructions": [
                {"programIdIndex": 1, "accounts": [0, 1], "data": "inner", "stackHeight": 2}
              ]}
            ],
            "logMessages": ["Program ProgramOne invoke [1]", "Program ProgramOne success"],
            "loadedAddresses": {"writable": ["WritableOne"], "readonly": ["ReadonlyOne"]}
          }
        }
      ]
    }"#;

    #[test]
    fn deserialises_every_field_the_parser_reads() {
        let block: Block = serde_json::from_str(RESPONSE).expect("valid block");

        assert_eq!(block.blockhash, "9zc1abc");
        assert_eq!(block.previous_blockhash, "8yb0def");
        assert_eq!(block.parent_slot, 430);
        assert_eq!(block.block_time, Some(1700000000));

        let txs = block.transactions.expect("transactions present");
        assert_eq!(txs.len(), 1);
        let tx = &txs[0];
        assert!(!tx.is_failed());
        assert_eq!(tx.transaction.signatures, ["5sig"]);
        assert_eq!(tx.transaction.message.account_keys, ["AccountOne", "ProgramOne"]);

        let ix = &tx.transaction.message.instructions[0];
        assert_eq!(ix.program_id_index, 1);
        assert_eq!(ix.accounts, [0]);
        assert_eq!(ix.data, "3Bxs");

        let meta = tx.meta.as_ref().expect("meta present");
        assert_eq!(
            meta.log_messages.as_deref().expect("logs present"),
            ["Program ProgramOne invoke [1]", "Program ProgramOne success"]
        );

        let inner = meta.inner_instructions.as_deref().expect("inner present");
        assert_eq!(inner[0].index, 0);
        assert_eq!(inner[0].instructions[0].program_id_index, 1);
        assert_eq!(inner[0].instructions[0].accounts, [0, 1]);
        assert_eq!(inner[0].instructions[0].data, "inner");

        let loaded = meta.loaded_addresses.as_ref().expect("loaded present");
        assert_eq!(loaded.writable, ["WritableOne"]);
        assert_eq!(loaded.readonly, ["ReadonlyOne"]);
    }

    #[test]
    fn err_marks_the_transaction_failed() {
        let failed = RESPONSE.replace(
            r#""err": null"#,
            r#""err": {"InstructionError": [1, {"Custom": 98}]}"#,
        );
        let block: Block = serde_json::from_str(&failed).expect("valid block");
        assert!(block.transactions.expect("transactions")[0].is_failed());
    }

    #[test]
    fn absent_transactions_and_meta_are_tolerated() {
        let block: Block = serde_json::from_str(r#"{"blockhash":"h","parentSlot":1}"#)
            .expect("sparse block");
        assert!(block.transactions.is_none());
        assert_eq!(block.blockhash, "h");

        let no_meta: Block =
            serde_json::from_str(r#"{"transactions":[{"transaction":{"signatures":[]}}]}"#)
                .expect("block without meta");
        // No meta reads as failed, which is what the typed path does.
        assert!(no_meta.transactions.expect("transactions")[0].is_failed());
    }
}
