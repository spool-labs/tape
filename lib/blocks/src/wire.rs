//! Minimal deserialisation of a `getBlock` response.
//!
//! The typed solana client walks every response byte into a `serde_json::Value`
//! tree and back out through `from_value`, and its `EncodedTransaction`,
//! `UiMessage` and `UiInstruction` are `#[serde(untagged)]`, so each one buffers
//! into a second intermediate tree just to pick a variant.
//!
//! We pin the request to `encoding: base64` and `transactionDetails: full`, so
//! the response shape is fixed and none of that is needed. Only the fields the
//! parser reads are declared here, and serde drops the rest without building
//! them. Balances alone are 40% of a mainnet block and are never read.
//!
//! The transaction arrives as one base64 blob, not a tree of strings to parse
//! and base58-decode. Meta keeps its json shape, so the event path is unchanged.

use serde::Deserialize;
use serde::de::IgnoredAny;
use tape_crypto::address::Address;

/// Ceiling on one decoded transaction, above what solana accepts today so a
/// larger limit upstream does not silently start failing here.
const MAX_TRANSACTION_BYTES: usize = 5 * 1024;

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

/// The transaction itself, decoded from the `[base64, "base64"]` pair.
#[derive(Debug, Clone, Default)]
pub struct TransactionBody {
    pub signatures: Vec<Signature>,
    pub message: Message,
}

#[derive(Debug, Clone, Default)]
pub struct Message {
    pub account_keys: Vec<Address>,
    pub instructions: Vec<CompiledInstruction>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    /// Raw payload. Base58 on the meta side, already bytes on the wire side.
    #[serde(deserialize_with = "base58_bytes")]
    pub data: Vec<u8>,
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
    #[serde(deserialize_with = "base58_addresses")]
    pub writable: Vec<Address>,
    #[serde(deserialize_with = "base58_addresses")]
    pub readonly: Vec<Address>,
}

/// A transaction signature, kept as bytes so a txid needs no decode.
pub type Signature = [u8; 64];

impl<'de> Deserialize<'de> for TransactionBody {
    /// Reads the `[payload, "base64"]` pair. The encoding name is checked, so an
    /// rpc still answering json fails loudly instead of dropping every event.
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let (payload, encoding) = <(String, String)>::deserialize(de)?;
        if encoding != "base64" {
            return Err(D::Error::custom(format!("transaction encoding {encoding}, want base64")));
        }
        let raw = base64::decode(payload)
            .map_err(|_| D::Error::custom("transaction is not base64"))?;
        // The limit is what keeps a length prefix from asking for an allocation;
        // wincode's own default ceiling is 4 MiB.
        let tx: solana_transaction::versioned::VersionedTransaction = wincode::config::deserialize(
            &raw,
            wincode::config::Configuration::default()
                .with_preallocation_size_limit::<MAX_TRANSACTION_BYTES>(),
        )
        .map_err(|_| D::Error::custom("transaction did not decode"))?;

        Ok(Self {
            signatures: tx.signatures.iter().filter_map(|sig| sig.as_ref().try_into().ok()).collect(),
            message: Message {
                account_keys: tx
                    .message
                    .static_account_keys()
                    .iter()
                    .map(|key| Address::from(key.to_bytes()))
                    .collect(),
                instructions: tx
                    .message
                    .instructions()
                    .iter()
                    .map(|ix| CompiledInstruction {
                        program_id_index: ix.program_id_index,
                        accounts: ix.accounts.clone(),
                        data: ix.data.clone(),
                    })
                    .collect(),
            },
        })
    }
}

fn base58_bytes<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
    let encoded = String::deserialize(de)?;
    bs58::decode(&encoded)
        .into_vec()
        .map_err(|_| serde::de::Error::custom("instruction data is not base58"))
}

fn base58_addresses<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Vec<Address>, D::Error> {
    let encoded = Vec::<String>::deserialize(de)?;
    encoded
        .iter()
        .map(|key| key.parse().map_err(|_| serde::de::Error::custom("address is not base58")))
        .collect()
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
                data: bs58::decode(&ix.data).into_vec().unwrap_or_default(),
            }
        }
    }

    impl From<EncodedTransactionWithStatusMeta> for Transaction {
        fn from(tx: EncodedTransactionWithStatusMeta) -> Self {
            let body = match tx.transaction {
                EncodedTransaction::Json(ui_tx) => TransactionBody {
                    signatures: ui_tx
                        .signatures
                        .iter()
                        .filter_map(|sig| {
                            bs58::decode(sig).into_vec().ok()?.as_slice().try_into().ok()
                        })
                        .collect(),
                    message: match ui_tx.message {
                        UiMessage::Raw(raw) => Message {
                            account_keys: raw
                                .account_keys
                                .iter()
                                .filter_map(|key| key.parse().ok())
                                .collect(),
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
                    writable: loaded.writable.iter().filter_map(|k| k.parse().ok()).collect(),
                    readonly: loaded.readonly.iter().filter_map(|k| k.parse().ok()).collect(),
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
          "transaction": ["__TX_B64__", "base64"],
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
                {"programIdIndex": 1, "accounts": [0, 1], "data": "3Bxs", "stackHeight": 2}
              ]}
            ],
            "logMessages": ["Program ProgramOne invoke [1]", "Program ProgramOne success"],
            "loadedAddresses": {"writable": ["__WRITABLE__"], "readonly": ["__READONLY__"]}
          }
        }
      ]
    }"#;

    /// The fixture with its placeholders filled in.
    fn response() -> String {
        let (encoded, _, _) = encoded_transaction();
        RESPONSE
            .replace("__TX_B64__", &encoded)
            .replace("__WRITABLE__", &bs58::encode([3u8; 32]).into_string())
            .replace("__READONLY__", &bs58::encode([4u8; 32]).into_string())
    }

    /// A one-instruction transaction in the shape the rpc returns under base64.
    fn encoded_transaction() -> (String, [u8; 32], [u8; 32]) {
        use solana_transaction::versioned::VersionedTransaction;

        let account = [7u8; 32];
        let program = [9u8; 32];
        let message = solana_message::Message::new_with_blockhash(
            &[solana_instruction::Instruction::new_with_bytes(
                program.into(),
                &[1, 2, 3],
                vec![solana_instruction::AccountMeta::new(account.into(), true)],
            )],
            Some(&account.into()),
            &Default::default(),
        );
        let tx = VersionedTransaction {
            signatures: vec![solana_transaction::Signature::from([5u8; 64])],
            message: solana_message::VersionedMessage::Legacy(message),
        };
        let raw = wincode::serialize(&tx).expect("encode");
        (base64::encode(raw), account, program)
    }

    #[test]
    fn deserialises_every_field_the_parser_reads() {
        let (_, account, program) = encoded_transaction();
        let writable = Address::from([3u8; 32]);
        let readonly = Address::from([4u8; 32]);
        let block: Block = serde_json::from_str(&response()).expect("valid block");

        assert_eq!(block.blockhash, "9zc1abc");
        assert_eq!(block.previous_blockhash, "8yb0def");
        assert_eq!(block.parent_slot, 430);
        assert_eq!(block.block_time, Some(1700000000));

        let txs = block.transactions.expect("transactions present");
        assert_eq!(txs.len(), 1);
        let tx = &txs[0];
        assert!(!tx.is_failed());
        assert_eq!(tx.transaction.signatures, [[5u8; 64]]);
        // The payer signs, so it leads the keys and the program follows.
        assert!(tx.transaction.message.account_keys.contains(&Address::from(account)));
        assert!(tx.transaction.message.account_keys.contains(&Address::from(program)));

        let ix = &tx.transaction.message.instructions[0];
        assert_eq!(ix.data, vec![1, 2, 3], "instruction payload survives as bytes");

        let meta = tx.meta.as_ref().expect("meta present");
        assert_eq!(
            meta.log_messages.as_deref().expect("logs present"),
            ["Program ProgramOne invoke [1]", "Program ProgramOne success"]
        );

        let inner = meta.inner_instructions.as_deref().expect("inner present");
        assert_eq!(inner[0].index, 0);
        assert_eq!(inner[0].instructions[0].program_id_index, 1);
        assert_eq!(inner[0].instructions[0].accounts, [0, 1]);
        assert_eq!(
            inner[0].instructions[0].data,
            bs58::decode("3Bxs").into_vec().expect("base58"),
            "meta instructions stay base58 in every encoding"
        );

        let loaded = meta.loaded_addresses.as_ref().expect("loaded present");
        assert_eq!(loaded.writable, [writable]);
        assert_eq!(loaded.readonly, [readonly]);
    }

    #[test]
    fn err_marks_the_transaction_failed() {
        let failed = response().replace(
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

        let (encoded, _, _) = encoded_transaction();
        let without_meta =
            format!(r#"{{"transactions":[{{"transaction":["{encoded}","base64"]}}]}}"#);
        let no_meta: Block =
            serde_json::from_str(&without_meta).expect("block without meta");
        // No meta reads as failed, which is what the typed path does.
        assert!(no_meta.transactions.expect("transactions")[0].is_failed());
    }
}
