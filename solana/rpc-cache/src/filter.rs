//! Filter a `UiConfirmedBlock` down to tape-relevant data, in both encodings
//! the cache serves.
//!
//! Two effects: drop transactions that don't touch any program we care
//! about, and null out per-tx fields the node-side parser never reads.
//! The keep-predicate consults the decoded transaction's static account
//! keys, ALT-resolved `loaded_addresses`, AND `Program <id> invoke` log
//! lines so a transaction that references a tracked program through any
//! of those paths survives — the same ALT footgun the block parser had to
//! fix. Blocks arrive base64-encoded; the json form of each kept
//! transaction is rendered from its bytes for consumers that ask for it.

use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodableWithMeta, EncodedTransaction,
    EncodedTransactionWithStatusMeta, UiConfirmedBlock,
};

use tape_crypto::address::Address;

/// A filtered block: the block with its kept transactions as upstream sent
/// them (base64), and the json rendering of each, in the same order
pub struct FilteredBlock {
    pub block: UiConfirmedBlock,
    pub json_transactions: Vec<EncodedTransaction>,
}

/// Keep the transactions that touch a tracked program, stripped of what no
/// consumer reads, with a json rendering of each
pub fn filter_block(mut block: UiConfirmedBlock, program_ids: &[Address]) -> FilteredBlock {
    block.rewards = None;
    block.signatures = None;
    block.num_reward_partitions = None;

    let program_id_strings: Vec<String> = program_ids.iter().map(|p| p.to_string()).collect();
    let mut kept = Vec::new();
    let mut json_transactions = Vec::new();
    if let Some(transactions) = block.transactions.take() {
        for tx in transactions {
            let Some(decoded) = kept_transaction(&tx, program_ids, &program_id_strings) else {
                continue;
            };
            json_transactions.push(decoded.json_encode());
            kept.push(strip_tx(tx));
        }
    }
    block.transactions = Some(kept);
    FilteredBlock { block, json_transactions }
}

/// The decoded transaction when it succeeded and touches a tracked program
/// through its static keys, its loaded addresses, or its logs
fn kept_transaction(
    tx: &EncodedTransactionWithStatusMeta,
    program_ids: &[Address],
    program_id_strings: &[String],
) -> Option<VersionedTransaction> {
    let meta = tx.meta.as_ref()?;
    if meta.status.is_err() {
        return None;
    }
    let decoded = tx.transaction.decode()?;

    let (alt_writable, alt_readonly): (&[String], &[String]) = match &meta.loaded_addresses {
        OptionSerializer::Some(loaded) => (&loaded.writable, &loaded.readonly),
        OptionSerializer::None | OptionSerializer::Skip => (&[], &[]),
    };
    let logs: &[String] = match &meta.log_messages {
        OptionSerializer::Some(l) => l,
        OptionSerializer::None | OptionSerializer::Skip => &[],
    };

    let has_tracked_static_key = decoded
        .message
        .static_account_keys()
        .iter()
        .any(|key| program_ids.contains(&Address::from(key.to_bytes())));
    let has_tracked_loaded_or_log = program_id_strings.iter().any(|program_id| {
        alt_writable.iter().any(|k| k == program_id)
            || alt_readonly.iter().any(|k| k == program_id)
            || logs.iter().any(|line| log_invokes_program(line, program_id))
    });
    (has_tracked_static_key || has_tracked_loaded_or_log).then_some(decoded)
}

fn log_invokes_program(line: &str, pid: &str) -> bool {
    let Some(rest) = line.strip_prefix("Program ") else {
        return false;
    };
    let Some(rest) = rest.strip_prefix(pid) else {
        return false;
    };
    rest.starts_with(" invoke")
}

fn strip_tx(mut tx: EncodedTransactionWithStatusMeta) -> EncodedTransactionWithStatusMeta {
    if let Some(meta) = tx.meta.as_mut() {
        meta.fee = 0;
        meta.pre_balances = Vec::new();
        meta.post_balances = Vec::new();
        meta.pre_token_balances = OptionSerializer::Skip;
        meta.post_token_balances = OptionSerializer::Skip;
        meta.rewards = OptionSerializer::Skip;
        meta.return_data = OptionSerializer::Skip;
        meta.compute_units_consumed = OptionSerializer::Skip;
        meta.cost_units = OptionSerializer::Skip;
    }
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_instruction::{AccountMeta, Instruction};
    use solana_message::{Message, VersionedMessage};
    use solana_transaction::Signature;
    use solana_transaction_error::TransactionError;
    use solana_transaction_status::{
        TransactionBinaryEncoding, UiLoadedAddresses, UiMessage, UiTransactionStatusMeta,
    };

    fn pid() -> Address {
        Address::new_unique()
    }

    /// A one-instruction transaction that invokes `program`, base64-encoded
    /// as the rpc returns it; its static keys are the payer and `program`
    fn encoded_transaction(program: Address) -> EncodedTransaction {
        let payer = Address::new_unique();
        let message = Message::new_with_blockhash(
            &[Instruction::new_with_bytes(
                (*program.as_bytes()).into(),
                &[1, 2, 3],
                vec![AccountMeta::new((*payer.as_bytes()).into(), true)],
            )],
            Some(&(*payer.as_bytes()).into()),
            &Default::default(),
        );
        let tx = VersionedTransaction {
            signatures: vec![Signature::from([5u8; 64])],
            message: VersionedMessage::Legacy(message),
        };
        let raw = wincode::serialize(&tx).expect("encode");
        EncodedTransaction::Binary(base64::encode(raw), TransactionBinaryEncoding::Base64)
    }

    fn make_tx(
        program: Address,
        loaded: Option<UiLoadedAddresses>,
        logs: Vec<String>,
        ok: bool,
    ) -> EncodedTransactionWithStatusMeta {
        let status = if ok {
            Ok(())
        } else {
            Err(TransactionError::AccountNotFound)
        };
        EncodedTransactionWithStatusMeta {
            transaction: encoded_transaction(program),
            meta: Some(UiTransactionStatusMeta {
                err: None,
                status: status.map_err(Into::into),
                fee: 5000,
                pre_balances: vec![1, 2, 3],
                post_balances: vec![1, 2, 3],
                inner_instructions: OptionSerializer::Skip,
                log_messages: OptionSerializer::Some(logs),
                pre_token_balances: OptionSerializer::Some(vec![]),
                post_token_balances: OptionSerializer::Some(vec![]),
                rewards: OptionSerializer::Some(vec![]),
                loaded_addresses: match loaded {
                    Some(l) => OptionSerializer::Some(l),
                    None => OptionSerializer::Skip,
                },
                return_data: OptionSerializer::Skip,
                compute_units_consumed: OptionSerializer::Some(1),
                cost_units: OptionSerializer::Skip,
            }),
            version: None,
        }
    }

    fn make_block(transactions: Vec<EncodedTransactionWithStatusMeta>) -> UiConfirmedBlock {
        UiConfirmedBlock {
            previous_blockhash: "prev".into(),
            blockhash: "this".into(),
            parent_slot: 99,
            transactions: Some(transactions),
            signatures: Some(vec!["dropme".into()]),
            rewards: Some(vec![]),
            block_time: Some(123),
            block_height: Some(7),
            num_reward_partitions: Some(2),
        }
    }

    fn kept(filtered: &FilteredBlock) -> &[EncodedTransactionWithStatusMeta] {
        filtered.block.transactions.as_deref().expect("transactions")
    }

    // a program among the static keys keeps the transaction
    #[test]
    fn keeps_static_key() {
        let p = pid();
        let tx = make_tx(p, None, vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(kept(&filtered).len(), 1);
        assert_eq!(filtered.json_transactions.len(), 1);
    }

    // a program only among the writable loaded addresses keeps it
    #[test]
    fn keeps_loaded_writable() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![p.to_string()],
            readonly: vec![],
        };
        let tx = make_tx(pid(), Some(loaded), vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(kept(&filtered).len(), 1);
    }

    // a program only among the readonly loaded addresses keeps it
    #[test]
    fn keeps_loaded_readonly() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![],
            readonly: vec![p.to_string()],
        };
        let tx = make_tx(pid(), Some(loaded), vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(kept(&filtered).len(), 1);
    }

    // a program invoked only from the logs keeps it
    #[test]
    fn keeps_logged_invoke() {
        let p = pid();
        let logs = vec![
            "Program 11111111111111111111111111111111 invoke [1]".into(),
            format!("Program {p} invoke [2]"),
            format!("Program {p} success"),
        ];
        let tx = make_tx(pid(), None, logs, true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(kept(&filtered).len(), 1);
    }

    // a transaction that touches no tracked program is dropped
    #[test]
    fn drops_unrelated() {
        let tx = make_tx(pid(), None, vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[pid()]);
        assert!(kept(&filtered).is_empty());
        assert!(filtered.json_transactions.is_empty());
    }

    // a failed transaction is dropped even when it names the program
    #[test]
    fn drops_failed() {
        let p = pid();
        let tx = make_tx(p, None, vec![], false);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert!(kept(&filtered).is_empty());
    }

    // a transaction whose payload does not decode is dropped
    #[test]
    fn drops_undecodable() {
        let p = pid();
        let mut tx = make_tx(p, None, vec![], true);
        tx.transaction = EncodedTransaction::Binary("not base64".into(), TransactionBinaryEncoding::Base64);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert!(kept(&filtered).is_empty());
    }

    // a transaction without meta is dropped
    #[test]
    fn drops_without_meta() {
        let p = pid();
        let mut tx = make_tx(p, None, vec![], true);
        tx.meta = None;
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert!(kept(&filtered).is_empty());
    }

    // the json rendering carries the decoded keys beside the base64 original
    #[test]
    fn renders_json() {
        let p = pid();
        let logs = vec![format!("Program {p} invoke [1]")];
        let tx = make_tx(p, None, logs, true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        assert!(matches!(kept(&filtered)[0].transaction, EncodedTransaction::Binary(_, TransactionBinaryEncoding::Base64)));
        assert_eq!(filtered.json_transactions.len(), 1);
        assert!(matches!(filtered.json_transactions[0], EncodedTransaction::Json(_)));
        let EncodedTransaction::Json(ui) = &filtered.json_transactions[0] else {
            unreachable!()
        };
        assert!(matches!(ui.message, UiMessage::Raw(_)));
        let UiMessage::Raw(raw) = &ui.message else {
            unreachable!()
        };
        assert!(raw.account_keys.contains(&p.to_string()));
        assert_eq!(raw.instructions.len(), 1);
    }

    // per-transaction noise goes, the loaded addresses stay
    #[test]
    fn strips_noise() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![p.to_string()],
            readonly: vec![pid().to_string()],
        };
        let tx = make_tx(pid(), Some(loaded), vec!["Program x invoke [1]".into()], true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        let meta = kept(&filtered)[0].meta.as_ref().expect("kept transaction carries meta");

        assert_eq!(meta.fee, 0);
        assert!(meta.pre_balances.is_empty());
        assert!(meta.post_balances.is_empty());
        assert!(matches!(meta.pre_token_balances, OptionSerializer::Skip));
        assert!(matches!(meta.post_token_balances, OptionSerializer::Skip));
        assert!(matches!(meta.rewards, OptionSerializer::Skip));
        assert!(matches!(meta.return_data, OptionSerializer::Skip));
        assert!(matches!(meta.compute_units_consumed, OptionSerializer::Skip));
        assert!(matches!(meta.cost_units, OptionSerializer::Skip));

        assert!(meta.status.is_ok());
        assert!(matches!(meta.log_messages, OptionSerializer::Some(_)));
        assert!(matches!(meta.loaded_addresses, OptionSerializer::Some(_)));
        let OptionSerializer::Some(loaded) = &meta.loaded_addresses else {
            unreachable!()
        };
        assert_eq!(loaded.writable.len(), 1);
        assert_eq!(loaded.readonly.len(), 1);
    }

    // block-level noise goes, the header fields stay
    #[test]
    fn strips_block_noise() {
        let p = pid();
        let tx = make_tx(p, None, vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[p]);
        let block = &filtered.block;

        assert!(block.rewards.is_none());
        assert!(block.signatures.is_none());
        assert!(block.num_reward_partitions.is_none());
        assert_eq!(block.previous_blockhash, "prev");
        assert_eq!(block.blockhash, "this");
        assert_eq!(block.parent_slot, 99);
        assert_eq!(block.block_time, Some(123));
        assert_eq!(block.block_height, Some(7));
    }

    // a block with nothing kept still carries an empty list, not none
    #[test]
    fn empty_block() {
        let tx = make_tx(pid(), None, vec![], true);
        let filtered = filter_block(make_block(vec![tx]), &[pid()]);
        assert!(kept(&filtered).is_empty());
        assert!(filtered.json_transactions.is_empty());
    }

    // the log match needs " invoke" right after the program id
    #[test]
    fn log_match() {
        let p_str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdef";
        assert!(log_invokes_program(&format!("Program {p_str} invoke [1]"), p_str));
        assert!(!log_invokes_program(&format!("Program {p_str}EXTRA invoke [1]"), p_str));
        assert!(!log_invokes_program(&format!("Program {p_str} success"), p_str));
    }
}
