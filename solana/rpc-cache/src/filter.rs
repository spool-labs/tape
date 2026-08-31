//! Filter a `UiConfirmedBlock` down to tape-relevant data.
//!
//! Two effects: drop transactions that don't touch any program we care
//! about, and null out per-tx fields the node-side parser never reads.
//! The keep-predicate consults static `account_keys`, ALT-resolved
//! `loaded_addresses`, AND `Program <id> invoke` log lines so a
//! transaction that references a tracked program through any of those
//! paths survives — the same ALT footgun the block parser had to fix.

use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction,
    EncodedTransactionWithStatusMeta, UiConfirmedBlock, UiMessage,
};
use tape_crypto::address::Address;

pub fn filter_block(
    mut block: UiConfirmedBlock,
    program_ids: &[Address],
) -> UiConfirmedBlock {
    block.rewards = None;
    block.signatures = None;
    block.num_reward_partitions = None;

    let pid_strs: Vec<String> = program_ids.iter().map(|p| p.to_string()).collect();

    if let Some(transactions) = block.transactions.take() {
        let kept: Vec<_> = transactions
            .into_iter()
            .filter_map(|tx| keep_tx(&tx, &pid_strs).then(|| strip_tx(tx)))
            .collect();
        block.transactions = Some(kept);
    }

    block
}

fn keep_tx(tx: &EncodedTransactionWithStatusMeta, pid_strs: &[String]) -> bool {
    let Some(meta) = &tx.meta else { return false };
    if meta.status.is_err() {
        return false;
    }

    let static_keys: &[String] = match &tx.transaction {
        EncodedTransaction::Json(ui_tx) => match &ui_tx.message {
            UiMessage::Raw(raw) => &raw.account_keys,
            _ => return false,
        },
        _ => return false,
    };

    let (alt_writable, alt_readonly): (&[String], &[String]) = match &meta.loaded_addresses {
        OptionSerializer::Some(loaded) => (&loaded.writable, &loaded.readonly),
        _ => (&[], &[]),
    };

    let logs: &[String] = match &meta.log_messages {
        OptionSerializer::Some(l) => l,
        _ => &[],
    };

    pid_strs.iter().any(|pid| {
        static_keys.iter().any(|k| k == pid)
            || alt_writable.iter().any(|k| k == pid)
            || alt_readonly.iter().any(|k| k == pid)
            || logs.iter().any(|line| log_invokes_program(line, pid))
    })
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
    use solana_message::MessageHeader;
    use solana_transaction_error::TransactionError;
    use solana_transaction_status::{
        UiCompiledInstruction, UiLoadedAddresses, UiRawMessage, UiTransaction,
        UiTransactionStatusMeta,
    };

    fn pid() -> Address {
        Address::new_unique()
    }

    fn make_tx(
        static_keys: Vec<String>,
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
            transaction: EncodedTransaction::Json(UiTransaction {
                signatures: vec!["sig".into()],
                message: UiMessage::Raw(UiRawMessage {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: static_keys,
                    recent_blockhash: "11111111111111111111111111111111".into(),
                    instructions: vec![UiCompiledInstruction {
                        program_id_index: 0,
                        accounts: vec![],
                        data: String::new(),
                        stack_height: None,
                    }],
                    address_table_lookups: None,
                }),
            }),
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

    #[test]
    fn keeps_when_program_in_static_keys() {
        let p = pid();
        let tx = make_tx(vec![p.to_string(), "other".into()], None, vec![], true);
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(block.transactions.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn keeps_when_program_in_alt_writable() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![p.to_string()],
            readonly: vec![],
        };
        let tx = make_tx(vec!["other".into()], Some(loaded), vec![], true);
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(block.transactions.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn keeps_when_program_in_alt_readonly() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![],
            readonly: vec![p.to_string()],
        };
        let tx = make_tx(vec!["other".into()], Some(loaded), vec![], true);
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(block.transactions.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn keeps_when_program_only_in_logs() {
        let p = pid();
        let logs = vec![
            "Program 11111111111111111111111111111111 invoke [1]".into(),
            format!("Program {p} invoke [2]"),
            format!("Program {p} success"),
        ];
        let tx = make_tx(vec!["other".into()], None, logs, true);
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert_eq!(block.transactions.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn drops_unrelated_tx() {
        let tracked = pid();
        let other = pid();
        let tx = make_tx(vec![other.to_string()], None, vec![], true);
        let block = filter_block(make_block(vec![tx]), &[tracked]);
        assert!(block.transactions.as_ref().unwrap().is_empty());
    }

    #[test]
    fn drops_failed_tx_even_if_program_present() {
        let p = pid();
        let tx = make_tx(vec![p.to_string()], None, vec![], false);
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert!(block.transactions.as_ref().unwrap().is_empty());
    }

    #[test]
    fn drops_non_json_encoding() {
        let p = pid();
        let mut tx = make_tx(vec![p.to_string()], None, vec![], true);
        tx.transaction = EncodedTransaction::LegacyBinary("base58".into());
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert!(block.transactions.as_ref().unwrap().is_empty());
    }

    #[test]
    fn drops_when_no_meta() {
        let p = pid();
        let mut tx = make_tx(vec![p.to_string()], None, vec![], true);
        tx.meta = None;
        let block = filter_block(make_block(vec![tx]), &[p]);
        assert!(block.transactions.as_ref().unwrap().is_empty());
    }

    #[test]
    fn strips_per_tx_noise_but_preserves_loaded_addresses() {
        let p = pid();
        let loaded = UiLoadedAddresses {
            writable: vec![p.to_string()],
            readonly: vec!["readonly".into()],
        };
        let tx = make_tx(vec!["other".into()], Some(loaded), vec![], true);
        let block = filter_block(make_block(vec![tx]), &[p]);

        let kept = &block.transactions.as_ref().unwrap()[0];
        let meta = kept.meta.as_ref().unwrap();

        assert_eq!(meta.fee, 0);
        assert!(meta.pre_balances.is_empty());
        assert!(meta.post_balances.is_empty());
        assert!(matches!(meta.pre_token_balances, OptionSerializer::Skip));
        assert!(matches!(meta.post_token_balances, OptionSerializer::Skip));
        assert!(matches!(meta.rewards, OptionSerializer::Skip));
        assert!(matches!(meta.return_data, OptionSerializer::Skip));
        assert!(matches!(meta.compute_units_consumed, OptionSerializer::Skip));
        assert!(matches!(meta.cost_units, OptionSerializer::Skip));

        // Preserved.
        assert!(meta.status.is_ok());
        assert!(matches!(meta.log_messages, OptionSerializer::Some(_)));
        match &meta.loaded_addresses {
            OptionSerializer::Some(l) => {
                assert_eq!(l.writable.len(), 1);
                assert_eq!(l.readonly.len(), 1);
            }
            _ => panic!("loaded_addresses must be preserved"),
        }
    }

    #[test]
    fn strips_block_level_noise() {
        let p = pid();
        let tx = make_tx(vec![p.to_string()], None, vec![], true);
        let block = filter_block(make_block(vec![tx]), &[p]);

        assert!(block.rewards.is_none());
        assert!(block.signatures.is_none());
        assert!(block.num_reward_partitions.is_none());

        // Preserved.
        assert_eq!(block.previous_blockhash, "prev");
        assert_eq!(block.blockhash, "this");
        assert_eq!(block.parent_slot, 99);
        assert_eq!(block.block_time, Some(123));
        assert_eq!(block.block_height, Some(7));
    }

    #[test]
    fn empty_filtered_block_is_well_formed() {
        let tracked = pid();
        let other = pid();
        let tx = make_tx(vec![other.to_string()], None, vec![], true);
        let block = filter_block(make_block(vec![tx]), &[tracked]);

        // transactions stays Some(vec![]), distinct from None.
        assert!(block.transactions.is_some());
        assert!(block.transactions.as_ref().unwrap().is_empty());
    }

    #[test]
    fn log_prefix_match_does_not_partial_match_program_id() {
        // Defensive: a substring match could mistake "abc1invoke" for "abc invoke".
        // We require " invoke" (space) immediately after the pid.
        let p_str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdef";
        assert!(log_invokes_program(
            &format!("Program {p_str} invoke [1]"),
            p_str
        ));
        assert!(!log_invokes_program(
            &format!("Program {p_str}EXTRA invoke [1]"),
            p_str
        ));
        assert!(!log_invokes_program(
            &format!("Program {p_str} success"),
            p_str
        ));
    }
}
