use litesvm::types::TransactionResult;
use solana_sdk::clock::Slot;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock, TransactionDetails, TransactionWithStatusMeta,
    UiConfirmedBlock, UiTransactionEncoding, VersionedTransactionWithStatusMeta,
};

use crate::convert::tx_result_to_status_meta;

pub(crate) struct RecordedTransaction {
    pub tx: VersionedTransaction,
    pub result: TransactionResult,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
}

pub(crate) struct SlotData {
    pub blockhash: String,
    pub previous_blockhash: String,
    pub parent_slot: Slot,
    pub transactions: Vec<RecordedTransaction>,
    pub block_height: u64,
}

impl SlotData {
    fn to_confirmed_block(&self) -> ConfirmedBlock {
        let transactions = self
            .transactions
            .iter()
            .map(|recorded| {
                TransactionWithStatusMeta::Complete(VersionedTransactionWithStatusMeta {
                    transaction: recorded.tx.clone(),
                    meta: tx_result_to_status_meta(
                        &recorded.result,
                        recorded.pre_balances.clone(),
                        recorded.post_balances.clone(),
                    ),
                })
            })
            .collect();

        ConfirmedBlock {
            previous_blockhash: self.previous_blockhash.clone(),
            blockhash: self.blockhash.clone(),
            parent_slot: self.parent_slot,
            transactions,
            rewards: vec![],
            num_partitions: None,
            block_time: None,
            block_height: Some(self.block_height),
        }
    }

    pub fn to_ui_confirmed_block(&self) -> Result<UiConfirmedBlock, String> {
        let block = self.to_confirmed_block();
        block
            .encode_with_options(
                UiTransactionEncoding::Json,
                BlockEncodingOptions {
                    transaction_details: TransactionDetails::Full,
                    show_rewards: true,
                    max_supported_transaction_version: Some(0),
                },
            )
            .map_err(|e| e.to_string())
    }
}
