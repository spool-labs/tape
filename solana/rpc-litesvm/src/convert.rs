use litesvm::types::{FailedTransactionMetadata, TransactionMetadata, TransactionResult};
use solana_transaction::TransactionError;
use solana_transaction_status::{
    InnerInstruction, InnerInstructions, TransactionConfirmationStatus, TransactionStatus,
    TransactionStatusMeta,
};

fn meta_to_inner_instructions(
    inner: &[Vec<solana_message::inner_instruction::InnerInstruction>],
) -> Vec<InnerInstructions> {
    inner
        .iter()
        .enumerate()
        .filter(|(_, ixs)| !ixs.is_empty())
        .map(|(idx, ixs)| InnerInstructions {
            index: idx as u8,
            instructions: ixs
                .iter()
                .map(|ix| InnerInstruction {
                    instruction: ix.instruction.clone(),
                    stack_height: Some(ix.stack_height as u32),
                })
                .collect(),
        })
        .collect()
}

fn success_to_status_meta(
    meta: &TransactionMetadata,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
) -> TransactionStatusMeta {
    TransactionStatusMeta {
        status: Ok(()),
        fee: pre_balances
            .first()
            .zip(post_balances.first())
            .map(|(pre, post)| pre.saturating_sub(*post))
            .unwrap_or(0),
        pre_balances,
        post_balances,
        inner_instructions: Some(meta_to_inner_instructions(&meta.inner_instructions)),
        log_messages: Some(meta.logs.clone()),
        pre_token_balances: None,
        post_token_balances: None,
        rewards: None,
        loaded_addresses: Default::default(),
        return_data: Some(meta.return_data.clone()),
        compute_units_consumed: Some(meta.compute_units_consumed),
        cost_units: None,
    }
}

fn failure_to_status_meta(
    failed: &FailedTransactionMetadata,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
) -> TransactionStatusMeta {
    let mut meta = success_to_status_meta(&failed.meta, pre_balances, post_balances);
    meta.status = Err(failed.err.clone());
    meta
}

pub fn tx_result_to_status_meta(
    result: &TransactionResult,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
) -> TransactionStatusMeta {
    match result {
        Ok(meta) => success_to_status_meta(meta, pre_balances, post_balances),
        Err(failed) => failure_to_status_meta(failed, pre_balances, post_balances),
    }
}

pub fn tx_result_to_transaction_status(
    result: &TransactionResult,
    slot: u64,
) -> TransactionStatus {
    match result {
        Ok(_) => TransactionStatus {
            slot,
            confirmations: None,
            status: Ok(()),
            err: None,
            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
        },
        Err(failed) => TransactionStatus {
            slot,
            confirmations: None,
            status: Err(failed.err.clone()),
            err: Some(failed.err.clone()),
            confirmation_status: Some(TransactionConfirmationStatus::Finalized),
        },
    }
}

pub fn tx_result_to_status_result(
    result: &TransactionResult,
) -> Result<(), TransactionError> {
    match result {
        Ok(_) => Ok(()),
        Err(failed) => Err(failed.err.clone()),
    }
}
