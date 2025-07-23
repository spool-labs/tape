use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signer, Signature},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

pub async fn subsidize_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    tape_address: Pubkey,
    ata: Pubkey,
    amount: u64,
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let subsidize_ix = build_subsidize_ix(
        signer.pubkey(),
        ata,
        tape_address,
        amount,
    );

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let tx = Transaction::new_signed_with_payer(
        &[compute_budget_ix, subsidize_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    let signature_bytes = send_and_confirm_transaction(client, &tx)
        .await
        .map_err(|e| anyhow!("Failed to subsidize tape: {}", e))?;

    let signature: Signature = deserialize(&signature_bytes)?;

    Ok(signature)
}
