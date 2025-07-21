use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::prelude::*;
use crate::utils::*;

pub async fn register_miner(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    name: &str,
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let register_ix = build_register_ix(signer.pubkey(), name);

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let tx = Transaction::new_signed_with_payer(
        &[compute_budget_ix, register_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    let signature_bytes = send_and_confirm_transaction(client, &tx)
        .await
        .map_err(|e| anyhow!("Failed to register miner: {}", e))?;

    let signature: Signature = deserialize(&signature_bytes)?;

    Ok(signature)
}

