use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::spool::build_create_ix;
use crate::utils::*;

pub async fn create_spool(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    miner_address: Pubkey,
    number: u64,
) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let create_ix = build_create_ix(signer.pubkey(), miner_address, number);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, create_ix],
        client,
        signer.pubkey(),
        &[signer],
    )
    .await
    .map_err(|e| anyhow!("Failed to create spool: {}", e))?;

    Ok(signature)
}
