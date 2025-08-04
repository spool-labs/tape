use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::spool::build_destroy_ix;
use crate::utils::*;

pub async fn destroy_spool(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    miner_address: Pubkey,
    number: u64,
) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let destroy_ix = build_destroy_ix(signer.pubkey(), miner_address, number);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, destroy_ix],
        client,
        signer.pubkey(),
        &[signer],
    )
    .await
    .map_err(|e| anyhow!("Failed to destroy spool: {}", e))?;

    Ok(signature)
}
