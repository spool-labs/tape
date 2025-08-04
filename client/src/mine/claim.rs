use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::miner::build_claim_ix;
use crate::utils::*;

pub async fn claim_rewards(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    miner: Pubkey,
    beneficiary: Pubkey,
    amount: u64,
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let claim_ix = build_claim_ix(signer.pubkey(), miner, beneficiary, amount);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, claim_ix],
        client,
        signer.pubkey(),
        &[signer]
    )
    .await
    .map_err(|e| anyhow!("Failed to claim rewards: {}", e))?;

    Ok(signature)
}
