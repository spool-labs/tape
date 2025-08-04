use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::program::build_airdrop_ix;
use crate::utils::*;

pub async fn airdrop_tokens(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    beneficiary: Pubkey,
    amount: u64,
) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let airdrop_ix = build_airdrop_ix(signer.pubkey(), beneficiary, amount);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, airdrop_ix],
        client,
        signer.pubkey(),
        &[signer],
    )
    .await
    .map_err(|e| anyhow!("Failed to airdrop tokens: {}", e))?;

    Ok(signature)
}
