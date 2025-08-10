use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::spool::build_commit_ix;
use tape_api::types::*;
use crate::utils::*;

pub async fn commit_solution(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    miner_address: Pubkey,
    spool_address: Pubkey,
    index: u64,
    proof: ProofPath,
    value: [u8; 32],
) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(700_000);
    let commit_ix = build_commit_ix(signer.pubkey(), miner_address, spool_address, index, proof, value);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, commit_ix],
        client,
        signer.pubkey(),
        &[signer],
    )
    .await
    .map_err(|e| anyhow!("Failed to commit solution: {}", e))?;

    Ok(signature)
}
