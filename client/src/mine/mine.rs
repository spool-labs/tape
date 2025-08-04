use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use crankx::Solution;
use tape_api::prelude::*;
use tape_api::instruction::miner::build_mine_ix;
use crate::utils::*;

pub async fn perform_mining(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    miner_address: Pubkey,
    tape_address: Pubkey,
    solution: Solution,
    recall_segment: [u8; SEGMENT_SIZE],
    merkle_proof: [[u8; 32]; SEGMENT_TREE_HEIGHT],
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(700_000);
    let mine_ix = build_mine_ix(
        signer.pubkey(),
        miner_address,
        tape_address,
        solution,
        recall_segment,
        merkle_proof,
    );

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, mine_ix],
        client,
        signer.pubkey(),
        &[signer]
    )
    .await
    .map_err(|e| anyhow!("Failed to mine: {}", e))?;

    Ok(signature)
}


