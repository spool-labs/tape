use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use crankx::Solution;
use tape_api::prelude::*;
use crate::utils::*;

pub async fn perform_mining(
    client: &RpcClient,
    signer: &Keypair,
    miner_address: Pubkey,
    tape_address: Pubkey,
    solution: Solution,
    recall_segment: [u8; SEGMENT_SIZE],
    merkle_proof: [[u8; 32]; TREE_HEIGHT],
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

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let tx = Transaction::new_signed_with_payer(
        &[compute_budget_ix, mine_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    let signature_bytes = send_and_confirm_transaction(client, &tx)
        .await
        .map_err(|e| anyhow!("Failed to mine: {}", e))?;

    let signature: Signature = deserialize(&signature_bytes)?;

    Ok(signature)
}


