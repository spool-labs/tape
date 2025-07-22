use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::prelude::*;
use crate::utils::*;

pub async fn claim_rewards(
    client: &RpcClient,
    signer: &Keypair,
    miner: Pubkey,
    beneficiary: Pubkey,
    amount: u64,
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let claim_ix = build_claim_ix(signer.pubkey(), miner, beneficiary, amount);

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let tx = Transaction::new_signed_with_payer(
        &[compute_budget_ix, claim_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    let signature_bytes = send_and_confirm_transaction(client, &tx)
        .await
        .map_err(|e| anyhow!("Failed to claim rewards: {}", e))?;

    let signature: Signature = deserialize(&signature_bytes)?;

    Ok(signature)
}
