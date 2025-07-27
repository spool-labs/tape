use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signer, Signature},
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
  
    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, subsidize_ix],
        client,
        signer.pubkey(),
        &[signer]
    )
    .await
    .map_err(|e| anyhow!("Failed to subsidize tape: {}", e))?;

    Ok(signature)
}
