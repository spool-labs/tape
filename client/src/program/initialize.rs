use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::prelude::*;
use crate::utils::*;

pub async fn initialize(client: &Arc<RpcClient>, signer: &Keypair) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(250_000);
    let create_ix = build_initialize_ix(signer.pubkey());

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix,create_ix],
        client,
        signer.pubkey(),
        &[signer]
    )
    .await
    .map_err(|e| anyhow!("Failed to initialize program: {}", e))?;

    Ok(signature)
}
