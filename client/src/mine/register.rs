use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::prelude::*;
use crate::utils::*;

pub async fn register_miner(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    name: &str,
) -> Result<Signature> {

    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(50_000);
    let register_ix = build_register_ix(signer.pubkey(), name);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, register_ix],
        client,
        signer.pubkey(),
        &[signer]
    )
    .await
    .map_err(|e| anyhow!("Failed to register miner: {}", e))?;

    Ok(signature)
}

