use std::sync::Arc;

use anyhow::{anyhow, Result};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
};
use solana_client::nonblocking::rpc_client::RpcClient;

use tape_api::instruction::spool::build_unpack_ix;
use tape_api::consts::TAPE_PROOF_LEN;
use crate::utils::*;

pub async fn unpack_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    spool_address: Pubkey,
    index: u64,
    proof: [[u8; 32]; TAPE_PROOF_LEN],
    value: [u8; 32],
) -> Result<Signature> {
    let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(200_000);
    let unpack_ix = build_unpack_ix(signer.pubkey(), spool_address, index, proof, value);

    let signature = build_send_and_confirm_tx(
        &[compute_budget_ix, unpack_ix],
        client,
        signer.pubkey(),
        &[signer],
    )
    .await
    .map_err(|e| anyhow!("Failed to unpack tape: {}", e))?;

    Ok(signature)
}
