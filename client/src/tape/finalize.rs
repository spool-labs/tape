use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::instruction::tape::build_finalize_ix;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

/// Finalizes the tape with the last segment's signature.
pub async fn finalize_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
) -> Result<()> {

    let finalize_ix = build_finalize_ix(
        signer.pubkey(),
        tape_address,
        writer_address,
    );

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let finalize_tx = Transaction::new_signed_with_payer(
        &[finalize_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    send(client, &finalize_tx).await?;

    Ok(())
}

