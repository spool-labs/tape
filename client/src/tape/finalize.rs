use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

use super::TapeHeader;

/// Finalizes the tape with the last segment's signature.
pub async fn finalize_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
    header: TapeHeader,
) -> Result<()> {
    let header_data = header.to_bytes().try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert header to bytes"))?;

    let finalize_ix = build_finalize_ix(
        signer.pubkey(),
        tape_address,
        writer_address,
        Some(header_data),
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

