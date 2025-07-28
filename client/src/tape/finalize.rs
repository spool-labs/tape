use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer},
    pubkey::Pubkey,
};
use tape_api::prelude::*;
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

    build_send_and_confirm_tx(
        &[finalize_ix],
        client,
        signer.pubkey(),
        &[signer]
    ).await?;

    Ok(())
}

