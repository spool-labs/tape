use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer, Signature},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

use super::TapeHeader;

/// Creates a new tape and returns the tape address, writer address, and initial signature.
pub async fn create_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    name: &str,
    header: TapeHeader,
) -> Result<(Pubkey, Pubkey, Signature)> {

    let header_data = header.to_bytes().try_into()
        .map_err(|_| anyhow::anyhow!("Failed to convert header to bytes"))?;

    let (tape_address, _tape_bump) = tape_pda(signer.pubkey(), &to_name(name));
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    let create_ix = build_create_ix(
        signer.pubkey(), 
        name, 
        Some(header_data)
    );

    let blockhash_bytes = get_latest_blockhash(client).await?;
    let recent_blockhash = deserialize(&blockhash_bytes)?;
    let create_tx = Transaction::new_signed_with_payer(
        &[create_ix],
        Some(&signer.pubkey()),
        &[signer],
        recent_blockhash,
    );

    let signature = send_and_confirm(client, &create_tx).await?;

    Ok((tape_address, writer_address, signature))
}

