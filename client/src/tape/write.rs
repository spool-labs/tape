use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer, Signature},
    pubkey::Pubkey,
};
use tape_api::instruction::tape::build_write_ix;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

pub async fn write_to_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
    data: &[u8],
    max_transaction_retries: u32
) -> Result<Signature> {

    let instruction = build_write_ix(
        signer.pubkey(),
        tape_address,
        writer_address,
        data,
    );

    let sig = send_with_retry(client, &instruction, signer, max_transaction_retries).await?;
    Ok(sig)
}
