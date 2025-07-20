use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer, Signature},
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::{
    consts::*,
    utils::*,
};

pub async fn write_to_tape(
    client: &RpcClient,
    signer: &Keypair,
    tape_address: Pubkey,
    writer_address: Pubkey,
    data: &[u8],
) -> Result<Signature> {

    let instruction = build_write_ix(
        signer.pubkey(),
        tape_address,
        writer_address,
        data,
    );

    let sig = send_with_retry(client, &instruction, signer, MAX_RETRIES).await?;
    Ok(sig)
}

