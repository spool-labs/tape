use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

pub async fn subsidize_tape(
    client: &RpcClient,
    signer: &Keypair,
    tape_address: Pubkey,
) -> Result<()> {

    // let subsidize_ix = build_subsidize_ix(
    //     signer.pubkey(),
    //     tape_address,
    // );
    //
    // let blockhash_bytes = get_latest_blockhash(client).await?;
    // let recent_blockhash = deserialize(&blockhash_bytes)?;
    // let finalize_tx = Transaction::new_signed_with_payer(
    //     &[finalize_ix],
    //     Some(&signer.pubkey()),
    //     &[signer],
    //     recent_blockhash,
    // );
    //
    // send(client, &finalize_tx).await?;

    Ok(())
}

