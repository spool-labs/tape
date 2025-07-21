use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer, Signature},
    transaction::Transaction,
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

/// Creates a new tape and returns the tape address, writer address, and initial signature.
pub async fn create_tape(
    client: &RpcClient,
    signer: &Keypair,
    name: &str,
) -> Result<(Pubkey, Pubkey, Signature)> {

    let (tape_address, _tape_bump) = tape_pda(signer.pubkey(), &to_name(name));
    let (writer_address, _writer_bump) = writer_pda(tape_address);

    let create_ix = build_create_ix(
        signer.pubkey(), 
        name, 
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

