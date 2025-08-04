use std::sync::Arc;

use anyhow::Result;
use solana_sdk::{
    signature::{Keypair, Signer, Signature},
    pubkey::Pubkey,
};
use tape_api::prelude::*;
use tape_api::instruction::tape::build_create_ix;
use solana_client::nonblocking::rpc_client::RpcClient;
use crate::utils::*;

/// Creates a new tape and returns the tape address, writer address, and initial signature.
pub async fn create_tape(
    client: &Arc<RpcClient>,
    signer: &Keypair,
    name: &str,
) -> Result<(Pubkey, Pubkey, Signature)> {

    let (tape_address, _tape_bump) = tape_find_pda(signer.pubkey(), &to_name(name));
    let (writer_address, _writer_bump) = writer_find_pda(tape_address);

    let create_ix = build_create_ix(
        signer.pubkey(), 
        name, 
    );

    let signature = build_send_and_confirm_tx(
        &[create_ix],
        client,
        signer.pubkey(),
        &[signer]
    ).await?;

    Ok((tape_address, writer_address, signature))
}

