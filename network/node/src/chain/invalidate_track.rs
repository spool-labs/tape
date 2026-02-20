use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcError;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Signature, Signer};
use store::Store;
use tape_api::instruction::build_invalidate_track_ix;
use tape_api::program::tapedrive::{CommitteeBitmap, epoch_pda, system_pda};
use tape_crypto::Hash;

use crate::runtime::NodeContext;

pub async fn submit_invalidate_track<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    tape_address: Pubkey,
    track: Pubkey,
    bitmap: CommitteeBitmap,
    signature: tape_core::bls::BlsSignature,
    observed_root: Hash,
) -> Result<Signature, RpcError> {
    let fee_payer = context.keypair.pubkey();
    let (system_address, _) = system_pda();
    let (epoch_address, _) = epoch_pda();

    let ix = build_invalidate_track_ix(
        fee_payer,
        system_address,
        epoch_address,
        tape_address,
        track,
        bitmap,
        signature,
        observed_root,
    );

    context.rpc.send_instructions(&context.keypair, vec![ix]).await
}
