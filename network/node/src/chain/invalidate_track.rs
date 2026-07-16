use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_core::bls::BlsSignature;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::SpoolBitmap;
use tape_crypto::Hash;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_invalidate_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    track: CompressedTrackProof,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
    observed_root: Hash,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let epoch = ctx.state().epoch();

    let ix = build_invalidate_track_ix(
        fee_payer,
        track,
        epoch,
        bitmap,
        signature,
        observed_root,
    );

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), INVALIDATE_TRACK_CU, vec![ix])
        .await
}
