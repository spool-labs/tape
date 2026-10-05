use std::sync::Arc;

use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_api::compute::INVALIDATE_TRACK_CU;
use tape_api::instruction::build_invalidate_track_ix;
use tape_core::bls::BlsSignature;
use tape_core::track::types::CompressedTrackProof;
use tape_core::types::SpoolBitmap;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_invalidate_track<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    track: CompressedTrackProof,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let state = ctx.state();
    let epoch = state.epoch();
    let group = state
        .live_group(track.state.group)
        .ok_or_else(|| RpcError::Request("no spool group is live".into()))?;

    let ix = build_invalidate_track_ix(
        fee_payer,
        track,
        epoch,
        group,
        bitmap,
        signature,
    );

    ctx.rpc
        .simulate_then_send_with_compute_unit_limit(ctx.signer(), INVALIDATE_TRACK_CU, vec![ix])
        .await
}
