use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::WRITE_SNAPSHOT_CU;
use tape_api::instruction::build_write_snapshot_ix;
use tape_core::bls::BlsSignature;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkNumber, EpochNumber, SpoolGroupBitmap};
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_write_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
    group: SpoolGroup,
    chunk: ChunkNumber,
    bitmap: SpoolGroupBitmap,
    signature: BlsSignature,
    blob: &BlobInfo,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let ix = build_write_snapshot_ix(
        fee_payer,
        snapshot_epoch,
        group,
        chunk,
        bitmap,
        signature,
        blob,
    );

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), WRITE_SNAPSHOT_CU, vec![ix])
        .await
}
