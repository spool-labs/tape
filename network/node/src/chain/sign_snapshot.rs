use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::SIGN_SNAPSHOT_CU;
use tape_api::instruction::build_sign_snapshot_ix;
use tape_core::bls::BlsSignature;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolBitmap};
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_sign_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
    group: GroupIndex,
    bitmap: SpoolBitmap,
    signature: BlsSignature,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let ix = build_sign_snapshot_ix(fee_payer, snapshot_epoch, group, bitmap, signature);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), SIGN_SNAPSHOT_CU, vec![ix])
        .await
}
