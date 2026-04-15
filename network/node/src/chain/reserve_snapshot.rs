use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::RESERVE_SNAPSHOT_CU;
use tape_api::instruction::build_reserve_snapshot_ix;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_reserve_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let ix = build_reserve_snapshot_ix(fee_payer, snapshot_epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), RESERVE_SNAPSHOT_CU, vec![ix])
        .await
}
