use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::FINALIZE_SNAPSHOT_CU;
use tape_api::instruction::build_finalize_snapshot_ix;
use tape_api::state::Tape;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_finalize_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    tape: Tape,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_finalize_snapshot_ix(fee_payer, epoch, tape);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), FINALIZE_SNAPSHOT_CU, vec![ix])
        .await
}
