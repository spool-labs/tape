use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::PROPOSE_SNAPSHOT_CU;
use tape_api::instruction::build_propose_snapshot_ix;
use tape_crypto::{Hash, tx::Txid};
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_propose_snapshot<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    hash: Hash,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let current_epoch = ctx.state().epoch();
    let ix = build_propose_snapshot_ix(fee_payer, current_epoch, hash);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), PROPOSE_SNAPSHOT_CU, vec![ix])
        .await
}
