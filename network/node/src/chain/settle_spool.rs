use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::SETTLE_SPOOL_CU;
use tape_api::instruction::build_settle_spool_ix;
use tape_core::types::SpoolIndex;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_settle_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    spool: SpoolIndex,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let pool = ctx.node_address();
    let current_epoch = ctx.state().epoch();
    let ix = build_settle_spool_ix(fee_payer, pool, current_epoch, spool);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), SETTLE_SPOOL_CU, vec![ix])
        .await
}
