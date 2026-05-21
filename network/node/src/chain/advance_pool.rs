use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::ADVANCE_POOL_CU;
use tape_api::instruction::build_advance_pool_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_advance_pool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let ix = build_advance_pool_ix(fee_payer, ctx.node_address(), ctx.state().epoch());

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), ADVANCE_POOL_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_advance_pool;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Snapshot)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let prev_epoch = EPOCH.saturating_sub(EpochNumber(1));

        submit_advance_pool(&ctx).await.expect("submit advance pool");

        let authority = ctx.pubkey().address();
        let node = ctx.rpc.get_node(&authority).await.expect("fetch node");
        let history = ctx
            .rpc
            .get_history(&harness.node(NODE).node_address.into())
            .await
            .expect("fetch history");

        assert_eq!(node.latest_advance_epoch, prev_epoch);
        assert_eq!(history.latest_epoch, prev_epoch);
    }
}
