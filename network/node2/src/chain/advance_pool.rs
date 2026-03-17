use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::ADVANCE_POOL_CU;
use tape_api::instruction::build_advance_pool_ix;
use tape_protocol::Api;

use crate::core::context::NodeContext;

pub async fn submit_advance_pool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();
    let authority = ctx.pubkey();
    let node_address = ctx.node_address();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        ADVANCE_POOL_CU);

    let ix = build_advance_pool_ix(fee_payer, authority, node_address);

    ctx.rpc
        .send_instructions(
            ctx.signer(),
            vec![cu_ix, ix]
    ).await
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
            .phase(EpochPhase::Settling)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        submit_advance_pool(&ctx).await.expect("submit advance pool");

        let node = ctx.rpc.get_node(&ctx.pubkey()).await.expect("fetch node");
        let history = ctx
            .rpc
            .get_history(&harness.node(NODE).node_address)
            .await
            .expect("fetch history");

        assert_eq!(node.latest_advance_epoch, EPOCH);
        assert_eq!(history.latest_epoch, EPOCH);
    }
}
