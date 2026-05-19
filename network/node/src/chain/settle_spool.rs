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

#[cfg(test)]
mod tests {
    use tape_core::spooler::GroupIndex;
    use tape_core::system::EpochPhase;
    use tape_core::types::{BitmapRead, EpochNumber, SpoolIndex};

    use super::submit_settle_spool;
    use crate::harness::{NodeHarness, TestContext};

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Settle)
            .prev_committee_size(20)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let previous_epoch = EPOCH.saturating_sub(EpochNumber(1));
        let (group, position, spool) = previous_spool_for_node(&ctx);

        submit_settle_spool(&ctx, spool)
            .await
            .expect("submit settle spool");

        let group_account = ctx
            .rpc
            .get_group(previous_epoch, group)
            .await
            .expect("fetch previous group");
        let node = ctx
            .rpc
            .get_node(&ctx.pubkey().address())
            .await
            .expect("fetch node");

        assert!(group_account.settled.is_set(position));
        assert_eq!(node.pool.pending_settled, 1);
    }

    fn previous_spool_for_node(ctx: &TestContext) -> (GroupIndex, usize, SpoolIndex) {
        let state = ctx.state();
        let previous = state.previous.as_ref().expect("previous epoch");
        let node = ctx.node_address();

        previous
            .groups
            .iter()
            .find_map(|group| {
                group
                    .spools
                    .iter()
                    .position(|spool| spool.node == node)
                    .map(|position| (group.id, position, group.id.spool_at(position)))
            })
            .expect("previous spool for node")
    }
}
