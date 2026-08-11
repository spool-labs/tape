use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::COMMIT_EPOCH_CU;
use tape_api::instruction::build_commit_epoch_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_commit_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let current_epoch = ctx.state().epoch();
    let ix = build_commit_epoch_ix(fee_payer, current_epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), COMMIT_EPOCH_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_commit_epoch;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(20)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let next_epoch = EPOCH.next();

        submit_commit_epoch(&ctx).await.expect("submit commit epoch");

        let current = ctx.rpc.get_epoch(EPOCH).await.expect("fetch current epoch");
        let next = ctx.rpc.get_epoch(next_epoch).await.expect("fetch next epoch");

        assert_eq!(current.state.phase(), Some(EpochPhase::Closing));
        assert_eq!(next.id, next_epoch);
    }
}
