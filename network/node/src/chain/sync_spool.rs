use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::SYNC_SPOOL_CU;
use tape_api::instruction::build_sync_spool_ix;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_sync_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    spool: SpoolIndex,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let authority = ctx.pubkey().into();
    let node_address = ctx.node_address();
    let ix = build_sync_spool_ix(
        fee_payer,
        authority,
        node_address,
        epoch,
        GroupIndex::containing(spool),
        spool,
    );

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), SYNC_SPOOL_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_sync_spool;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let spool = harness.owned_spools(NODE)[0];

        submit_sync_spool(&ctx, EPOCH, spool)
            .await
            .expect("submit sync spool");

        let authority = ctx.pubkey().address();
        let node = ctx.rpc.get_node(&authority).await.expect("fetch node");
        assert_eq!(node.latest_sync_epoch, EPOCH);
    }
}
