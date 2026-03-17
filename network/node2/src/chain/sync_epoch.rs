use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::SYNC_EPOCH_CU;
use tape_api::instruction::build_epoch_sync_ix;
use tape_core::types::EpochNumber;
use tape_protocol::Api;

use crate::core::context::NodeContext;

pub async fn submit_sync_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    owned_spools: &[u16],
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();
    let authority = ctx.pubkey();
    let node_address = ctx.node_address();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(SYNC_EPOCH_CU);

    let ix = build_epoch_sync_ix(
        fee_payer,
        authority,
        node_address,
        epoch,
        owned_spools,
    );

    ctx.rpc
        .send_instructions(
            ctx.signer(),
            vec![cu_ix, ix],
        )
        .await
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_sync_epoch;
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
        let owned_spools = harness.owned_spools(NODE);

        submit_sync_epoch(&ctx, EPOCH, &owned_spools)
            .await
            .expect("submit sync epoch");

        let node = ctx.rpc.get_node(&ctx.pubkey()).await.expect("fetch node");
        assert_eq!(node.latest_sync_epoch, EPOCH);
    }
}
