use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use store::Store;
use tape_api::compute::FINALIZE_SNAPSHOT_EPOCH_CU;
use tape_api::instruction::build_finalize_snapshot_epoch_ix;
use tape_core::types::EpochNumber;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_finalize_snapshot_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
) -> Result<Signature, RpcError> {
    let fee_payer = ctx.pubkey();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(FINALIZE_SNAPSHOT_EPOCH_CU);
    let ix = build_finalize_snapshot_epoch_ix(fee_payer, snapshot_epoch);

    ctx.rpc
        .send_instructions(ctx.signer(), vec![cu_ix, ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_finalize_snapshot_epoch;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
    use crate::harness::NodeHarness;
    use crate::chain::submit_init_snapshot_epoch;

    const EPOCH: EpochNumber = EpochNumber(3);
    const SNAPSHOT_EPOCH: EpochNumber = EpochNumber(2);
    const NODE: usize = 7;

    #[tokio::test]
    async fn rejects_incomplete_manifest() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        submit_init_snapshot_epoch(&ctx, SNAPSHOT_EPOCH)
            .await
            .expect("init snapshot epoch");

        let outcome = classify_tx(submit_finalize_snapshot_epoch(&ctx, SNAPSHOT_EPOCH).await);
        assert!(matches!(
            outcome,
            TxOutcome::Program(TapeError::SnapshotIncomplete)
        ));
    }
}
