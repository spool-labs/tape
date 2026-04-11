use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::INIT_SNAPSHOT_EPOCH_CU;
use tape_api::instruction::build_init_snapshot_epoch_ix;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_init_snapshot_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let ix = build_init_snapshot_epoch_ix(fee_payer, epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), INIT_SNAPSHOT_EPOCH_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_init_snapshot_epoch;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const SNAPSHOT_EPOCH: EpochNumber = EpochNumber(2);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .no_prev_snapshot_manifest()
            .build()
            .await
            .expect("build harness");

        let ctx = harness.ctx_for(NODE);

        submit_init_snapshot_epoch(&ctx, SNAPSHOT_EPOCH)
            .await
            .expect("submit init snapshot epoch");

        let manifest = ctx
            .rpc
            .get_snapshot_manifest(SNAPSHOT_EPOCH)
            .await
            .expect("fetch snapshot manifest");

        assert_eq!(manifest.group_bitmap.count_ones(), 0);
    }

    #[tokio::test]
    async fn rejects_closed_epoch() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .no_prev_snapshot_manifest()
            .build()
            .await
            .expect("build harness");

        let ctx = harness.ctx_for(NODE);

        let outcome = classify_tx(submit_init_snapshot_epoch(&ctx, EPOCH).await);
        assert!(matches!(
            outcome,
            TxOutcome::Program(TapeError::SnapshotEpochClosed)
        ));
    }
}
