use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::ADVANCE_EPOCH_CU;
use tape_api::instruction::build_advance_epoch_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_advance_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let current_epoch = ctx.state().epoch();

    let ix = build_advance_epoch_ix(fee_payer, current_epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), ADVANCE_EPOCH_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::submit_advance_epoch;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Closing)
            .next_committee_size(20)
            .next_assignment_ready()
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let next_epoch = EPOCH.saturating_add(EpochNumber(1));

        submit_advance_epoch(&ctx)
            .await
            .expect("submit advance epoch");

        let system = ctx.rpc.get_system().await.expect("fetch system");
        let current = ctx.rpc.get_epoch(EPOCH).await.expect("fetch current epoch");
        let next = ctx.rpc.get_epoch(next_epoch).await.expect("fetch next epoch");

        assert_eq!(system.current_epoch, next_epoch);
        assert_eq!(current.state.phase(), Some(EpochPhase::Completed));
        assert_eq!(next.id, next_epoch);
        assert_eq!(next.state.phase(), Some(EpochPhase::Sync));
    }

    #[tokio::test]
    async fn assignment_missing() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Closing)
            .next_committee_size(20)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        let outcome = classify_tx(submit_advance_epoch(&ctx).await);
        assert!(matches!(
            outcome,
            TxOutcome::Program(TapeError::SpoolsNotSettled)
        ));
    }
}
