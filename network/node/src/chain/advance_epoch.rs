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
    let authority = ctx.pubkey().into();
    let ix = build_advance_epoch_ix(fee_payer, authority);

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
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(20)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        submit_advance_epoch(&ctx)
            .await
            .expect("submit advance epoch");

        let epoch = ctx.rpc.get_epoch().await.expect("fetch epoch");
        assert_eq!(epoch.id, EPOCH + EpochNumber(1));
        assert_eq!(EpochPhase::try_from(epoch.state.phase), Ok(EpochPhase::Syncing));
    }

    #[tokio::test]
    async fn insufficient_committee() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(1)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        let outcome = classify_tx(submit_advance_epoch(&ctx).await);
        assert!(matches!(
            outcome,
            TxOutcome::Program(TapeError::InsufficientCommittee)
        ));
    }
}
