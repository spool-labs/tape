use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use store::Store;
use tape_api::compute::INIT_SNAPSHOT_EPOCH_CU;
use tape_api::instruction::build_init_snapshot_epoch_ix;
use tape_core::types::EpochNumber;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_init_snapshot_epoch<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_epoch: EpochNumber,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(INIT_SNAPSHOT_EPOCH_CU);
    let ix = build_init_snapshot_epoch_ix(fee_payer, snapshot_epoch);

    ctx.rpc
        .send_instructions(ctx.signer(), vec![cu_ix, ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_api::prelude::tapedrive;
    use tape_api::program::tapedrive::snapshot_state_pda;
    use tape_api::state::SnapshotState;
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
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let (snapshot_state_address, _) = snapshot_state_pda();
        ctx.rpc
            .rpc()
            .set_account_data(
                snapshot_state_address,
                tapedrive::ID,
                &SnapshotState {
                    tail_epoch: EpochNumber(1),
                }
                .pack(),
            )
            .expect("store snapshot state");

        submit_init_snapshot_epoch(&ctx, SNAPSHOT_EPOCH)
            .await
            .expect("submit init snapshot epoch");

        let snapshot_state = ctx
            .rpc
            .get_snapshot_state()
            .await
            .expect("fetch snapshot state");

        assert_eq!(snapshot_state.tail_epoch, EpochNumber(1));
    }

    #[tokio::test]
    async fn rejects_closed_epoch() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
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
