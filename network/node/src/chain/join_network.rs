use std::sync::Arc;

use rpc::{Rpc, RpcError};
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use store::Store;
use tape_api::compute::JOIN_NETWORK_CU;
use tape_api::instruction::build_join_network_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_join_network<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let authority = ctx.pubkey().into();
    let node_address = ctx.node_address();

    let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        JOIN_NETWORK_CU);

    let ix = build_join_network_ix(fee_payer, authority, node_address);

    ctx.rpc
        .send_instructions(
            ctx.signer(),
            vec![cu_ix, ix]
    ).await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;
    use tape_core::types::coin::TAPE;

    use super::submit_join_network;
    use crate::core::chain_tx::{TxOutcome, classify_tx};
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

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

        submit_join_network(&ctx)
            .await
            .expect("submit join network");

        let system = ctx.rpc.get_system().await.expect("fetch system");
        assert!(system.committee_next.index_of(&harness.node(NODE).node_id).is_some());
    }

    #[tokio::test]
    async fn not_staked() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .node(NODE, |node| node.stake = TAPE(0))
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);

        let outcome = classify_tx(submit_join_network(&ctx).await);
        assert!(matches!(outcome, TxOutcome::Program(TapeError::NotStaked)));
    }
}
