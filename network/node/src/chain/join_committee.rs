use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::JOIN_COMMITTEE_CU;
use tape_api::instruction::build_join_committee_ix;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_join_committee<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let authority = ctx.pubkey().into();
    let node_address = ctx.node_address();
    let current_epoch = ctx.state().epoch();
    let ix = build_join_committee_ix(fee_payer, authority, node_address, current_epoch);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), JOIN_COMMITTEE_CU, vec![ix])
        .await
}

#[cfg(test)]
mod tests {
    use tape_api::errors::TapeError;
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;
    use tape_core::types::coin::TAPE;

    use super::submit_join_committee;
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

        submit_join_committee(&ctx)
            .await
            .expect("submit join committee");

        let next_epoch = EPOCH.next();
        let committee = ctx
            .rpc
            .get_committee(next_epoch)
            .await
            .expect("fetch next committee");
        let peers = ctx.rpc.get_peer_set().await.expect("fetch peer set");
        let node = ctx.node_address();

        assert!(committee.iter().any(|member| member.node == node));
        assert!(peers.iter().any(|peer| peer.node == node));
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

        let outcome = classify_tx(submit_join_committee(&ctx).await);
        assert!(matches!(outcome, TxOutcome::Program(TapeError::NotStaked)));
    }
}
