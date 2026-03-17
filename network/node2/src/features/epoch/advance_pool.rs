use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;

use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit an AdvancePool transaction to settle rewards for this
//          node's staking pool. This is required for any node that was
//          in the current or previous committee.
//
// AdvancePool is permissionless — anyone can call it. But nodes call it
// for themselves because each node needs its pool advanced before it can
// earn rewards and before JoinNetwork can be called.
//
// Algorithm:
// 1. Loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit AdvancePool transaction via rpc.send_instructions:
//       - build_advance_pool_ix(fee_payer, authority, node_address)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//    d. On AlreadyAdvanced → return Done (idempotent).
//    e. On BadEpochState → the on-chain program rejected the call.
//       This happens if the phase hasn't reached Settling yet (we were
//       scheduled too early) or if the epoch state is somehow wrong.
//       Retry with backoff — the phase may advance soon.
//    f. On NoRewards / RewardsOverflow → return Rejected.
//       These indicate the node has no claimable rewards. The lifecycle
//       worker marks AdvancePool as done and moves on.
//    g. On retriable errors → backoff and retry.
//
// Unlike SyncEpoch, AdvancePool has no preconditions beyond being in
// the right epoch phase. No spool readiness check needed.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::context::test_utils::test_context;

    const EPOCH: EpochNumber = EpochNumber(3);

    // Settling phase, node in committee → should submit and return Done.
    #[tokio::test]
    #[ignore]
    async fn success() {
        let ctx = test_context();
        // TODO: deploy program, init system/epoch, register node, advance to Settling
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }

    // Pool already advanced → should return Done (idempotent).
    #[tokio::test]
    #[ignore]
    async fn already_advanced() {
        let ctx = test_context();
        // TODO: set up on-chain state where AdvancePool was already called
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }

    // Phase not yet Settling → should retry internally until cancelled.
    #[tokio::test]
    #[ignore]
    async fn wrong_phase_then_cancel() {
        let ctx = test_context();
        // TODO: set up on-chain state in Syncing phase (too early for AdvancePool)
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::AdvancePool, _)));
    }

    // Immediate cancel → should return Cancelled without submitting.
    #[tokio::test]
    #[ignore]
    async fn immediate_cancel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::AdvancePool, _)));
    }

    // Node was in prev committee but not current → should still submit.
    #[tokio::test]
    #[ignore]
    async fn prev_committee_only() {
        let ctx = test_context();
        // TODO: set up state where node is in committee_prev but not committee
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }
}
