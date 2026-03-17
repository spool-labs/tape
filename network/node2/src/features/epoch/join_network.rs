use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tokio_util::sync::CancellationToken;

use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit a JoinNetwork transaction to volunteer for the next
//          epoch's committee. This signals the node intends to serve
//          in the upcoming epoch.
//
// Timing gate: JoinNetwork should not be submitted until 90% of the
// epoch duration has elapsed. This prevents committing to a potentially
// week-long epoch too early, reducing the risk of being unavailable
// at the epoch transition point. The on-chain program will enforce
// this constraint in the future.
//
// Algorithm:
// 1. Wait for timing gate:
//    a. Compute threshold = last_epoch + (EPOCH_DURATION * 90 / 100).
//    b. Loop: check cancel, compute now vs threshold.
//       If now < threshold → sleep for min(remaining, poll_interval).
//       If now >= threshold → proceed.
//    Note: last_epoch and EPOCH_DURATION come from protocol state.
//    EPOCH_DURATION is a compile-time constant from tape_api.
//    last_epoch needs to be in ProtocolState (requires adding it
//    to fetch_state).
//
// 2. Submit loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit JoinNetwork transaction via rpc.send_instructions:
//       - build_join_network_ix(fee_payer, authority, node_address)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//    d. On UnexpectedState → the node might already be in committee_next.
//       Query the on-chain state to check:
//       - If already in committee_next → return Done.
//       - If not → retry with backoff.
//    e. On NodeStale / NotStaked → return Rejected.
//       These indicate the node's on-chain state is not suitable for
//       joining. The lifecycle worker re-evaluates.
//    f. On BadEpochState → retry with backoff.
//       Phase may not have reached the required state yet.
//    g. On retriable errors → backoff and retry.
//
// Prerequisite: AdvancePool must be done. This is enforced by the
// lifecycle decision function (next_action), not by this task.

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

    // 90% of epoch duration elapsed, node eligible → should submit and return Done.
    #[tokio::test]
    #[ignore]
    async fn success() {
        let ctx = test_context();
        // TODO: deploy program, init system/epoch, register node
        // TODO: set last_epoch timestamp such that 90% has elapsed
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::JoinNetwork, _)));
    }

    // Less than 90% elapsed → should wait at timing gate until cancelled.
    #[tokio::test]
    #[ignore]
    async fn timing_gate_not_met() {
        let ctx = test_context();
        // TODO: set last_epoch to recent timestamp (< 90% elapsed)
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            cancel_clone.cancel();
        });

        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::JoinNetwork, _)));
    }

    // Node already in committee_next → should return Done (idempotent).
    #[tokio::test]
    #[ignore]
    async fn already_joined() {
        let ctx = test_context();
        // TODO: set up on-chain state where node is already in committee_next
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Done(Action::JoinNetwork, _)));
    }

    // Node not staked → should return Rejected.
    #[tokio::test]
    #[ignore]
    async fn not_staked() {
        let ctx = test_context();
        // TODO: set up on-chain state where node has no active stake
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, CancellationToken::new()).await;
        assert!(matches!(result, TaskDone::Rejected(Action::JoinNetwork, _)));
    }

    // Immediate cancel → should return Cancelled before timing gate.
    #[tokio::test]
    #[ignore]
    async fn immediate_cancel() {
        let ctx = test_context();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = run(ctx, EpochLifecycleConfig::default(), EPOCH, cancel).await;
        assert!(matches!(result, TaskDone::Cancelled(Action::JoinNetwork, _)));
    }
}
