use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::Backoff;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_pool;
use crate::core::chain_tx::{TxOutcome, backoff_or_cancel, classify_tx};
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
    info!(epoch = epoch.0, "advance_pool: submitting");

    let mut backoff = Backoff::new(config.tx_retry);

    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::AdvancePool, epoch);
        }

        let result = submit_advance_pool(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_pool: confirmed");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(TapeError::AlreadyAdvanced) => {
                info!(epoch = epoch.0, "advance_pool: already advanced");
                return TaskDone::Done(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(err @ (TapeError::NoRewards | TapeError::RewardsOverflow)) => {
                warn!(epoch = epoch.0, ?err, "advance_pool: rejected");
                return TaskDone::Rejected(Action::AdvancePool, epoch);
            }
            TxOutcome::Program(TapeError::BadEpochState) => {
                debug!(epoch = epoch.0, "advance_pool: bad epoch state, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::AdvancePool, epoch);
                }
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "advance_pool: unexpected program error");
                return TaskDone::Rejected(Action::AdvancePool, epoch);
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "advance_pool: transport error, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::AdvancePool, epoch);
                }
            }
        }
    }
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
