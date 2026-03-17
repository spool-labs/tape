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
    use std::time::Duration;

    use tape_core::system::EpochPhase;
    use tape_retry::RetryConfig;

    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }

    #[tokio::test]
    async fn already_advanced() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .node(NODE, |node| node.latest_advance_epoch = EPOCH)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }

    #[tokio::test]
    async fn wrong_phase_then_cancel() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Syncing)
            .build()
            .await
            .expect("build harness");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_clone.cancel();
        });

        let result = run(harness.ctx_for(NODE), fast_retry_config(), EPOCH, cancel).await;

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvancePool, _)));
    }

    #[tokio::test]
    async fn immediate_cancel() {
        let harness = NodeHarness::builder()
            .nodes(20)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .build()
            .await
            .expect("build harness");

        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            cancel,
        )
        .await;

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvancePool, _)));
    }

    #[tokio::test]
    async fn prev_committee_only() {
        let current_committee: Vec<_> = (0..=20).filter(|&index| index != NODE).collect();
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .current_committee_nodes(current_committee)
            .prev_committee_size(20)
            .build()
            .await
            .expect("build harness");

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            CancellationToken::new(),
        )
        .await;

        assert!(matches!(result, TaskDone::Done(Action::AdvancePool, _)));
    }

    fn fast_retry_config() -> EpochLifecycleConfig {
        EpochLifecycleConfig {
            tx_retry: RetryConfig {
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                max_retries: None,
            },
            ..EpochLifecycleConfig::default()
        }
    }
}
