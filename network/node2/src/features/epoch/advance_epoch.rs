use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::Backoff;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_advance_epoch;
use crate::core::chain_tx::{TxOutcome, backoff_or_cancel, classify_tx};
use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::features::epoch::types::{Action, TaskDone};

// Purpose: Submit an AdvanceEpoch transaction to advance the network
//          to the next epoch. Any committee member can submit this.
//
// The on-chain program checks:
//   - Phase is Active
//   - now >= last_epoch + EPOCH_DURATION (enough time has passed)
//   - committee_next has enough members (>= MIN_COMMITTEE_SIZE)
//
// Algorithm:
// 1. Loop (with backoff, checking cancel):
//    a. Check cancel token.
//    b. Submit AdvanceEpoch transaction via rpc.send_instructions:
//       - build_advance_epoch_ix(fee_payer, authority)
//       - Wrap with compute budget instruction.
//    c. On success → return Done.
//       The epoch has advanced. The EpochManager will observe the
//       AdvanceEpoch instruction in the block stream, fetch new
//       protocol state, and publish it via state_rx. The lifecycle
//       worker will then see the new epoch and reset.
//    d. On TooSoon → not enough time has elapsed.
//       Sleep for a portion of the remaining time, then retry.
//    e. On InsufficientCommittee → not enough nodes have joined.
//       Retry with backoff. Other nodes may still be joining.
//    f. On SnapshotIncomplete → snapshot not yet finalized.
//       Retry with backoff.
//    g. On BadEpochState → phase is not Active.
//       Retry with backoff. This shouldn't happen if the lifecycle
//       decision function is working correctly, but it's recoverable.
//    h. On BadSchedule → return Rejected. This is a permanent error
//       indicating an on-chain bug or misconfiguration. The lifecycle
//       worker will re-evaluate and respawn (since we never give up),
//       but it will keep hitting this until the epoch advances via
//       another node.
//    i. On retriable errors → backoff and retry.
//
// Note: Multiple nodes may attempt AdvanceEpoch simultaneously.
// Only the first one to land will succeed. The others will observe
// the epoch advance via the block stream and the lifecycle worker
// will reset. This is fine — the on-chain check is idempotent in
// the sense that a stale AdvanceEpoch attempt simply fails.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    info!(epoch = epoch.0, "advance_epoch: submitting");

    let mut backoff = Backoff::new(config.tx_retry);

    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::AdvanceEpoch, epoch);
        }

        let result = submit_advance_epoch(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "advance_epoch: confirmed");
                return TaskDone::Done(Action::AdvanceEpoch, epoch);
            }
            TxOutcome::Program(
                err @ (TapeError::TooSoon
                | TapeError::InsufficientCommittee
                | TapeError::SnapshotIncomplete
                | TapeError::BadEpochState),
            ) => {
                debug!(epoch = epoch.0, ?err, "advance_epoch: not ready, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::AdvanceEpoch, epoch);
                }
            }
            TxOutcome::Program(TapeError::BadSchedule) => {
                warn!(epoch = epoch.0, "advance_epoch: bad schedule, rejected");
                return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "advance_epoch: unexpected program error");
                return TaskDone::Rejected(Action::AdvanceEpoch, epoch);
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "advance_epoch: transport error, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::AdvanceEpoch, epoch);
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
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(20)
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

        assert!(matches!(result, TaskDone::Done(Action::AdvanceEpoch, _)));
    }

    #[tokio::test]
    async fn too_soon() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .last_epoch(0)
            .next_committee_size(20)
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

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvanceEpoch, _)));
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

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_clone.cancel();
        });

        let result = run(harness.ctx_for(NODE), fast_retry_config(), EPOCH, cancel).await;

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvanceEpoch, _)));
    }

    #[tokio::test]
    async fn wrong_phase() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Settling)
            .onchain_time_elapsed()
            .next_committee_size(20)
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

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvanceEpoch, _)));
    }

    #[tokio::test]
    async fn immediate_cancel() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .onchain_time_elapsed()
            .next_committee_size(20)
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

        assert!(matches!(result, TaskDone::Cancelled(Action::AdvanceEpoch, _)));
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
