use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rpc::Rpc;
use store::Store;
use tape_api::errors::TapeError;
use tape_api::program::EPOCH_DURATION;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_retry::Backoff;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::submit_join_network;
use crate::core::chain_tx::{TxOutcome, backoff_or_cancel, classify_tx};
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

/// Fraction of EPOCH_DURATION that must elapse before joining (90%).
const JOIN_THRESHOLD_NUM: i64 = 90;
const JOIN_THRESHOLD_DEN: i64 = 100;

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    // Wait for 90% of epoch duration to elapse.
    let last_epoch = ctx.state().last_epoch;
    let threshold = last_epoch + (EPOCH_DURATION * JOIN_THRESHOLD_NUM / JOIN_THRESHOLD_DEN);

    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::JoinNetwork, epoch);
        }

        let now = unix_now();
        if now >= threshold {
            break;
        }

        let remaining = (threshold - now).max(0) as u64;
        let sleep_dur = std::time::Duration::from_secs(remaining.min(
            config.time_poll_interval.as_secs().max(1),
        ));

        debug!(
            epoch = epoch.0,
            remaining_secs = remaining,
            "join_network: waiting for timing gate"
        );

        tokio::select! {
            _ = cancel.cancelled() => {
                return TaskDone::Cancelled(Action::JoinNetwork, epoch);
            }
            _ = tokio::time::sleep(sleep_dur) => {}
        }
    }

    info!(epoch = epoch.0, "join_network: timing gate passed, submitting");

    // Submit loop with retry.
    let mut backoff = Backoff::new(config.tx_retry);

    loop {
        if cancel.is_cancelled() {
            return TaskDone::Cancelled(Action::JoinNetwork, epoch);
        }

        let result = submit_join_network(&ctx).await;

        match classify_tx(result) {
            TxOutcome::Confirmed(sig) => {
                info!(epoch = epoch.0, %sig, "join_network: confirmed");
                return TaskDone::Done(Action::JoinNetwork, epoch);
            }
            TxOutcome::Program(TapeError::UnexpectedState) => {
                let state = ctx.state();
                if state.find_member_next(ctx.node_id()).is_some() {
                    info!(epoch = epoch.0, "join_network: already in committee_next");
                    return TaskDone::Done(Action::JoinNetwork, epoch);
                }
                debug!(epoch = epoch.0, "join_network: unexpected state, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::JoinNetwork, epoch);
                }
            }
            TxOutcome::Program(
                err @ (TapeError::NodeStale | TapeError::NotStaked),
            ) => {
                warn!(epoch = epoch.0, ?err, "join_network: rejected");
                return TaskDone::Rejected(Action::JoinNetwork, epoch);
            }
            TxOutcome::Program(TapeError::BadEpochState) => {
                debug!(epoch = epoch.0, "join_network: bad epoch state, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::JoinNetwork, epoch);
                }
            }
            TxOutcome::Program(err) => {
                warn!(epoch = epoch.0, ?err, "join_network: unexpected program error");
                return TaskDone::Rejected(Action::JoinNetwork, epoch);
            }
            TxOutcome::Transport(err) => {
                debug!(epoch = epoch.0, %err, "join_network: transport error, retrying");
                if backoff_or_cancel(&mut backoff, &cancel).await {
                    return TaskDone::Cancelled(Action::JoinNetwork, epoch);
                }
            }
        }
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use tape_core::types::coin::TAPE;
    use tape_core::system::EpochPhase;

    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .time_elapsed()
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

        assert!(matches!(result, TaskDone::Done(Action::JoinNetwork, _)));
    }

    #[tokio::test]
    async fn gate_wait() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .last_epoch(unix_now())
            .build()
            .await
            .expect("build harness");

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel_clone.cancel();
        });

        let result = run(
            harness.ctx_for(NODE),
            EpochLifecycleConfig::default(),
            EPOCH,
            cancel,
        )
        .await;

        assert!(matches!(result, TaskDone::Cancelled(Action::JoinNetwork, _)));
    }

    #[tokio::test]
    async fn already_joined() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .time_elapsed()
            .next_committee_nodes([NODE])
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

        assert!(matches!(result, TaskDone::Done(Action::JoinNetwork, _)));
    }

    #[tokio::test]
    async fn not_staked() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .time_elapsed()
            .node(NODE, |node| node.stake = TAPE(0))
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

        assert!(matches!(result, TaskDone::Rejected(Action::JoinNetwork, _)));
    }

    #[tokio::test]
    async fn immediate_cancel() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .last_epoch(unix_now())
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

        assert!(matches!(result, TaskDone::Cancelled(Action::JoinNetwork, _)));
    }
}
