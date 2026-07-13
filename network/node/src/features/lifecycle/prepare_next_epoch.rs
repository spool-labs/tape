use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::program::MIN_COMMITTEE_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_protocol::{Api, ProtocolState};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::{
    submit_create_committee, submit_create_epoch, submit_resize_committee, submit_resize_peer_set,
};
use crate::context::NodeContext;
use crate::core::chain_tx::{
    stagger_by_rank, submit_if_at_tip, wait_for_state_change, TxOutcome, TxRejectionKind,
};
use crate::features::lifecycle::manager::committee_rank;
use crate::features::lifecycle::types::{Action, TaskDone};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SetupStep {
    CreateEpoch,
    CreateCommittee,
    ResizeCommittee,
    ResizePeerSet,
    Done,
    InvalidCommitteeCapacity { capacity: u64, target: u64 },
}

// Purpose: while Epoch(N) is closing, ensure Epoch(N+2) and Committee(N+2)
// exist and are allocated before AdvanceEpoch enters Epoch(N+1).

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    let next_epoch = epoch.next();
    let candidate_epoch = epoch.saturating_add(EpochNumber(2));
    let node = ctx.node_address();
    let rank = committee_rank(&ctx.state(), node);
    let mut state_rx = ctx.subscribe_state();

    loop {
        let state = ctx.state();
        if state.epoch() != epoch {
            info!(
                epoch = epoch.0,
                local_epoch = state.epoch().0,
                "prepare_next_epoch: wrong epoch"
            );
            return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
        }

        match state.phase() {
            EpochPhase::Closing => {}
            EpochPhase::Completed => {
                info!(
                    epoch = epoch.0,
                    phase = ?state.phase(),
                    "prepare_next_epoch: setup window has passed"
                );
                return TaskDone::Done(Action::PrepareNextEpoch, epoch);
            }
            phase => {
                info!(epoch = epoch.0, ?phase, "prepare_next_epoch: outside closing phase");
                return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
            }
        }

        let step = next_setup_step(&state, next_epoch, candidate_epoch);
        drop(state);

        match step {
            SetupStep::Done => {
                info!(
                    epoch = epoch.0,
                    candidate_epoch = candidate_epoch.0,
                    "prepare_next_epoch: setup complete"
                );
                return TaskDone::Done(Action::PrepareNextEpoch, epoch);
            }
            SetupStep::InvalidCommitteeCapacity { capacity, target } => {
                warn!(
                    epoch = epoch.0,
                    candidate_epoch = candidate_epoch.0,
                    capacity,
                    target,
                    "prepare_next_epoch: invalid committee capacity"
                );
                return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
            }
            step => {
                // Stagger by rank so lower ranks submit first. Re-staggered per
                // step because each setup account is a separate race; after the
                // delay, re-check in case a lower rank already advanced this step.
                if stagger_by_rank(rank, &cancel).await {
                    break;
                }
                if next_setup_step(&ctx.state(), next_epoch, candidate_epoch) != step {
                    continue;
                }
                if submit_setup_step(&ctx, epoch, candidate_epoch, step).await {
                    return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
                }
            }
        }

        // The setup preconditions (accounts existing, capacity) only flip when a
        // new block lands, so wait for a state change rather than a clock backoff.
        if wait_for_state_change(&mut state_rx, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::PrepareNextEpoch, epoch)
}

fn next_setup_step(
    state: &ProtocolState,
    next_epoch: EpochNumber,
    candidate_epoch: EpochNumber,
) -> SetupStep {
    let committee_target = state
        .next_epoch
        .as_ref()
        .filter(|epoch| epoch.id == next_epoch)
        .map(|epoch| epoch.preferences.committee_size)
        .unwrap_or(0);

    if committee_target < MIN_COMMITTEE_SIZE as u64 {
        return SetupStep::InvalidCommitteeCapacity {
            capacity: committee_target,
            target: MIN_COMMITTEE_SIZE as u64,
        };
    }

    let candidate_epoch_ready = state
        .candidate_epoch
        .as_ref()
        .is_some_and(|epoch| epoch.id == candidate_epoch);
    if !candidate_epoch_ready {
        return SetupStep::CreateEpoch;
    }

    decide_setup_step(
        state.system.committee_size,
        committee_target,
        state.candidate_committee_capacity,
        state.peer_capacity,
    )
}

fn decide_setup_step(
    current_committee_capacity: u64,
    committee_target: u64,
    committee_capacity: Option<u64>,
    peer_capacity: u64,
) -> SetupStep {
    let Some(committee_capacity) = committee_capacity else {
        return SetupStep::CreateCommittee;
    };

    if committee_capacity < committee_target {
        return SetupStep::ResizeCommittee;
    }

    if committee_capacity > committee_target {
        return SetupStep::InvalidCommitteeCapacity {
            capacity: committee_capacity,
            target: committee_target,
        };
    }

    let peer_target = current_committee_capacity
        .max(committee_target)
        .saturating_mul(3);

    if peer_capacity < peer_target {
        return SetupStep::ResizePeerSet;
    }

    SetupStep::Done
}

async fn submit_setup_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    candidate_epoch: EpochNumber,
    step: SetupStep,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    match step {
        SetupStep::CreateEpoch => submit_create_epoch_step(ctx, epoch, candidate_epoch).await,
        SetupStep::CreateCommittee => submit_create_committee_step(ctx, epoch, candidate_epoch).await,
        SetupStep::ResizeCommittee => submit_resize_committee_step(ctx, epoch, candidate_epoch).await,
        SetupStep::ResizePeerSet => submit_resize_peer_set_step(ctx, epoch).await,
        SetupStep::Done | SetupStep::InvalidCommitteeCapacity { .. } => false,
    }
}

async fn submit_create_epoch_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    candidate_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        candidate_epoch = candidate_epoch.0,
        "prepare_next_epoch: creating epoch account"
    );
    let outcome = submit_if_at_tip(
        &ctx.ingest,
        "create_epoch",
        submit_create_epoch(ctx, candidate_epoch),
    )
    .await;
    log_setup_outcome("create_epoch", epoch, candidate_epoch, outcome)
}

async fn submit_create_committee_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    candidate_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        candidate_epoch = candidate_epoch.0,
        "prepare_next_epoch: creating committee account"
    );
    let outcome = submit_if_at_tip(
        &ctx.ingest,
        "create_committee",
        submit_create_committee(ctx, candidate_epoch),
    )
    .await;
    log_setup_outcome("create_committee", epoch, candidate_epoch, outcome)
}

async fn submit_resize_committee_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    candidate_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        candidate_epoch = candidate_epoch.0,
        "prepare_next_epoch: resizing committee account"
    );
    let outcome =
        submit_if_at_tip(&ctx.ingest, "resize_committee", submit_resize_committee(ctx)).await;
    log_setup_outcome("resize_committee", epoch, candidate_epoch, outcome)
}

async fn submit_resize_peer_set_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    let candidate_epoch = epoch.saturating_add(EpochNumber(2));

    info!(
        epoch = epoch.0,
        candidate_epoch = candidate_epoch.0,
        "prepare_next_epoch: resizing peer set account"
    );

    let outcome =
        submit_if_at_tip(&ctx.ingest, "resize_peer_set", submit_resize_peer_set(ctx)).await;

    log_setup_outcome("resize_peer_set", epoch, candidate_epoch, outcome)
}

fn log_setup_outcome(
    action: &'static str,
    epoch: EpochNumber,
    candidate_epoch: EpochNumber,
    outcome: TxOutcome,
) -> bool {
    match outcome {
        TxOutcome::Confirmed(txid) => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, %txid, "prepare_next_epoch: confirmed");
            false
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::Program(err),
            ..
        } => {
            warn!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, ?err, "prepare_next_epoch: program error");
            false
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::KnownContention,
            err,
        } => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, %err, "prepare_next_epoch: setup already applied, waiting for state update");
            false
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::KnownStaleState,
            err,
        } => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, %err, "prepare_next_epoch: stale submission ignored");
            false
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::UnknownExecution,
            err,
        } => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, %err, "prepare_next_epoch: transaction rejected");
            false
        }
        TxOutcome::Rejected {
            kind: TxRejectionKind::Transport,
            err,
        } => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, %err, "prepare_next_epoch: transport error");
            false
        }
        TxOutcome::SkippedStale => {
            debug!(action, epoch = epoch.0, candidate_epoch = candidate_epoch.0, "prepare_next_epoch: ingest stale, deferring");
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{decide_setup_step, SetupStep};

    #[test]
    fn setup_step_creates_committee_after_epoch() {
        assert_eq!(
            decide_setup_step(20, 20, None, 0),
            SetupStep::CreateCommittee
        );
    }

    #[test]
    fn setup_step_resizes_one_account_at_a_time() {
        assert_eq!(
            decide_setup_step(20, 20, Some(0), 0),
            SetupStep::ResizeCommittee
        );

        assert_eq!(
            decide_setup_step(20, 20, Some(20), 0),
            SetupStep::ResizePeerSet
        );

        assert_eq!(decide_setup_step(20, 20, Some(20), 60), SetupStep::Done);
    }
}
