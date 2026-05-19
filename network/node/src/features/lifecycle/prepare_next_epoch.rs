use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_protocol::{Api, ProtocolState};
use tape_retry::{backoff_or_cancel, Backoff, RetryConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::chain::{
    submit_create_committee, submit_create_epoch, submit_resize_committee, submit_resize_peer_set,
};
use crate::context::NodeContext;
use crate::core::chain_tx::{submit_if_at_tip, TxOutcome};
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

// Purpose: ensure Epoch(N+1) and Committee(N+1) exist and are allocated before
// JoinCommittee and CommitEpoch need them. Group(N+1, *) accounts are created
// later by assignment finalization.

pub async fn run<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    cancel: CancellationToken,
) -> TaskDone {
    let target_epoch = epoch.saturating_add(EpochNumber(1));
    let mut backoff = Backoff::new(RetryConfig::infinite());

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
            EpochPhase::Active => {}
            EpochPhase::Closing | EpochPhase::Completed => {
                info!(
                    epoch = epoch.0,
                    phase = ?state.phase(),
                    "prepare_next_epoch: setup window has passed"
                );
                return TaskDone::Done(Action::PrepareNextEpoch, epoch);
            }
            phase => {
                info!(epoch = epoch.0, ?phase, "prepare_next_epoch: outside active phase");
                return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
            }
        }

        let step = next_setup_step(&state, target_epoch);
        drop(state);

        match step {
            SetupStep::Done => {
                info!(
                    epoch = epoch.0,
                    target_epoch = target_epoch.0,
                    "prepare_next_epoch: setup complete"
                );
                return TaskDone::Done(Action::PrepareNextEpoch, epoch);
            }
            SetupStep::InvalidCommitteeCapacity { capacity, target } => {
                warn!(
                    epoch = epoch.0,
                    target_epoch = target_epoch.0,
                    capacity,
                    target,
                    "prepare_next_epoch: next committee capacity exceeds target"
                );
                return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
            }
            step => {
                if submit_setup_step(&ctx, epoch, target_epoch, step).await {
                    return TaskDone::Rejected(Action::PrepareNextEpoch, epoch);
                }
            }
        }

        if backoff_or_cancel(&mut backoff, &cancel).await {
            break;
        }
    }

    TaskDone::Cancelled(Action::PrepareNextEpoch, epoch)
}

fn next_setup_step(state: &ProtocolState, target_epoch: EpochNumber) -> SetupStep {
    if !has_next_epoch(state, target_epoch) {
        return SetupStep::CreateEpoch;
    }

    if !has_next_committee(state, target_epoch) {
        return SetupStep::CreateCommittee;
    }

    let committee_target = state.system.committee_size;
    let committee_capacity = state.next_committee_capacity.unwrap_or(0);

    if committee_capacity < committee_target {
        return SetupStep::ResizeCommittee;
    }

    if committee_capacity > committee_target {
        return SetupStep::InvalidCommitteeCapacity {
            capacity: committee_capacity,
            target: committee_target,
        };
    }

    let peer_target = committee_target.saturating_mul(3);

    if state.peer_capacity < peer_target {
        return SetupStep::ResizePeerSet;
    }

    SetupStep::Done
}

async fn submit_setup_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    target_epoch: EpochNumber,
    step: SetupStep,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    match step {
        SetupStep::CreateEpoch => submit_create_epoch_step(ctx, epoch, target_epoch).await,
        SetupStep::CreateCommittee => submit_create_committee_step(ctx, epoch, target_epoch).await,
        SetupStep::ResizeCommittee => submit_resize_committee_step(ctx, epoch, target_epoch).await,
        SetupStep::ResizePeerSet => submit_resize_peer_set_step(ctx, epoch).await,
        SetupStep::Done | SetupStep::InvalidCommitteeCapacity { .. } => false,
    }
}

async fn submit_create_epoch_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        target_epoch = target_epoch.0,
        "prepare_next_epoch: creating epoch account"
    );
    let outcome = submit_if_at_tip(&ctx.ingest, submit_create_epoch(ctx, target_epoch)).await;
    log_setup_outcome("create_epoch", epoch, target_epoch, outcome)
}

async fn submit_create_committee_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        target_epoch = target_epoch.0,
        "prepare_next_epoch: creating committee account"
    );
    let outcome = submit_if_at_tip(&ctx.ingest, submit_create_committee(ctx, target_epoch)).await;
    log_setup_outcome("create_committee", epoch, target_epoch, outcome)
}

async fn submit_resize_committee_step<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> bool
where
    Db: Store,
    Cluster: Api,
    Blockchain: Rpc,
{
    info!(
        epoch = epoch.0,
        target_epoch = target_epoch.0,
        "prepare_next_epoch: resizing committee account"
    );
    let outcome = submit_if_at_tip(&ctx.ingest, submit_resize_committee(ctx, target_epoch)).await;
    log_setup_outcome("resize_committee", epoch, target_epoch, outcome)
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
    let target_epoch = epoch.saturating_add(EpochNumber(1));

    info!(
        epoch = epoch.0,
        target_epoch = target_epoch.0,
        "prepare_next_epoch: resizing peer set account"
    );

    let outcome = submit_if_at_tip(&ctx.ingest, submit_resize_peer_set(ctx)).await;

    log_setup_outcome("resize_peer_set", epoch, target_epoch, outcome)
}

fn log_setup_outcome(
    action: &'static str,
    epoch: EpochNumber,
    target_epoch: EpochNumber,
    outcome: TxOutcome,
) -> bool {
    match outcome {
        TxOutcome::Confirmed(txid) => {
            debug!(action, epoch = epoch.0, target_epoch = target_epoch.0, %txid, "prepare_next_epoch: confirmed");
            false
        }
        TxOutcome::Program(err) => {
            warn!(action, epoch = epoch.0, target_epoch = target_epoch.0, ?err, "prepare_next_epoch: program error");
            false
        }
        TxOutcome::Transport(err) => {
            debug!(action, epoch = epoch.0, target_epoch = target_epoch.0, %err, "prepare_next_epoch: transport error");
            false
        }
        TxOutcome::SkippedStale => {
            debug!(action, epoch = epoch.0, target_epoch = target_epoch.0, "prepare_next_epoch: ingest stale, deferring");
            true
        }
    }
}

fn has_next_epoch(state: &ProtocolState, target_epoch: EpochNumber) -> bool {
    state
        .next_epoch
        .as_ref()
        .is_some_and(|epoch| epoch.id == target_epoch)
}

fn has_next_committee(state: &ProtocolState, target_epoch: EpochNumber) -> bool {
    state.next_committee.is_some() && target_epoch == state.epoch() + EpochNumber(1)
}

#[cfg(test)]
mod tests {
    use super::{next_setup_step, SetupStep};
    use tape_api::state::Epoch;
    use tape_core::types::EpochNumber;
    use tape_protocol::ProtocolState;
    use bytemuck::Zeroable;

    fn setup_state() -> ProtocolState {
        let mut state = ProtocolState::default();
        state.current.epoch.id = EpochNumber(7);
        state.system.committee_size = 20;
        state
    }

    #[test]
    fn setup_step_creates_epoch_first() {
        let state = setup_state();
        assert_eq!(
            next_setup_step(&state, EpochNumber(8)),
            SetupStep::CreateEpoch
        );
    }

    #[test]
    fn setup_step_resizes_one_account_at_a_time() {
        let mut state = setup_state();
        state.next_epoch = Some(Epoch {
            id: EpochNumber(8),
            ..Epoch::zeroed()
        });
        state.next_committee = Some(Vec::new());

        assert_eq!(
            next_setup_step(&state, EpochNumber(8)),
            SetupStep::ResizeCommittee
        );

        state.next_committee_capacity = Some(20);
        assert_eq!(
            next_setup_step(&state, EpochNumber(8)),
            SetupStep::ResizePeerSet
        );

        state.peer_capacity = 60;
        assert_eq!(next_setup_step(&state, EpochNumber(8)), SetupStep::Done);
    }
}
