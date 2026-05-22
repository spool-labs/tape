// Epoch Lifecycle
//
// Pure decision function that determines which epoch action to run next.
// Not a traditional FSM, it's a single sequential progression through
// epoch phases, gated by both the on-chain phase and local completion state.
//
// The lifecycle manager (spawned alongside the StateManager) calls
// next_action() on every state change and task completion to decide
// what to do. At most one action runs at a time.
//
// ── Decision table ──────────────────────────────────────────────────
//
//   Phase     | Condition                                   | Action
//   ----------|---------------------------------------------|----------------
//   Sync      | in committee, spools not ready               | WaitSpoolReady
//   Sync      | in committee, spools ready, sync not done    | SyncSpools
//   Sync      | SyncSpools done or not in committee          | None (wait)
//   Snapshot  | pool not done                                | AdvancePool
//   Snapshot  | pool done                                    | None (snapshot manager)
//   Active    | pool not done                                | AdvancePool
//   Active    | next epoch setup incomplete                  | PrepareNextEpoch
//   Active    | setup done, join not done, 90% time elapsed  | JoinCommittee
//   Active    | join done, commit not done                   | CommitEpoch
//   Active    | all done                                     | None (wait)
//   Closing   | assignment incomplete                        | None (assignment manager)
//   Closing   | assignment complete, advance not done        | AdvanceEpoch
//   Closing   | advance done                                 | None (wait)
//
// ── Phase skipping ──────────────────────────────────────────────────
//
//   If the node comes online mid-epoch:
//     - SyncSpools is skipped. The phase moved on; the on-chain program
//       would reject a late sync anyway.
//
//   If the phase jumps ahead while a task is running:
//     - The running task is responsible for deciding whether the new phase is
//       still acceptable. If it completes or rejects, the manager replans.
//
// ── Epoch reset ─────────────────────────────────────────────────────
//
//   When epoch advances, the done set is cleared and the lifecycle
//   restarts from Sync.
//
// ── Spool readiness (WaitSpoolReady) ─────────────────────────────────
//
//   Before submitting SyncSpool, the node must have all its assigned
//   spools in a ready state. This is a separate lifecycle action
//   (WaitSpoolReady) that polls the store until all owned spools are
//   Active. Once done, the lifecycle advances to SyncSpools.
//   Readiness is determined by polling the store (iter_all_spools) 
//   not via a cross-feature channel.
//
// ── JoinCommittee timing gate ──────────────────────────────────────
//
//   JoinCommittee should not be submitted until 90% of the epoch
//   duration has elapsed. This prevents committing to the next epoch
//   too early (which could be up to a week) and risking unavailability
//   at the transition point.
//
//   Calculated as: now >= last_epoch + (EPOCH_DURATION * 90 / 100)
//
//   Requires `last_epoch` timestamp in ProtocolState (needs to be added
//   to fetch_state from the on-chain Epoch account).
//
// ── No permanent failure ────────────────────────────────────────────
//
//   The lifecycle manager never gives up. If a task returns Rejected
//   or any error, the manager keeps the action eligible and will retry
//   it on the next heartbeat or state change. Successful completions
//   still replan immediately so the happy path stays fast.
//
//   This deliberately avoids a tight respawn loop when a task can
//   reject quickly against unchanged local or on-chain state (for
//   example JoinCommittee returning NotStaked/NodeStale). The system
//   remains resilient to outages and resumes on its own, but retries
//   are paced by the existing lifecycle interval instead of a raw
//   completion-driven spin loop.
//
// ── Manager architecture ────────────────────────────────────────────
//
//   The lifecycle manager is a long-lived task that:
//     1. Subscribes to state_rx (epoch/phase changes from StateManager).
//     2. Maintains at most ONE active task in a JoinSet.
//     3. Selects on: state_rx.changed(), join_set.join_next(), cancel.
//     4. On state change or heartbeat: re-evaluate next_action().
//        - If current action is still correct: no-op (task continues).
//        - If action changed: cancel current task, spawn new one.
//        - If no action needed: ensure no task is running.
//     5. On task completion:
//        - Done → re-evaluate immediately so the next lifecycle step
//          can start without waiting for the heartbeat.
//        - Rejected / panic → wait for heartbeat or state change before
//          retrying, which prevents a hot loop on unchanged state.
//        - Cancelled → rely on the state-change path that caused the
//          cancellation.
//
//   Individual tasks loop internally with retry and backoff.
//   They only return on: success, cancel, or permanent rejection.
//
// ── Relationship to StateManager ────────────────────────────────────
//
//   StateManager is the reactive side (processes blocks, updates state).
//   Lifecycle manager is the proactive side (submits transactions).
//   They share the same state_rx but are independent services.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_protocol::{Api, ProtocolState};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::lifecycle::types::{Action, TaskDone};
use crate::features::lifecycle::{
    advance_epoch, advance_pool, commit_epoch, join_committee, prepare_next_epoch, sync_spools,
    wait_spool_ready,
};

const LIFECYCLE_HEARTBEAT: Duration = Duration::from_secs(1);

pub struct LifecycleManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    LifecycleManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch();
        let mut done: HashSet<Action> = HashSet::new();
        let mut tasks: JoinSet<TaskDone> = JoinSet::new();

        let mut running: Option<Action> = None;

        loop {
            tokio::select! {
                // Shutdown signal
                _ = self.cancel.cancelled() => {
                    info!("lifecycle: shutting down, aborting tasks");

                    tasks.abort_all();
                    return Ok(());
                }

                // Task completed
                Some(completion) = tasks.join_next() => {
                    let replan_immediately = match completion {
                        Ok(result) => {
                            self.handle_task_completion(result, &mut done, &mut running)
                        }
                        Err(e) => {
                            if e.is_cancelled() {
                                // Abort was intentional (epoch change). The state-change path
                                // already cleared or replaced `running`, so we intentionally
                                // defer replanning to that path to avoid duplicating work.
                                debug!("lifecycle: task was aborted");
                            } else {
                                // A panic can otherwise cause an immediate respawn on unchanged
                                // state and devolve into a tight loop, so wait for the
                                // heartbeat or a fresh state update before trying again.
                                warn!(?e, "lifecycle: task panicked");
                                running = None;
                            }
                            false
                        }
                    };

                    if replan_immediately {
                        self.try_spawn_next(&mut tasks, &mut running, &mut done, observed_epoch);
                    }
                }

                // State changed ("replan" signal)
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        warn!("lifecycle: state channel closed");

                        tasks.abort_all();
                        return Ok(());
                    }

                    let state = state_rx.borrow().clone();

                    if state.epoch() != observed_epoch {
                        info!(
                            old_epoch = observed_epoch.0,
                            new_epoch = state.epoch().0,
                            "lifecycle: epoch advanced, resetting"
                        );

                        done.clear();
                        tasks.abort_all();
                        running = None;

                        observed_epoch = state.epoch();
                    }

                    self.try_spawn_next(&mut tasks, &mut running, &mut done, observed_epoch);
                }

                // Periodic heartbeat
                _ = tokio::time::sleep(LIFECYCLE_HEARTBEAT) => {
                    self.try_spawn_next(&mut tasks, &mut running, &mut done, observed_epoch);
                }
            }
        }
    }

    /// Try to spawn the next pending action, but only if nothing is currently running.
    fn try_spawn_next(
        &self,
        tasks: &mut JoinSet<TaskDone>,
        running: &mut Option<Action>,
        done: &mut HashSet<Action>,
        epoch: EpochNumber,
    ) {

        // If something is already running -> skip
        if running.is_some() {
            return;
        }

        // Block ingestor must be caught up to the finalized dispatch edge
        // before we plan any protocol-changing transaction.
        if !self.context.is_at_tip() {
            debug!(epoch = epoch.0, "lifecycle: skipping spawn (ingest not at tip)");
            return;
        }

        let state = self.context.state();
        let node = self.context.node_address();

        let Some(action) = next_action(&state, node, done) else {
            return;
        };

        info!(?action, epoch = epoch.0, "lifecycle: spawning task");

        let ctx = self.context.clone();
        let token = self.cancel.child_token();

        tasks.spawn(
            async move {
                match action {
                    Action::WaitSpoolReady => wait_spool_ready::run(ctx, epoch, token).await,
                    Action::SyncSpools => sync_spools::run(ctx, epoch, token).await,
                    Action::AdvancePool => advance_pool::run(ctx, epoch, token).await,
                    Action::PrepareNextEpoch => prepare_next_epoch::run(ctx, epoch, token).await,
                    Action::JoinCommittee => join_committee::run(ctx, epoch, token).await,
                    Action::CommitEpoch => commit_epoch::run(ctx, epoch, token).await,
                    Action::AdvanceEpoch => advance_epoch::run(ctx, epoch, token).await,
                }
            }
            .in_current_span(),
        );

        *running = Some(action);
    }

    fn handle_task_completion(
        &self,
        result: TaskDone,
        done: &mut HashSet<Action>,
        running: &mut Option<Action>,
    ) -> bool {
        let replan_immediately = matches!(result, TaskDone::Done(..));
        let action = match &result {
            TaskDone::Done(a, _) | 
            TaskDone::Rejected(a, _) | 
            TaskDone::Cancelled(a, _) => *a,
        };

        // Clear running only if it matches the completed action (defensive)
        if running.as_ref() == Some(&action) {
            *running = None;
        }

        match result {
            TaskDone::Done(action, epoch) => {
                info!(?action, epoch = epoch.0, "lifecycle: task completed");

                done.insert(action);
            }
            TaskDone::Rejected(action, epoch) => {
                debug!(?action, epoch = epoch.0, "lifecycle: task rejected");

                // Keep the action eligible, but let the heartbeat/state-change path pace retries.
                // This avoids immediately respawning a task that just rejected against unchanged
                // prerequisites (for example JoinCommittee on a stale or unstaked node).
            }
            TaskDone::Cancelled(action, epoch) => {
                debug!(?action, epoch = epoch.0, "lifecycle: task cancelled");
            }
        }

        replan_immediately
    }
}

/// Determine the next epoch action based on current state.
///
/// Returns None if no action is needed (waiting for phase change or next epoch).
pub fn next_action(
    state: &ProtocolState,
    node: Address,
    done: &HashSet<Action>,
) -> Option<Action> {

    let in_current = state.find_member(node).is_some();
    let in_next = state.find_member_next(node).is_some();

    let phase = state.phase();

    if !done.contains(&Action::AdvancePool) 
        && phase_allows_advance_pool(phase) 
    {
        return Some(Action::AdvancePool);
    }

    // Global phase, not just our local state. The gates progress via 2/3 majority. We may still
    // want to run some of these actions even if the phase has advanced.
    match phase {

        // We only need to attest SyncSpool during Sync. Once we move past Sync, it's too
        // late to sync and the program would reject it anyway, so we skip it.
        EpochPhase::Sync => {

            // Wait for our spools to be ready before attesting SyncSpool.
            if in_current && !done.contains(&Action::WaitSpoolReady) {
                return Some(Action::WaitSpoolReady);
            }

            // Once spools are ready, submit SyncSpool for each assigned spool.
            if in_current && !done.contains(&Action::SyncSpools) {
                return Some(Action::SyncSpools);
            }

            None
        }

        EpochPhase::Snapshot => {
            // SnapshotManager owns proposal, voting, and finalization.
            None
        }

        EpochPhase::Active => {
            if !next_epoch_setup_ready(state) {
                return None;
            }

            // JoinCommittee: gated by time, checked by the task itself.
            if !in_next && !done.contains(&Action::JoinCommittee) {
                return Some(Action::JoinCommittee);
            }

            // CommitEpoch: captures the next-epoch nonce and enters Closing.
            if !done.contains(&Action::CommitEpoch) {
                return Some(Action::CommitEpoch);
            }

            None
        }

        EpochPhase::Unknown
        | EpochPhase::Completed => None,

        EpochPhase::Closing => {
            if !done.contains(&Action::PrepareNextEpoch) {
                return Some(Action::PrepareNextEpoch);
            }

            if assignment_ready(state) && !done.contains(&Action::AdvanceEpoch) {
                return Some(Action::AdvanceEpoch);
            }

            None
        }
    }
}

fn assignment_ready(state: &ProtocolState) -> bool {
    let Some(next_epoch) = state.next_epoch.as_ref() else {
        return false;
    };

    next_epoch.has_assignment_hash() && next_epoch.total_groups == state.system.target_group_count
}

fn next_epoch_setup_ready(state: &ProtocolState) -> bool {
    let next = state.epoch().saturating_add(EpochNumber(1));
    let next_epoch_ready = state
        .next_epoch
        .as_ref()
        .is_some_and(|epoch| epoch.id == next);
    let next_committee_ready = state.next_committee.is_some()
        && state
            .next_committee_capacity
            .is_some_and(|capacity| capacity == state.system.committee_size);
    next_epoch_ready && next_committee_ready
}

fn phase_allows_advance_pool(phase: EpochPhase) -> bool {
    match phase {
        EpochPhase::Snapshot => true,
        EpochPhase::Active => true,
        _ => false,
    }
}


#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytemuck::Zeroable;
    use tape_api::state::{Epoch, Group};
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{EpochPhase, Member};
    use tape_core::types::coin::TAPE;
    use tape_core::types::{EpochNumber, StorageUnits};
    use tape_crypto::{Address, Hash};
    use tape_protocol::{EpochBundle, ProtocolState};

    use super::next_action;
    use crate::features::lifecycle::types::Action;

    fn member(node: Address) -> Member {
        Member {
            node,
            stake: TAPE(100),
            assigned: StorageUnits::zero(),
            blacklisted: StorageUnits::zero(),
            spools: 1,
        }
    }

    fn state_with_phase(phase: EpochPhase) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.current.epoch.state.phase = phase as u64;
        state
    }

    fn state_with_previous_spool(node: Address, phase: EpochPhase) -> ProtocolState {
        let mut state = state_with_phase(phase);
        let prev = EpochNumber(1);
        let curr = EpochNumber(2);
        let group_id = GroupIndex(0);
        let mut group = Group {
            id: group_id,
            epoch: prev,
            ..Group::zeroed()
        };
        group.spools[0].node = node;

        state.current.epoch.id = curr;
        state.previous = Some(EpochBundle {
            epoch: Epoch {
                id: prev,
                ..Epoch::zeroed()
            },
            committee: vec![member(node)],
            groups: vec![group],
        });
        state
    }

    fn state_with_next_assignment(
        phase: EpochPhase,
        assignment_hash: Hash,
        total_groups: u64,
        target_group_count: u64,
    ) -> ProtocolState {
        let mut state = state_with_phase(phase);
        state.system.target_group_count = target_group_count;
        state.next_epoch = Some(Epoch {
            id: EpochNumber(2),
            assignment_hash,
            total_groups,
            ..Epoch::zeroed()
        });
        state
    }

    #[test]
    fn snapshot_advances_pool() {
        let node = Address::new_unique();
        let state = state_with_previous_spool(node, EpochPhase::Snapshot);
        let done = HashSet::new();

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::AdvancePool));
    }

    #[test]
    fn snapshot_waits_after_pool_done() {
        let node = Address::new_unique();
        let state = state_with_previous_spool(node, EpochPhase::Snapshot);
        let mut done = HashSet::new();
        done.insert(Action::AdvancePool);

        let action = next_action(&state, node, &done);

        assert_eq!(action, None);
    }

    #[test]
    fn active_waits_when_next_epoch_setup_missing() {
        let node = Address::new_unique();
        let state = state_with_phase(EpochPhase::Active);
        let mut done = HashSet::new();
        done.insert(Action::AdvancePool);

        let action = next_action(&state, node, &done);

        assert_eq!(action, None);
    }

    #[test]
    fn active_skips_join_when_already_in_next_committee() {
        let node = Address::new_unique();
        let mut state = state_with_phase(EpochPhase::Active);
        state.next_epoch = Some(Epoch {
            id: state.epoch().saturating_add(EpochNumber(1)),
            ..Epoch::zeroed()
        });
        state.next_committee = Some(vec![member(node)]);
        state.next_committee_capacity = Some(state.system.committee_size);
        state.peer_capacity = state.system.committee_size.saturating_mul(3);
        let mut done = HashSet::new();
        done.insert(Action::AdvancePool);

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::CommitEpoch));
    }

    #[test]
    fn closing_waits_when_assignment_hash_missing() {
        let node = Address::new_unique();
        let state = state_with_next_assignment(EpochPhase::Closing, Hash::zeroed(), 8, 8);
        let mut done = HashSet::new();
        done.insert(Action::PrepareNextEpoch);

        let action = next_action(&state, node, &done);

        assert_eq!(action, None);
    }

    #[test]
    fn closing_waits_when_assignment_groups_incomplete() {
        let node = Address::new_unique();
        let state = state_with_next_assignment(EpochPhase::Closing, Hash::from([7; 32]), 7, 8);
        let mut done = HashSet::new();
        done.insert(Action::PrepareNextEpoch);

        let action = next_action(&state, node, &done);

        assert_eq!(action, None);
    }

    #[test]
    fn closing_advances_when_assignment_complete() {
        let node = Address::new_unique();
        let state = state_with_next_assignment(EpochPhase::Closing, Hash::from([7; 32]), 8, 8);
        let mut done = HashSet::new();
        done.insert(Action::PrepareNextEpoch);

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::AdvanceEpoch));
    }

    #[test]
    fn closing_prepares_next_epoch_before_advance() {
        let node = Address::new_unique();
        let state = state_with_next_assignment(EpochPhase::Closing, Hash::from([7; 32]), 8, 8);
        let done = HashSet::new();

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::PrepareNextEpoch));
    }
}
