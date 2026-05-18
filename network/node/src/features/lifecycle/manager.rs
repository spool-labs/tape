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
//   Syncing   | in committee, spools not ready               | WaitSpoolReady
//   Syncing   | in committee, spools ready, sync not done    | SyncSpools
//   Syncing   | SyncSpools done or not in committee          | None (wait)
//   Settling  | previous spools not settled                  | SettleSpools
//   Settling  | settlement done, pool not done               | AdvancePool
//   Snapshot  | previous spools not settled                  | SettleSpools
//   Snapshot  | settlement done, pool not done               | AdvancePool
//   Active    | previous spools not settled                  | SettleSpools
//   Active    | settlement done, pool not done               | AdvancePool
//   Active    | join not done, 90% time elapsed              | JoinCommittee
//   Active    | join done, advance not done                  | AdvanceEpoch
//   Active    | all done                                     | None (wait)
//
// ── Phase skipping ──────────────────────────────────────────────────
//
//   If the node comes online mid-epoch (e.g. phase is already Settling):
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
//   restarts from Syncing.
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
use tracing::{debug, info, warn};

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::lifecycle::types::{Action, TaskDone};
use crate::features::lifecycle::{
    advance_epoch, advance_pool, join_committee, settle_spools, sync_spools, wait_spool_ready,
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

        tasks.spawn(async move {
            match action {
                Action::WaitSpoolReady => wait_spool_ready::run(ctx, epoch, token).await,
                Action::SyncSpools     => sync_spools::run(ctx, epoch, token).await,
                Action::SettleSpools   => settle_spools::run(ctx, epoch, token).await,
                Action::AdvancePool    => advance_pool::run(ctx, epoch, token).await,
                Action::JoinCommittee  => join_committee::run(ctx, epoch, token).await,
                Action::AdvanceEpoch   => advance_epoch::run(ctx, epoch, token).await,
            }
        });

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

    let has_previous_spools = !state.member_spools_prev(node).is_empty();
    let settlement_done = !has_previous_spools || done.contains(&Action::SettleSpools);
    let phase = state.phase();

    // If we had spools in the previous epoch, we need to settle them before doing anything else.
    if has_previous_spools
        && !done.contains(&Action::SettleSpools)
        && phase_allows_settle_spools(phase)
    {
        return Some(Action::SettleSpools);
    }

    // If we have settled previous spools, we have rewards pending.
    if settlement_done 
        && !done.contains(&Action::AdvancePool) 
        && phase_allows_advance_pool(phase) 
    {
        return Some(Action::AdvancePool);
    }

    // Global phase, not just our local state. The gates progress via 2/3 majority. We may still
    // want to run some of these actions even if the phase has advanced.
    match phase {

        // We only need to attest SyncSpool during Syncing. Once we move past Syncing, it's too
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

        EpochPhase::Settle => {
            // Handled above, we typically always want to settle all our previous spools,
            // regardless of phase. 
            None
        }

        EpochPhase::Snapshot => {
            None
        }

        EpochPhase::Active => {
            // JoinCommittee: gated by time, checked by the task itself.
            if !in_next && !done.contains(&Action::JoinCommittee) {
                return Some(Action::JoinCommittee);
            }

            // AdvanceEpoch: anyone can submit it.
            if !done.contains(&Action::AdvanceEpoch) {
                return Some(Action::AdvanceEpoch);
            }

            None
        }

        EpochPhase::Unknown
        | EpochPhase::Closing
        | EpochPhase::Completed => None,
    }
}

fn phase_allows_advance_pool(phase: EpochPhase) -> bool {
    match phase {
        EpochPhase::Settle => true,
        EpochPhase::Snapshot => true,
        EpochPhase::Active => true,
        _ => false,
    }
}

fn phase_allows_settle_spools(phase: EpochPhase) -> bool {
    match phase {
        EpochPhase::Settle => true,
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
    use tape_crypto::Address;
    use tape_protocol::{EpochBundle, ProtocolState};

    use super::next_action;
    use crate::features::lifecycle::types::Action;

    fn member(node: Address) -> Member {
        Member {
            node,
            stake: TAPE(100),
            blacklist: StorageUnits::zero(),
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

    #[test]
    fn settle_runs_before_advance_pool() {
        let node = Address::new_unique();
        let state = state_with_previous_spool(node, EpochPhase::Settle);
        let done = HashSet::new();

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::SettleSpools));
    }

    #[test]
    fn settle_done_allows_advance_pool() {
        let node = Address::new_unique();
        let state = state_with_previous_spool(node, EpochPhase::Settle);
        let mut done = HashSet::new();
        done.insert(Action::SettleSpools);

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::AdvancePool));
    }

    #[test]
    fn active_skips_join_when_already_in_next_committee() {
        let node = Address::new_unique();
        let mut state = state_with_phase(EpochPhase::Active);
        state.next_committee = Some(vec![member(node)]);
        let mut done = HashSet::new();
        done.insert(Action::AdvancePool);

        let action = next_action(&state, node, &done);

        assert_eq!(action, Some(Action::AdvanceEpoch));
    }
}
