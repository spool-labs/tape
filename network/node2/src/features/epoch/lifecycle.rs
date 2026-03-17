// Epoch Lifecycle
//
// Pure decision function that determines which epoch action to run next.
// Not a traditional FSM — it's a single sequential progression through
// epoch phases, gated by both the on-chain phase and local completion state.
//
// The lifecycle worker (spawned alongside the EpochManager) calls
// next_action() on every state change and task completion to decide
// what to do. At most one action runs at a time.
//
// ── Decision table ──────────────────────────────────────────────────
//
//   Phase     | Condition                                   | Action
//   ----------|---------------------------------------------|----------------
//   Syncing   | in committee, spools not ready               | WaitSpoolReady
//   Syncing   | in committee, spools ready, sync not done    | SyncEpoch
//   Syncing   | SyncEpoch done or not in committee           | None (wait)
//   Settling  | in committee or prev, pool not done          | AdvancePool
//   Settling  | pool done or not in either                   | None (wait)
//   Active    | pool not done, in committee or prev          | AdvancePool
//   Active    | join not done, 90% time elapsed              | JoinNetwork
//   Active    | join done, advance not done                  | AdvanceEpoch
//   Active    | all done                                     | None (wait)
//
// ── Phase skipping ──────────────────────────────────────────────────
//
//   If the node comes online mid-epoch (e.g. phase is already Settling):
//     - SyncEpoch is skipped. The phase moved on; the on-chain program
//       would reject a late sync anyway.
//
//   If the phase jumps ahead while a task is running:
//     - The lifecycle worker cancels the current task via its CancellationToken.
//     - Re-evaluates next_action() with the new phase.
//
// ── Epoch reset ─────────────────────────────────────────────────────
//
//   When epoch advances, the done set is cleared and the lifecycle
//   restarts from Syncing.
//
// ── Spool readiness (WaitSpoolReady) ─────────────────────────────────
//
//   Before submitting SyncEpoch, the node must have all its assigned
//   spools in a ready state. This is a separate lifecycle action
//   (WaitSpoolReady) that polls the store until all owned spools are
//   Active. Once done, the lifecycle advances to SyncEpoch.
//   Readiness is determined by polling the store (iter_all_spools) —
//   not via a cross-feature channel.
//
// ── JoinNetwork timing gate ────────────────────────────────────────
//
//   JoinNetwork should not be submitted until 90% of the epoch
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
//   The lifecycle worker never gives up. If a task returns Rejected
//   or any error, the worker re-evaluates. If the state hasn't changed,
//   it respawns the same task. The system must be resilient to outages
//   and resume on its own.
//
// ── Worker architecture ─────────────────────────────────────────────
//
//   The lifecycle worker is a long-lived task that:
//     1. Subscribes to state_rx (epoch/phase changes from EpochManager).
//     2. Maintains at most ONE active task in a JoinSet.
//     3. Selects on: state_rx.changed(), join_set.join_next(), cancel.
//     4. On any wake-up: re-evaluate next_action().
//        - If current action is still correct: no-op (task continues).
//        - If action changed: cancel current task, spawn new one.
//        - If no action needed: ensure no task is running.
//
//   Individual tasks loop internally with retry and backoff.
//   They only return on: success, cancel, or permanent rejection.
//
// ── Relationship to EpochManager ────────────────────────────────────
//
//   EpochManager is the reactive side (processes blocks, updates state).
//   Lifecycle worker is the proactive side (submits transactions).
//   They share the same state_rx but are independent services.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rpc::Rpc;
use store::Store;
use tape_core::system::EpochPhase;
use tape_core::types::{EpochNumber, NodeId};
use tape_protocol::{Api, ProtocolState};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::epoch::types::{Action, TaskDone};
use crate::features::epoch::{advance_epoch, advance_pool, join_network, sync_epoch, wait_spool_ready};

/// Determine the next epoch action based on current state.
///
/// Returns None if no action is needed (waiting for phase change or next epoch).
pub fn next_action(
    state: &ProtocolState,
    node_id: NodeId,
    done: &HashSet<Action>,
) -> Option<Action> {
    let in_committee = state.find_member(node_id).is_some();
    let in_prev = state.committee_prev.iter().any(|m| m.id == node_id);

    match state.phase {
        EpochPhase::Syncing => {
            if in_committee && !done.contains(&Action::WaitSpoolReady) {
                return Some(Action::WaitSpoolReady);
            }
            if in_committee && !done.contains(&Action::SyncEpoch) {
                return Some(Action::SyncEpoch);
            }
            None
        }
        EpochPhase::Settling => {
            if (in_committee || in_prev) && !done.contains(&Action::AdvancePool) {
                return Some(Action::AdvancePool);
            }
            None
        }
        EpochPhase::Active => {
            // AdvancePool can still be submitted during Active if we missed Settling.
            if (in_committee || in_prev) && !done.contains(&Action::AdvancePool) {
                return Some(Action::AdvancePool);
            }

            // JoinNetwork: gated by time, checked by the task itself.
            if (in_committee || in_prev) && !done.contains(&Action::JoinNetwork) {
                return Some(Action::JoinNetwork);
            }

            // AdvanceEpoch: any committee member can submit it.
            if in_committee && !done.contains(&Action::AdvanceEpoch) {
                return Some(Action::AdvanceEpoch);
            }

            None
        }
        EpochPhase::Unknown => None,
    }
}

pub struct LifecycleWorker<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochLifecycleConfig,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    LifecycleWorker<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: EpochLifecycleConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    // Algorithm:
    //
    // 1. Subscribe to state_rx for epoch/phase changes.
    // 2. Initialize done set (empty) and observed_epoch from current state.
    // 3. Evaluate next_action(). If an action is needed, spawn it.
    // 4. Main loop selects on:
    //    a. cancel — shutdown. Cancel active task, drain, return.
    //    b. state_rx.changed() — epoch or phase changed.
    //       - If epoch changed: clear done set, cancel active task,
    //         drain join_set, re-evaluate.
    //       - If phase changed (same epoch): cancel active task if
    //         the current action is no longer correct, re-evaluate.
    //    c. join_set.join_next() — active task completed.
    //       - Done: add action to done set, re-evaluate.
    //       - Cancelled: re-evaluate (may respawn or move on).
    //       - Rejected: re-evaluate (respawn if state unchanged).
    //
    // 5. On re-evaluate: call next_action(). Compare with currently
    //    running action (if any).
    //    - Same action running → do nothing.
    //    - Different action or no action → cancel current, spawn new (or idle).
    pub async fn run(self) -> Result<(), NodeError> {
        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;
        let node_id = self.context.node_id();

        let mut done: HashSet<Action> = HashSet::new();
        let mut join_set: JoinSet<TaskDone> = JoinSet::new();
        let mut active_cancel: Option<(Action, CancellationToken)> = None;

        // Initial evaluation
        self.maybe_spawn(
            &state_rx.borrow(),
            node_id,
            &done,
            &mut active_cancel,
            &mut join_set,
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    if let Some((_, token)) = active_cancel.take() {
                        token.cancel();
                    }
                    while join_set.join_next().await.is_some() {}
                    return Ok(());
                }

                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    let state = state_rx.borrow().clone();
                    if state.epoch != observed_epoch {
                        info!(
                            old_epoch = observed_epoch.0,
                            new_epoch = state.epoch.0,
                            "lifecycle: epoch advanced, resetting"
                        );
                        done.clear();
                        if let Some((_, token)) = active_cancel.take() {
                            token.cancel();
                        }
                        while join_set.join_next().await.is_some() {}
                        observed_epoch = state.epoch;
                    }

                    self.maybe_spawn(
                        &state,
                        node_id,
                        &done,
                        &mut active_cancel,
                        &mut join_set,
                    );
                }

                Some(result) = join_set.join_next() => {
                    let task_done = match result {
                        Ok(td) => td,
                        Err(error) => {
                            warn!(error = %error, "lifecycle task panicked");
                            active_cancel = None;
                            let state = state_rx.borrow().clone();
                            self.maybe_spawn(&state, node_id, &done, &mut active_cancel, &mut join_set);
                            continue;
                        }
                    };

                    active_cancel = None;

                    // Ignore results from stale epochs.
                    if task_done.epoch() != observed_epoch {
                        let state = state_rx.borrow().clone();
                        self.maybe_spawn(&state, node_id, &done, &mut active_cancel, &mut join_set);
                        continue;
                    }

                    match task_done {
                        TaskDone::Done(action, _) => {
                            info!(action = ?action, epoch = observed_epoch.0, "lifecycle: action complete");
                            done.insert(action);
                        }
                        TaskDone::Cancelled(action, _) => {
                            debug!(action = ?action, epoch = observed_epoch.0, "lifecycle: task cancelled");
                        }
                        TaskDone::Rejected(action, _) => {
                            debug!(action = ?action, epoch = observed_epoch.0, "lifecycle: task rejected, will re-evaluate");
                        }
                    }

                    let state = state_rx.borrow().clone();
                    self.maybe_spawn(&state, node_id, &done, &mut active_cancel, &mut join_set);
                }
            }
        }
    }

    /// Evaluate next_action and spawn if the desired action differs from what's running.
    fn maybe_spawn(
        &self,
        state: &ProtocolState,
        node_id: NodeId,
        done: &HashSet<Action>,
        active: &mut Option<(Action, CancellationToken)>,
        join_set: &mut JoinSet<TaskDone>,
    ) {
        let desired = next_action(state, node_id, done);

        // If already running the right action, do nothing.
        if let Some((current_action, _)) = active {
            if desired == Some(*current_action) {
                return;
            }
            // Wrong action running — cancel it.
            if let Some((_, token)) = active.take() {
                token.cancel();
            }
        }

        let Some(action) = desired else {
            return;
        };

        let token = CancellationToken::new();
        let epoch = state.epoch;

        debug!(action = ?action, epoch = epoch.0, "lifecycle: spawning task");

        match action {
            Action::WaitSpoolReady => {
                join_set.spawn(wait_spool_ready::run(
                    self.context.clone(),
                    self.config.clone(),
                    epoch,
                    token.clone(),
                ));
            }
            Action::SyncEpoch => {
                join_set.spawn(sync_epoch::run(
                    self.context.clone(),
                    self.config.clone(),
                    epoch,
                    token.clone(),
                ));
            }
            Action::AdvancePool => {
                join_set.spawn(advance_pool::run(
                    self.context.clone(),
                    self.config.clone(),
                    epoch,
                    token.clone(),
                ));
            }
            Action::JoinNetwork => {
                join_set.spawn(join_network::run(
                    self.context.clone(),
                    self.config.clone(),
                    epoch,
                    token.clone(),
                ));
            }
            Action::AdvanceEpoch => {
                join_set.spawn(advance_epoch::run(
                    self.context.clone(),
                    self.config.clone(),
                    epoch,
                    token.clone(),
                ));
            }
        }

        *active = Some((action, token));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::CommitteeMember;
    use tape_core::types::coin::{Coin, TAPE};

    const NODE: NodeId = NodeId(1);
    const OTHER: NodeId = NodeId(2);

    fn state_with(phase: EpochPhase, in_current: bool, in_prev: bool) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.phase = phase;
        if in_current {
            state
                .committee
                .push(CommitteeMember::new(NODE, Coin::<TAPE>::new(1000)));
        }
        if in_prev {
            state
                .committee_prev
                .push(CommitteeMember::new(NODE, Coin::<TAPE>::new(1000)));
        }
        state
    }

    #[test]
    fn wait_spools() {
        let state = state_with(EpochPhase::Syncing, true, false);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::WaitSpoolReady));
    }

    #[test]
    fn sync_after_spools() {
        let state = state_with(EpochPhase::Syncing, true, false);
        let done = HashSet::from([Action::WaitSpoolReady]);
        assert_eq!(next_action(&state, NODE, &done), Some(Action::SyncEpoch));
    }

    #[test]
    fn syncing_standby_node() {
        let state = state_with(EpochPhase::Syncing, false, false);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), None);
    }

    #[test]
    fn syncing_all_done() {
        let state = state_with(EpochPhase::Syncing, true, false);
        let done = HashSet::from([Action::WaitSpoolReady, Action::SyncEpoch]);
        assert_eq!(next_action(&state, NODE, &done), None);
    }

    #[test]
    fn settling_prev_member() {
        let state = state_with(EpochPhase::Settling, false, true);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::AdvancePool));
    }

    #[test]
    fn settling_current_member() {
        let state = state_with(EpochPhase::Settling, true, false);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::AdvancePool));
    }

    #[test]
    fn settling_neither() {
        let state = state_with(EpochPhase::Settling, false, false);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), None);
    }

    #[test]
    fn pool_first() {
        let state = state_with(EpochPhase::Active, true, true);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::AdvancePool));
    }

    #[test]
    fn join_next() {
        let state = state_with(EpochPhase::Active, true, false);
        let done = HashSet::from([Action::AdvancePool]);
        assert_eq!(next_action(&state, NODE, &done), Some(Action::JoinNetwork));
    }

    #[test]
    fn advance_next() {
        let state = state_with(EpochPhase::Active, true, false);
        let done = HashSet::from([Action::AdvancePool, Action::JoinNetwork]);
        assert_eq!(next_action(&state, NODE, &done), Some(Action::AdvanceEpoch));
    }

    #[test]
    fn active_all_done() {
        let state = state_with(EpochPhase::Active, true, false);
        let done = HashSet::from([Action::AdvancePool, Action::JoinNetwork, Action::AdvanceEpoch]);
        assert_eq!(next_action(&state, NODE, &done), None);
    }

    #[test]
    fn skip_active() {
        // Node comes online at Active, never synced — skips SyncEpoch,
        // goes straight to AdvancePool (if in committee).
        let state = state_with(EpochPhase::Active, true, true);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::AdvancePool));
    }

    #[test]
    fn standby_prev_only() {
        // Not in current committee but was in previous. During Active,
        // still needs AdvancePool and JoinNetwork.
        let state = state_with(EpochPhase::Active, false, true);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), Some(Action::AdvancePool));

        let done = HashSet::from([Action::AdvancePool]);
        assert_eq!(next_action(&state, NODE, &done), Some(Action::JoinNetwork));

        // But cannot AdvanceEpoch — not in current committee.
        let done = HashSet::from([Action::AdvancePool, Action::JoinNetwork]);
        assert_eq!(next_action(&state, NODE, &done), None);
    }

    #[test]
    fn unknown_phase() {
        let state = state_with(EpochPhase::Unknown, true, true);
        assert_eq!(next_action(&state, NODE, &HashSet::new()), None);
    }
}
