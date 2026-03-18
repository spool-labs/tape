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

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_core::system::EpochPhase;
use tape_core::types::NodeId;
use tape_protocol::{Api, ProtocolState};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::EpochLifecycleConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
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
            if !done.contains(&Action::JoinNetwork) {
                return Some(Action::JoinNetwork);
            }

            // AdvanceEpoch: anyone can submit it.
            if !done.contains(&Action::AdvanceEpoch) {
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

    pub async fn run(self) -> Result<(), NodeError> {

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;
        let mut done: HashSet<Action> = HashSet::new();


        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    // Cancel any active task before exiting.
                    
                    return Ok(());
                }

                changed = state_rx.changed() => {
                    if changed.is_err() {
                        warn!("lifecycle: state channel closed, exiting");
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
                        observed_epoch = state.epoch;
                    }

                    // Try to make progress after a state change
                    self.run_next(observed_epoch, &mut done, self.cancel.clone()).await;
                }

                _ = tokio::time::sleep(self.config.interval) => {
                    // We use the last known epoch we processed
                    self.run_next(observed_epoch, &mut done, self.cancel.clone()).await;
                }
            }
        }
    }

    /// Evaluate what lifecycle action should run next, and run it if needed.
    async fn run_next(
        &self,
        epoch: EpochNumber,
        done: &mut HashSet<Action>,
        token: CancellationToken,
    ) {

        let context = self.context.clone();
        let state = context.state();
        let node_id = context.node_id();

        if let Some(action) = next_action(&state, node_id, done) {
            let result = match action {
                Action::WaitSpoolReady => {
                    wait_spool_ready::run(context, epoch, token).await
                }
                Action::SyncEpoch => {
                    sync_epoch::run(context, epoch, token).await
                }
                Action::AdvancePool => {
                    advance_pool::run(context, epoch, token).await
                }
                Action::JoinNetwork => {
                    join_network::run(context, epoch, token).await
                }
                Action::AdvanceEpoch => {
                    advance_epoch::run(context, epoch, token).await
                }
            };

            // Determine if the task completed successfully, was cancelled, or rejected.
            match result {
                TaskDone::Done(action, epoch) => {
                    info!(action = ?action, epoch = epoch.0, "lifecycle: task completed");
                    done.insert(action);
                }
                TaskDone::Rejected(action, epoch) => {
                    debug!(action = ?action, epoch = epoch.0, "lifecycle: task rejected");
                }
                TaskDone::Cancelled(action, epoch) => {
                    debug!(action = ?action, epoch = epoch.0, "lifecycle: task cancelled");
                }
            }
        }
    }
}

