//! TaskScheduler — diffs desired vs running tasks based on FSM state changes.
//!
//! The scheduler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the task_runner. It maintains a view of what tasks *should*
//! be running and tells the task_runner to schedule or cancel tasks accordingly.

use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::pubkey::Pubkey;
use tape_protocol::Api;
use store::Store;
use tape_core::system::EpochPhase;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use tape_core::types::EpochNumber;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;

use crate::core::NodeContext;
use crate::fsm::StateChange;
use crate::{Task, TaskResult};

use crate::scheduler::LifecyclePlanner;
use crate::scheduler::SnapshotPlanner;
use crate::scheduler::SpoolPlanner;

const EPOCH_TASK_RETENTION_WINDOW: u64 = 2;

/// An action from the scheduler to the task_runner.
#[derive(Debug, Clone)]
pub enum Action {
    /// Schedule a new task.
    Schedule(Task),

    /// Cancel a running task.
    Cancel(Task),
}

/// Diffs desired state against running tasks to produce scheduling actions.
///
/// Maintains two core sets — `desired` (what SHOULD run) and `scheduled`
/// (what we've told the task_runner to run). Each tick, the diff between them
/// produces Schedule and Cancel actions. State changes from the FSM mutate
/// `desired`; task results from the task_runner remove keys from `scheduled`.
pub struct TaskScheduler<Db: Store, Cluster: Api, Blockchain: Rpc> {
    /// Shared node state (store, RPC client, identity, config).
    pub context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    /// Tasks that should be running given current state.
    pub desired: HashSet<Task>,
    /// Tasks we've told the task_runner to schedule (and haven't completed/cancelled).
    pub scheduled: HashSet<Task>,
    /// Epoch-scoped lifecycle task planner.
    pub lifecycle: LifecyclePlanner,
    /// Snapshot pipeline planner.
    pub snapshot: SnapshotPlanner,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> TaskScheduler<Db, Cluster, Blockchain> {
    pub fn new(context: Arc<NodeContext<Db, Cluster, Blockchain>>) -> Self {
        Self {
            context,
            desired: HashSet::new(),
            scheduled: HashSet::new(),
            lifecycle: LifecyclePlanner::new(),
            snapshot: SnapshotPlanner::new(),
        }
    }

    /// Main event loop. Selects over FSM state changes and task_runner task
    /// results. Each event recomputes `desired` and emits the diff as
    /// Schedule/Cancel actions to the task_runner.
    pub async fn run(
        mut self,
        mut change_rx: mpsc::Receiver<Vec<StateChange>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        action_tx: mpsc::Sender<Action>,
        cancel: CancellationToken,
    ) {
        // On startup, rebuild desired work from the current context-owned protocol state.
        self.bootstrap_desired();

        self.flush(&action_tx);
        tracing::trace!("scheduler bootstrapped");

        let mut received_changes: usize = 0;
        let mut handled_results: usize = 0;

        loop {
            tokio::select! {
                changes = change_rx.recv() => {
                    match changes {
                        Some(changes) => {
                            received_changes += 1;
                            tracing::trace!(
                                change_count = changes.len(),
                                received_changes,
                                "scheduler received state changes"
                            );
                            self.update_desired(&changes);
                            self.flush(&action_tx);
                        }
                        None => break,
                    }
                }

                result = result_rx.recv() => {
                    match result {
                        Some(result) => {
                            handled_results += 1;
                            tracing::trace!(
                                result = ?result,
                                handled_results,
                                "scheduler received task result"
                            );
                            self.handle_result(&result);
                            self.flush(&action_tx);
                        }
                        None => break,
                    }
                }

                _ = cancel.cancelled() => {
                    tracing::trace!("scheduler received cancel signal");
                    break;
                }
            }
        }
    }

    /// Translate FSM state changes into additions/removals in the `desired` set.
    pub fn update_desired(&mut self, changes: &[StateChange]) {
        for change in changes {
            tracing::trace!(change = ?change, "scheduler applying state change");

            match change {
                StateChange::EpochAdvanced { epoch } => self.handle_epoch_advanced(*epoch),
                StateChange::SpoolAssignmentChanged => self.handle_spool_assignment_changed(),
                StateChange::TrackCertified { track } => self.handle_track_certified(track),
                StateChange::NodeJoinedCommittee { node } => self.handle_node_joined(node),
                StateChange::NodeSynced { node } => self.handle_node_synced(node),

                StateChange::TrackDeleted { track }
                | StateChange::TrackInvalidated { track } => {
                    tracing::trace!(track = %track, "scheduler removing recoveries for deleted/invalidated track");
                    SpoolPlanner::remove_recoveries(&*self.context.store, track);
                }

                StateChange::PhaseAdvanced { phase } => self.handle_phase_advanced(*phase),
                StateChange::PoolAdvanced { node } => self.handle_pool_advanced(node),

                StateChange::TrackRegistered { .. }
                | StateChange::TapeReserved { .. }
                | StateChange::TapeDestroyed { .. }
                | StateChange::NodeRegistered { .. } => {}
            }
        }
    }

    fn bootstrap_desired(&mut self) {
        let epoch = self.context.state().epoch;
        if epoch.is_zero() {
            return;
        }

        self.rebuild_epoch_desired(epoch, false);
    }

    fn rebuild_epoch_desired(&mut self, epoch: EpochNumber, reset_planners: bool) {
        if reset_planners {
            self.lifecycle.state_mut().reset(epoch);
            self.snapshot.progress_mut().reset(epoch);
        }

        self.plan_spool_tasks();
        SpoolPlanner::prune_recoveries(&*self.context.store, &mut self.desired);
        self.schedule_snapshot_bootstrap_if_needed();
        self.lifecycle.schedule(
            self.chain_phase(),
            self.node_status(),
            epoch,
            &mut self.desired,
        );
    }

    fn plan_spool_tasks(&mut self) {
        SpoolPlanner::plan_spool_tasks(
            &*self.context.store,
            self.node_status(),
            &mut self.desired,
        );
    }

    fn schedule_snapshot_bootstrap_if_needed(&mut self) {
        if self.needs_bootstrap() {
            self.desired.insert(Task::SnapshotBootstrap);
        }
    }

    fn handle_epoch_advanced(&mut self, epoch: EpochNumber) {
        tracing::trace!(epoch = epoch.0, "scheduler handling epoch advanced");

        self.scheduled
            .retain(|key| !matches!(key.scheduled_epoch(), Some(e) if e != epoch));

        self.rebuild_epoch_desired(epoch, true);
    }

    fn handle_spool_assignment_changed(&mut self) {
        tracing::trace!("scheduler planning spool tasks after assignment change");
        self.plan_spool_tasks();
    }

    fn handle_track_certified(&mut self, track: &Pubkey) {
        tracing::trace!(track = %track, "scheduler checking slices after track certified");
        SpoolPlanner::check_slices(
            &*self.context.store,
            self.node_status(),
            track,
            &mut self.desired,
        );
    }

    fn handle_phase_advanced(&mut self, phase: EpochPhase) {
        tracing::trace!(?phase, "scheduler handling phase advance");
        let epoch = self.context.state().epoch;
        if epoch.is_zero() {
            return;
        }

        self.lifecycle.schedule(
            Some(phase),
            self.node_status(),
            epoch,
            &mut self.desired,
        );
    }

    fn handle_node_joined(&mut self, node: &Pubkey) {
        if *node != self.context.pubkey() {
            return;
        }

        tracing::trace!(node = %node, "scheduler handling join event for local node");
        self.plan_spool_tasks();
    }

    fn handle_node_synced(&mut self, node: &Pubkey) {
        if *node != self.context.pubkey() {
            return;
        }

        tracing::trace!(node = %node, "scheduler dropping local sync task after local node synced");
        self.desired
            .retain(|key| !matches!(key, Task::SyncEpoch { .. }));
    }

    fn handle_pool_advanced(&mut self, node: &Pubkey) {
        if *node != self.context.pubkey() {
            return;
        }

        tracing::trace!(node = %node, "scheduler dropping local advance task after local node advanced");
        self.desired
            .retain(|key| !matches!(key, Task::AdvancePool { .. }));
    }

    /// Process a task completion from the task_runner. Stale epoch results are
    /// silently dropped. Otherwise delegates to type-specific handlers.
    pub fn handle_result(&mut self, result: &TaskResult) {
        tracing::trace!(result = ?result, "processing scheduler task result");
        let key = match result {
            TaskResult::Success(k) => k,
            TaskResult::Canceled(k) => k,
            TaskResult::RetryableError(k, _) => k,
            TaskResult::PermanentError(k, _) => k,
        };

        if self.is_stale_epoch(key) {
            tracing::trace!(task = ?key, "dropping stale task result");
            self.scheduled.remove(key);
            return;
        }

        match result {
            TaskResult::Success(_) => self.handle_success(key),
            TaskResult::Canceled(_) => self.handle_cancelled(key),
            TaskResult::RetryableError(_, _) => self.handle_retry(),
            TaskResult::PermanentError(_, _) => self.handle_permanent(key),
        }
    }

    /// Task was canceled — remove from scheduled so it can be re-added if needed.
    fn handle_cancelled(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "scheduler removing canceled task from scheduled set");
        self.scheduled.remove(key);
    }

    /// Task succeeded. Marks lifecycle state, removes from desired, and
    /// triggers follow-up scheduling based on task type.
    fn handle_success(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "scheduler handling task success");

        self.scheduled.remove(key);
        self.desired.remove(key);
        self.lifecycle.state_mut().mark_done(key);

        match key {
            Task::SyncEpoch { .. } | Task::AdvancePool { .. } | Task::SnapshotBootstrap => {
                if matches!(key, Task::SnapshotBootstrap) {
                    SpoolPlanner::plan_spool_tasks(
                        &*self.context.store,
                        self.node_status(),
                        &mut self.desired,
                    );
                }
                self.reschedule_lifecycle();
            }
            Task::SpoolSync { .. } => {
                SpoolPlanner::plan_spool_tasks(
                    &*self.context.store,
                    self.node_status(),
                    &mut self.desired,
                );
            }
            Task::RecoveryScan { .. } => {
                SpoolPlanner::plan_spool_tasks(
                    &*self.context.store,
                    self.node_status(),
                    &mut self.desired,
                );
            }
            _ => {}
        }

        // Advance snapshot pipeline.
        let chain_phase_active = self.is_onchain_phase_active();
        self.snapshot.on_success(
            &self.context,
            key,
            &mut self.desired,
            &mut self.scheduled,
            &self.lifecycle.state,
            chain_phase_active,
        );
    }

    /// Re-run lifecycle scheduling from current chain state.
    fn reschedule_lifecycle(&mut self) {
        let state = self.context.state();
        if !state.epoch.is_zero() {
            self.lifecycle.schedule(
                self.chain_phase(),
                self.node_status(),
                state.epoch,
                &mut self.desired,
            );
        }
    }

    /// Current chain phase from protocol state. Returns None for Unknown.
    fn chain_phase(&self) -> Option<EpochPhase> {
        let state = self.context.state();
        match state.phase {
            EpochPhase::Unknown => None,
            phase => Some(phase),
        }
    }

    /// Read the node's current status derived from committee membership.
    pub fn node_status(&self) -> NodeStatus {
        self.context.node_status()
    }

    /// Whether the on-chain epoch phase is Active (all nodes synced/settled).
    fn is_onchain_phase_active(&self) -> bool {
        matches!(self.context.state().phase, EpochPhase::Active)
    }

    /// True if the task's epoch doesn't match the current chain epoch (the task
    /// was for a previous epoch that has since advanced).
    fn is_stale_epoch(&self, key: &Task) -> bool {
        let Some(task_epoch) = key.scheduled_epoch() else {
            return false;
        };
        let state = self.context.state();
        if state.epoch.is_zero() {
            return true;
        }
        task_epoch != state.epoch
    }

    /// True if this node is Active, at epoch >= 2, and has no sync cursor yet
    /// (meaning it needs to bootstrap state from a snapshot before syncing).
    fn needs_bootstrap(&self) -> bool {
        if !matches!(self.node_status(), NodeStatus::Active) {
            return false;
        }
        let current_epoch = self.context.state().epoch;
        let sync_cursor = self.context.store.get_sync_cursor().ok().flatten();
        current_epoch >= EpochNumber(2) && sync_cursor.is_none()
    }

    fn should_drop_epoch_task(&self, current_epoch: EpochNumber, task_epoch: EpochNumber) -> bool {
        task_epoch.0.saturating_add(EPOCH_TASK_RETENTION_WINDOW) < current_epoch.0
    }

    fn task_below_retention(&self, current_epoch: EpochNumber, key: &Task) -> bool {
        match key.scheduled_epoch() {
            Some(task_epoch) => self.should_drop_epoch_task(current_epoch, task_epoch),
            None => false,
        }
    }

    /// Diff `desired` vs `scheduled` and send Schedule/Cancel actions.
    ///
    /// Uses `try_send` so we never block waiting for the task_runner to drain
    /// actions — if the channel is full we break and let the unsent items
    /// be picked up on the next pass (they remain in `desired \ scheduled`).
    /// This prevents a bidirectional deadlock where the scheduler blocks on
    /// action sends while the task_runner blocks on result sends.
    pub fn flush(&mut self, tx: &mpsc::Sender<Action>) {
        self.prune_stale(tx);

        let desired_count = self.desired.len();
        let scheduled_count = self.scheduled.len();

        let (sent, send_fail) = self.send_schedules(tx);
        let (cancel_sent, cancel_fail) = self.send_cancels(tx);

        tracing::trace!(
            desired = desired_count,
            scheduled = scheduled_count,
            sent,
            send_fail,
            cancel_sent,
            cancel_fail,
            "flush summary"
        );
    }

    /// Remove epoch-scoped tasks older than the retention window from both
    /// `desired` and `scheduled`. Sends Cancel actions for scheduled ones.
    fn prune_stale(&mut self, tx: &mpsc::Sender<Action>) {
        let current_epoch = self.context.state().epoch;
        if current_epoch.is_zero() {
            return;
        }

        let stale_scheduled: Vec<_> = self
            .scheduled
            .iter()
            .filter(|key| self.task_below_retention(current_epoch, key))
            .cloned()
            .collect();

        for key in stale_scheduled {
            tracing::trace!(task = ?key, "cancelling stale scheduled task");
            match tx.try_send(Action::Cancel(key.clone())) {
                Ok(()) => {
                    self.scheduled.remove(&key);
                    self.desired.remove(&key);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(task = ?key, "action channel full, deferring stale cancel");
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return,
            }
        }

        let stale_desired: Vec<_> = self
            .desired
            .iter()
            .filter(|key| self.task_below_retention(current_epoch, key))
            .cloned()
            .collect();
        for key in stale_desired {
            self.desired.remove(&key);
        }
    }

    /// Send Schedule actions for tasks in `desired` but not `scheduled`.
    /// Returns `(sent, deferred)`.
    fn send_schedules(&mut self, tx: &mpsc::Sender<Action>) -> (usize, usize) {
        let to_schedule: Vec<_> = self.desired.difference(&self.scheduled).cloned().collect();
        let total = to_schedule.len();
        let mut sent: usize = 0;

        for key in &to_schedule {
            match tx.try_send(Action::Schedule(key.clone())) {
                Ok(()) => {
                    sent += 1;
                    self.scheduled.insert(key.clone());
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        task = ?key,
                        sent,
                        remaining = total - sent,
                        "action channel full, deferring remaining schedules"
                    );
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }

        (sent, total - sent)
    }

    /// Send Cancel actions for tasks in `scheduled` but not `desired`.
    /// Returns `(sent, deferred)`.
    fn send_cancels(&mut self, tx: &mpsc::Sender<Action>) -> (usize, usize) {
        let to_cancel: Vec<_> = self.scheduled.difference(&self.desired).cloned().collect();
        let total = to_cancel.len();
        let mut sent: usize = 0;

        for key in &to_cancel {
            match tx.try_send(Action::Cancel(key.clone())) {
                Ok(()) => {
                    sent += 1;
                    self.scheduled.remove(key);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        task = ?key,
                        sent,
                        remaining = total - sent,
                        "action channel full, deferring remaining cancels"
                    );
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }

        (sent, total - sent)
    }

    /// No-op: the task_runner handles retries internally. The key stays in
    /// `scheduled` so the scheduler doesn't re-issue a duplicate Schedule action.
    fn handle_retry(&self) {
        tracing::trace!("scheduler received retryable result");
    }

    /// Task failed permanently — remove from both sets so it is never retried.
    /// For SpoolSync failures, transitions the spool to recovery and re-plans.
    fn handle_permanent(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "scheduler dropping permanent failure task");
        self.scheduled.remove(key);
        self.desired.remove(key);

        if let Task::SpoolSync { spool } = key {
            SpoolPlanner::transition_to_scan(&*self.context.store, *spool);
        }
        if key.spool_id().is_some() {
            SpoolPlanner::plan_spool_tasks(&*self.context.store, self.node_status(), &mut self.desired);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::sync::Arc;

    use bytemuck::Zeroable;
    use rpc::Rpc;
    use tape_protocol::Api;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signer::Signer;
    use store::Store;
    use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_COUNT};
    use tape_core::bls::BlsSignature;
    use tape_core::snapshot::{ReplayableEvent, SnapshotEntry, SnapshotLog};
    use tape_core::spooler::{SpoolAssignment, SpoolGroup};
    use tape_core::system::{CommitteeMember, EpochPhase};
    use tape_core::types::{EpochNumber, NodeId, SlotNumber};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_crypto::Hash as CryptoHash;
    use tape_protocol::state::ProtocolState;
    use tokio::sync::mpsc;
    use tape_store::ops::{MetaOps, ObjectInfoOps, SliceOps, SpoolOps, TrackOps};
    use tape_store::types::{
        NodeStatus,
        ObjectInfo,
        Pubkey as StorePubkey,
        SnapshotCertResult,
        SnapshotChunkMeta,
        SpoolState, SpoolStatus,
        TrackInfo,
    };
    use crate::fsm::{Fsm, StateChange};
    use crate::core::NodeContext;
    use crate::core::test_utils::test_context;
    use crate::{Task, TaskResult};

    fn seed_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
        ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
        epoch: EpochNumber,
        phase: EpochPhase,
        status: NodeStatus,
    ) {
        let mut state = ProtocolState {
            epoch,
            phase,
            spools: SpoolAssignment::new([255u8; SPOOL_COUNT]),
            ..Default::default()
        };
        if matches!(status, NodeStatus::Active) {
            state.committee.push(CommitteeMember::new(ctx.node_id(), Coin::<TAPE>::new(1000)));
        }
        ctx.set_state(state);
    }

    fn mark_snapshot_build_complete<Db: Store, Cluster: Api, Blockchain: Rpc>(
        ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
        local_epoch: EpochNumber,
    ) {
        for group in 0..SPOOL_GROUP_COUNT {
            let chunk_index = tape_store::types::ChunkIndex(group as u64);
            ctx.store
                .set_snapshot_commitment(local_epoch, chunk_index, CryptoHash::new_unique())
                .unwrap();
            ctx.store
                .set_snapshot_metadata(
                    local_epoch,
                    chunk_index,
                    SnapshotChunkMeta {
                        leaves: Vec::new(),
                        stripe_size: 0,
                        stripe_count: 0,
                        encoding_type: 0,
                        encoding_params: 0,
                    },
                )
                .unwrap();
        }
    }

    fn mark_snapshot_group_ready<Db: Store, Cluster: Api, Blockchain: Rpc>(
        ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
        local_epoch: EpochNumber,
        group: u64,
    ) {
        let chunk_index = tape_store::types::ChunkIndex(group);

        ctx.store
            .set_snapshot_commitment(local_epoch, chunk_index, CryptoHash::new_unique())
            .unwrap();

        ctx.store
            .set_snapshot_metadata(
                local_epoch,
                chunk_index,
                SnapshotChunkMeta {
                    leaves: Vec::new(),
                    stripe_size: 0,
                    stripe_count: 0,
                    encoding_type: 0,
                    encoding_params: 0,
                },
            )
            .unwrap();
    }

    fn put_our_committee<Db: Store, Cluster: Api, Blockchain: Rpc>(
        ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
        epoch: EpochNumber,
        spools: Vec<u16>,
    ) {
        let mut spool_map = [255u8; SPOOL_COUNT];
        for &s in &spools {
            spool_map[s as usize] = 0;
        }
        ctx.set_state(ProtocolState {
            epoch,
            committee: vec![CommitteeMember::new(ctx.node_id(), Coin::<TAPE>::new(1000))],
            spools: SpoolAssignment::new(spool_map),
            ..Default::default()
        });
    }

    fn put_non_our_committee<Db: Store, Cluster: Api, Blockchain: Rpc>(
        ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
        epoch: EpochNumber,
        spools: Vec<u16>,
    ) {
        let mut spool_map = [255u8; SPOOL_COUNT];
        for &s in &spools {
            spool_map[s as usize] = 0;
        }
        ctx.set_state(ProtocolState {
            epoch,
            committee: vec![CommitteeMember::new(NodeId(999), Coin::<TAPE>::new(1000))],
            spools: SpoolAssignment::new(spool_map),
            ..Default::default()
        });
    }

    fn active_spool(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Active, epoch)
    }

    fn sync_spool(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Sync, epoch)
    }

    fn scan_spool(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Scan, epoch)
    }

    fn recover_spool(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::Recover, epoch)
    }

    fn locked_spool(epoch: EpochNumber) -> SpoolState {
        SpoolState::new(SpoolStatus::LockedToMove, epoch)
    }

    #[tokio::test]
    async fn epoch_advance() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(10, sync_spool(EpochNumber(0)))
            .unwrap();
        ctx.store
            .set_spool_state(20, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolSync { spool: 10 }));
        assert!(scheduled.contains(&Task::SpoolSync { spool: 20 }));
        assert!(scheduled.contains(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&Task::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&Task::JoinNetwork { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn spool_removed() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(10, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        while action_rx.try_recv().is_ok() {}

        ctx.store.remove_spool_state(10).unwrap();

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(2),
        }]);
        scheduler.flush(&action_tx);

        let mut cancelled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Cancel(key) = d {
                cancelled.insert(key);
            }
        }

        assert!(cancelled.contains(&Task::SpoolSync { spool: 10 }));
    }

    #[tokio::test]
    async fn success_cleared() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        let mut scheduler = TaskScheduler::new(ctx.clone());

        let key = Task::SpoolSync { spool: 42 };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler.handle_result(&TaskResult::Success(key.clone()));

        assert!(!scheduler.desired.contains(&key));
        assert!(!scheduler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn success_no_reschedule() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        let mut scheduler = TaskScheduler::new(ctx.clone());
        let (action_tx, mut action_rx) = mpsc::channel(16);

        let key = Task::SpoolSync { spool: 42 };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler.handle_result(&TaskResult::Success(key));
        scheduler.flush(&action_tx);

        assert!(action_rx.try_recv().is_err(), "no actions after success + flush");
    }

    #[tokio::test]
    async fn retryable_kept() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        let mut scheduler = TaskScheduler::new(ctx.clone());

        let key = Task::AdvanceEpoch { epoch: EpochNumber(1) };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler
            .handle_result(&TaskResult::RetryableError(key.clone(), "transient".into()));

        assert!(scheduler.desired.contains(&key));
        assert!(scheduler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn permanent_removed() {
        let ctx = test_context();
        let mut scheduler = TaskScheduler::new(ctx);

        let key = Task::SpoolSync { spool: 42 };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler
            .handle_result(&TaskResult::PermanentError(key.clone(), "fatal".into()));

        assert!(!scheduler.desired.contains(&key));
        assert!(!scheduler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn active_recover() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(30, recover_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolRecovery { spool: 30 }));
    }

    #[tokio::test]
    async fn spool_changed() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(15, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::SpoolAssignmentChanged]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolSync { spool: 15 }));
    }

    fn make_track_info(spool_group: u64) -> TrackInfo {
        TrackInfo {
            tape_address: StorePubkey([0u8; 32]),
            spool_group: SpoolGroup(spool_group),
            original_size: 1024,
            stripe_size: 512,
            stripe_count: 2,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }
    }

    #[tokio::test]
    async fn cert_missing() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(5, active_spool(EpochNumber(0)))
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolRecovery { spool: 5 }));
    }

    #[tokio::test]
    async fn cert_present() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(5, active_spool(EpochNumber(0)))
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        ctx.store.put_slice(5, store_track, vec![1, 2, 3]).unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&action_tx);

        assert!(action_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn cert_group() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(5, active_spool(EpochNumber(0)))
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(1)).unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&action_tx);

        assert!(action_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn joined_ours() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store.set_spool_state(10, sync_spool(EpochNumber(0))).unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::NodeJoinedCommittee { node: our_pubkey }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolSync { spool: 10 }));
    }

    #[tokio::test]
    async fn joined_other() {
        let ctx = test_context();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::NodeJoinedCommittee {
            node: Pubkey::new_unique(),
        }]);
        scheduler.flush(&action_tx);

        assert!(action_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sync_clears() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();

        let mut scheduler = TaskScheduler::new(ctx);
        let epoch = EpochNumber(0);
        scheduler.desired.insert(Task::SyncEpoch { epoch });
        scheduler.scheduled.insert(Task::SyncEpoch { epoch });

        scheduler.update_desired(&[StateChange::NodeSynced { node: our_pubkey }]);

        assert!(!scheduler.desired.contains(&Task::SyncEpoch { epoch }));
    }

    #[tokio::test]
    async fn closed_action() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(10, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, action_rx) = mpsc::channel(16);

        drop(action_rx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        assert!(scheduler.scheduled.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_trigger() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(3), EpochPhase::Unknown, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch: EpochNumber(3) }]);

        assert!(scheduler.desired.contains(&Task::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_skip() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(3), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_sync_cursor(SlotNumber(500))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch: EpochNumber(3) }]);

        assert!(!scheduler.desired.contains(&Task::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_refresh() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(3), EpochPhase::Unknown, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx);

        scheduler.desired.insert(Task::SnapshotBootstrap);
        scheduler.scheduled.insert(Task::SnapshotBootstrap);

        scheduler.handle_result(&TaskResult::Success(Task::SnapshotBootstrap));

        assert!(!scheduler.desired.contains(&Task::SnapshotBootstrap));
        assert!(scheduler.desired.contains(&Task::SyncEpoch { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn epoch_derive() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_state(10, sync_spool(EpochNumber(0)))
            .unwrap();
        ctx.store
            .set_spool_state(20, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        // 2 SpoolSync + SyncEpoch (AdvancePool/JoinNetwork gated on SyncEpoch)
        assert_eq!(scheduler.desired.len(), 3);
    }

    #[tokio::test]
    async fn schedules_pool() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(2), EpochPhase::Syncing, NodeStatus::Active);
        let epoch = EpochNumber(2);

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        assert!(scheduler.desired.contains(&Task::SyncEpoch { epoch }));
        assert!(!scheduler.desired.contains(&Task::AdvancePool { epoch }));

        ctx.update_phase(EpochPhase::Settling);
        scheduler.desired.insert(Task::SyncEpoch { epoch });
        scheduler.scheduled.insert(Task::SyncEpoch { epoch });
        scheduler.handle_result(&TaskResult::Success(Task::SyncEpoch { epoch }));

        assert!(scheduler.desired.contains(&Task::AdvancePool { epoch }));
    }

    #[tokio::test]
    async fn standby_blocks() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(10, sync_spool(EpochNumber(0)))
            .unwrap();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Standby);

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(!scheduled.contains(&Task::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&Task::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&Task::JoinNetwork { epoch: EpochNumber(1) }));
    }


    #[tokio::test]
    async fn lifecycle_reset() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.lifecycle.state_mut().reset(EpochNumber(3));
        scheduler
            .lifecycle
            .state_mut()
            .mark_done(&Task::SyncEpoch { epoch: EpochNumber(3) });
        assert!(scheduler.lifecycle.state().is_done(&Task::SyncEpoch { epoch: EpochNumber(3) }));

        seed_state(&ctx, EpochNumber(4), EpochPhase::Syncing, NodeStatus::Active);

        scheduler.lifecycle.schedule(
            Some(EpochPhase::Syncing),
            scheduler.node_status(),
            EpochNumber(4),
            &mut scheduler.desired,
        );

        assert_eq!(scheduler.lifecycle.state().epoch(), EpochNumber(4));
        assert!(!scheduler.lifecycle.state().is_done(&Task::SyncEpoch { epoch: EpochNumber(4) }));
        assert!(scheduler.desired.contains(&Task::SyncEpoch { epoch: EpochNumber(4) }));
    }

    #[tokio::test]
    async fn mismatch_resets() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(4), EpochPhase::Active, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.lifecycle.state_mut().reset(EpochNumber(3));
        let old_epoch = EpochNumber(3);
        let new_epoch = EpochNumber(4);
        scheduler
            .scheduled
            .insert(Task::AdvanceEpoch { epoch: old_epoch });
        scheduler
            .desired
            .insert(Task::AdvanceEpoch { epoch: old_epoch });

        scheduler.scheduled.retain(|key| !matches!(key.scheduled_epoch(), Some(e) if e != new_epoch));

        scheduler.lifecycle.schedule(
            Some(EpochPhase::Active),
            scheduler.node_status(),
            new_epoch,
            &mut scheduler.desired,
        );

        let (action_tx, mut action_rx) = mpsc::channel(16);
        scheduler.flush(&action_tx);

        let mut saw_cancel = false;
        let mut saw_schedule = false;
        while let Ok(d) = action_rx.try_recv() {
            match d {
                Action::Cancel(Task::AdvanceEpoch { epoch }) if epoch == old_epoch => {
                    saw_cancel = true
                }
                Action::Schedule(Task::AdvanceEpoch { epoch }) if epoch == new_epoch => {
                    saw_schedule = true
                }
                _ => {}
            }
        }

        assert!(
            !saw_cancel,
            "stale epoch-scoped tasks should be pruned before diffing"
        );
        assert!(saw_schedule, "expected fresh schedule for current epoch");
    }


    #[tokio::test]
    async fn stale_success() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(3), EpochPhase::Syncing, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.state_mut().reset(EpochNumber(3));
        let stale_epoch = EpochNumber(2);
        let current_epoch = EpochNumber(3);
        scheduler
            .desired
            .insert(Task::SyncEpoch { epoch: stale_epoch });
        scheduler
            .scheduled
            .insert(Task::SyncEpoch { epoch: stale_epoch });

        scheduler.handle_result(&TaskResult::Success(Task::SyncEpoch {
            epoch: stale_epoch,
        }));

        assert!(!scheduler.lifecycle.state().is_done(&Task::SyncEpoch { epoch: current_epoch }));
        assert!(scheduler
            .desired
            .contains(&Task::SyncEpoch { epoch: stale_epoch }));
    }

    #[tokio::test]
    async fn default_standby() {
        let ctx = test_context();
        let mut scheduler = TaskScheduler::new(ctx);

        scheduler.context.store.set_spool_state(10, sync_spool(EpochNumber(0))).unwrap();

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        assert!(!scheduler.desired.contains(&Task::SpoolSync { spool: 10 }));
        assert!(!scheduler.desired.contains(&Task::SyncEpoch { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn epoch_plans_spool_tasks() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store.set_spool_state(10, sync_spool(EpochNumber(0))).unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch: EpochNumber(1) }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolSync { spool: 10 }));
    }

    #[tokio::test]
    async fn epoch_lifecycle() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch: EpochNumber(3) }]);

        assert!(scheduler.desired.contains(&Task::SyncEpoch { epoch: EpochNumber(3) }));
        assert!(!scheduler
            .desired
            .contains(&Task::AdvancePool { epoch: EpochNumber(3) }));
        assert!(!scheduler
            .desired
            .contains(&Task::JoinNetwork { epoch: EpochNumber(3) }));
        assert!(scheduler.desired.contains(&Task::SnapshotBuild { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn epoch_build() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        let epoch = EpochNumber(3);

        let mut scheduler = TaskScheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        assert!(scheduler.desired.contains(&Task::SnapshotBuild { epoch }));
    }

    #[tokio::test]
    async fn epoch_skip() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        let epoch = EpochNumber(1);

        let mut scheduler = TaskScheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        assert!(!scheduler.desired.contains(&Task::SnapshotBuild { epoch }));
    }

    #[tokio::test]
    async fn built_certify() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.schedule(
            None,
            scheduler.node_status(),
            epoch,
            &mut scheduler.desired,
        );

        assert!(scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
        assert!(!scheduler.desired.contains(&Task::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
    }

    #[tokio::test]
    async fn built_no_groups() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_non_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.schedule(
            None,
            scheduler.node_status(),
            epoch,
            &mut scheduler.desired,
        );

        assert!(!scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
        assert!(!scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
        assert!(!scheduler.desired.contains(&Task::SnapshotSubmit { epoch }));
    }

    #[tokio::test]
    async fn cert_onchain() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                tape_store::types::ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.schedule(
            None,
            scheduler.node_status(),
            epoch,
            &mut scheduler.desired,
        );

        assert!(scheduler.desired.contains(&Task::SnapshotSubmit { epoch }));
        assert!(scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
        assert!(!scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn partial_onchain() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_our_committee(&ctx, EpochNumber(3), vec![5, 25]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                tape_store::types::ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.schedule(
            None,
            scheduler.node_status(),
            epoch,
            &mut scheduler.desired,
        );

        assert!(scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
        assert!(scheduler.desired.contains(&Task::SnapshotSubmit { epoch }));
        assert!(scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn epoch_rebuild() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);

        let mut scheduler = TaskScheduler::new(ctx);
        let epoch = EpochNumber(3);
        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch }]);

        assert!(!scheduler.desired.contains(&Task::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
        assert!(scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
    }

    #[tokio::test]
    async fn partial_build_register() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        put_our_committee(&ctx, EpochNumber(3), vec![5]);

        let local_epoch = EpochNumber(2);
        let group = 0u64;
        mark_snapshot_group_ready(&ctx, local_epoch, group);
        let epoch = EpochNumber(3);

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.lifecycle.schedule(
            None,
            scheduler.node_status(),
            epoch,
            &mut scheduler.desired,
        );


        assert!(scheduler.desired.contains(&Task::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&Task::RegisterSnapshot { epoch }));
        assert!(scheduler.desired.contains(&Task::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn delete_recovery() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(5, active_spool(EpochNumber(0)))
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();

        ctx.store.add_pending_recovery(5, store_track).unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());

        scheduler.update_desired(&[StateChange::TrackDeleted { track }]);

        let pending = ctx.store.iter_pending_recoveries(5, 100).unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn replay_cancel_recovery() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(2), EpochPhase::Syncing, NodeStatus::Active);
        ctx.store
            .set_spool_state(5, active_spool(EpochNumber(0)))
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        ctx.store.put_object_info(
            store_track,
            ObjectInfo::Valid {
                is_stored: false,
                track_address: store_track,
                registered_epoch: EpochNumber(1),
                certified_epoch: None,
                slot: SlotNumber(1),
            },
        ).unwrap();
        ctx.store.add_pending_recovery(5, store_track).unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.desired.insert(Task::SpoolRecovery { spool: 5 });
        scheduler.scheduled.insert(Task::SpoolRecovery { spool: 5 });

        let fsm = Fsm::new(ctx.clone());
        let log = SnapshotLog {
            version: 1,
            epoch: EpochNumber(2),
            start_slot: SlotNumber(10),
            end_slot: SlotNumber(10),
            entries: vec![SnapshotEntry {
                slot: SlotNumber(10),
                events: vec![ReplayableEvent::DeleteTrack {
                    track: track.to_bytes(),
                    epoch: EpochNumber(2),
                }],
            }],
        };
        fsm.replay_snapshot(&log).unwrap();

        scheduler.update_desired(&[StateChange::EpochAdvanced { epoch: EpochNumber(2) }]);

        let (action_tx, mut action_rx) = mpsc::channel(32);
        scheduler.flush(&action_tx);

        let mut saw_cancel = false;
        let mut saw_schedule = false;
        while let Ok(dir) = action_rx.try_recv() {
            match dir {
                Action::Cancel(Task::SpoolRecovery { spool }) if spool == 5 => saw_cancel = true,
                Action::Schedule(Task::SpoolRecovery { spool }) if spool == 5 => {
                    saw_schedule = true
                }
                _ => {}
            }
        }

        assert!(saw_cancel);
        assert!(!saw_schedule);
    }

    #[tokio::test]
    async fn sync_permanent_to_scan() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(42, sync_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.desired.insert(Task::SpoolSync { spool: 42 });
        scheduler.scheduled.insert(Task::SpoolSync { spool: 42 });

        scheduler.handle_result(&TaskResult::PermanentError(
            Task::SpoolSync { spool: 42 },
            "peer unreachable".into(),
        ));

        assert!(matches!(
            ctx.store.get_spool_state(42).unwrap().unwrap(),
            s if s.is_scanning() && s.epoch == EpochNumber(0)
        ));
        assert!(scheduler.desired.contains(&Task::RecoveryScan { spool: 42 }));
        assert!(!scheduler.desired.contains(&Task::SpoolSync { spool: 42 }));
    }

    #[tokio::test]
    async fn sync_success_replans_scan() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(42, scan_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.desired.insert(Task::SpoolSync { spool: 42 });
        scheduler.scheduled.insert(Task::SpoolSync { spool: 42 });

        scheduler.handle_result(&TaskResult::Success(Task::SpoolSync { spool: 42 }));

        assert!(scheduler.desired.contains(&Task::RecoveryScan { spool: 42 }));
        assert!(!scheduler.desired.contains(&Task::SpoolSync { spool: 42 }));
    }

    #[tokio::test]
    async fn recovery_permanent_reschedules() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(42, recover_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.desired.insert(Task::SpoolRecovery { spool: 42 });
        scheduler.scheduled.insert(Task::SpoolRecovery { spool: 42 });

        scheduler.handle_result(&TaskResult::PermanentError(
            Task::SpoolRecovery { spool: 42 },
            "exhausted retries".into(),
        ));

        assert!(matches!(
            ctx.store.get_spool_state(42).unwrap().unwrap(),
            s if s.is_recovering() && s.epoch == EpochNumber(0)
        ));
        // Planner re-adds recovery for Recover state
        assert!(scheduler.desired.contains(&Task::SpoolRecovery { spool: 42 }));
    }

    #[tokio::test]
    async fn scan_permanent_reschedules() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(42, scan_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        scheduler.desired.insert(Task::RecoveryScan { spool: 42 });
        scheduler.scheduled.insert(Task::RecoveryScan { spool: 42 });

        scheduler.handle_result(&TaskResult::PermanentError(
            Task::RecoveryScan { spool: 42 },
            "exhausted retries".into(),
        ));

        assert!(matches!(
            ctx.store.get_spool_state(42).unwrap().unwrap(),
            s if s.is_scanning() && s.epoch == EpochNumber(0)
        ));
        // Planner re-adds scan for Scan state
        assert!(scheduler.desired.contains(&Task::RecoveryScan { spool: 42 }));
    }

    #[tokio::test]
    async fn plan_active_recover() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(30, recover_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::SpoolRecovery { spool: 30 }));
        assert!(!scheduled.contains(&Task::RecoveryScan { spool: 30 }));
    }

    #[tokio::test]
    async fn plan_scan_schedules_scan() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store
            .set_spool_state(30, scan_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = action_rx.try_recv() {
            if let Action::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&Task::RecoveryScan { spool: 30 }));
        assert!(!scheduled.contains(&Task::SpoolRecovery { spool: 30 }));
    }

    #[tokio::test]
    async fn scan_success_replans() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(1), EpochPhase::Unknown, NodeStatus::Active);
        // After scan completes, spool transitions to Recover (done by spool_scan task).
        ctx.store
            .set_spool_state(30, recover_spool(EpochNumber(0)))
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx);
        scheduler.desired.insert(Task::RecoveryScan { spool: 30 });
        scheduler.scheduled.insert(Task::RecoveryScan { spool: 30 });

        scheduler.handle_result(&TaskResult::Success(Task::RecoveryScan { spool: 30 }));

        // Planner picks up the new Recover state and schedules SpoolRecovery.
        assert!(scheduler.desired.contains(&Task::SpoolRecovery { spool: 30 }));
        assert!(!scheduler.desired.contains(&Task::RecoveryScan { spool: 30 }));
    }

    #[tokio::test]
    async fn cleanup_respects_epoch() {
        let ctx = test_context();
        // Lock spool at epoch 3
        ctx.store
            .set_spool_state(10, locked_spool(EpochNumber(3)))
            .unwrap();

        // At epoch 6, should NOT be cleaned (3 + 4 > 6)
        SpoolPlanner::cleanup_locked(&*ctx.store, EpochNumber(6));
        assert!(ctx.store.get_spool_state(10).unwrap().is_some());

        // At epoch 7, should be cleaned (3 + 4 <= 7)
        SpoolPlanner::cleanup_locked(&*ctx.store, EpochNumber(7));
        assert!(ctx.store.get_spool_state(10).unwrap().is_none());
    }

    #[tokio::test]
    async fn cleanup_deletes_slices() {
        let ctx = test_context();

        // Lock spool 10 at epoch 3
        ctx.store
            .set_spool_state(10, locked_spool(EpochNumber(3)))
            .unwrap();

        // Seed slice data and pending recoveries for spool 10
        let track = StorePubkey::new_unique();
        ctx.store.put_slice(10, track, vec![0xAB; 64]).unwrap();
        ctx.store.add_pending_recovery(10, track).unwrap();

        // Cleanup at epoch 7 (3 + 4 <= 7)
        SpoolPlanner::cleanup_locked(&*ctx.store, EpochNumber(7));

        assert!(ctx.store.get_spool_state(10).unwrap().is_none());
        assert_eq!(ctx.store.count_slices_by_spool(10).unwrap(), 0);
        assert!(ctx.store.iter_pending_recoveries(10, 100).unwrap().is_empty());
    }

    #[tokio::test]
    async fn ownership_sets_epoch() {
        let ctx = test_context();
        let mut chain_spools = HashSet::new();
        chain_spools.insert(10u16);
        chain_spools.insert(20u16);

        let changed = SpoolPlanner::apply_ownership_changes(
            &*ctx.store,
            &chain_spools,
            EpochNumber(5),
            &SpoolAssignment::zeroed(),
            &[],
        );
        assert!(changed);

        let s10 = ctx.store.get_spool_state(10).unwrap().unwrap();
        assert!(s10.is_syncing() && s10.epoch == EpochNumber(5));

        let s20 = ctx.store.get_spool_state(20).unwrap().unwrap();
        assert!(s20.is_syncing() && s20.epoch == EpochNumber(5));
    }

    #[tokio::test]
    async fn ownership_sets_prev_owner() {
        let ctx = test_context();
        let mut chain_spools = HashSet::new();
        chain_spools.insert(10u16);

        let mut prev_map = [255u8; SPOOL_COUNT];
        prev_map[10] = 0;
        let prev_spools = SpoolAssignment::new(prev_map);
        let prev_committee = vec![CommitteeMember::new(NodeId(99), Coin::<TAPE>::new(1000))];

        let changed = SpoolPlanner::apply_ownership_changes(
            &*ctx.store,
            &chain_spools,
            EpochNumber(5),
            &prev_spools,
            &prev_committee,
        );
        assert!(changed);

        let state = ctx.store.get_spool_state(10).unwrap().unwrap();
        assert!(state.is_syncing());
        assert_eq!(state.prev_owner, Some(NodeId(99)));
    }

    #[tokio::test]
    async fn ownership_reactivates_locked() {
        let ctx = test_context();
        // Spool 10 locked at epoch 3
        ctx.store
            .set_spool_state(10, locked_spool(EpochNumber(3)))
            .unwrap();

        // Chain says we own spool 10 again at epoch 5.
        // Previous epoch had us (member 0) owning spool 10.
        let mut chain_spools = HashSet::new();
        chain_spools.insert(10u16);

        let mut prev_map = [255u8; SPOOL_COUNT];
        prev_map[10] = 0;
        let prev_spools = SpoolAssignment::new(prev_map);
        let prev_committee = vec![CommitteeMember::new(ctx.node_id(), Coin::<TAPE>::new(1000))];

        let changed = SpoolPlanner::apply_ownership_changes(
            &*ctx.store,
            &chain_spools,
            EpochNumber(5),
            &prev_spools,
            &prev_committee,
        );
        assert!(changed);

        let s10 = ctx.store.get_spool_state(10).unwrap().unwrap();
        assert!(s10.is_syncing());
        assert_eq!(s10.epoch, EpochNumber(5));
        assert_eq!(s10.prev_owner, Some(ctx.node_id()));
    }

    #[tokio::test]
    async fn ownership_skips_existing_lock() {
        let ctx = test_context();
        // Spool 10 locked at epoch 3
        ctx.store
            .set_spool_state(10, locked_spool(EpochNumber(3)))
            .unwrap();

        // Chain does not include spool 10 at epoch 5
        let chain_spools = HashSet::new();

        let changed = SpoolPlanner::apply_ownership_changes(
            &*ctx.store,
            &chain_spools,
            EpochNumber(5),
            &SpoolAssignment::zeroed(),
            &[],
        );
        // No change — spool already locked
        assert!(!changed);

        let s10 = ctx.store.get_spool_state(10).unwrap().unwrap();
        assert!(s10.is_locked());
        assert_eq!(s10.epoch, EpochNumber(3));
    }
}
