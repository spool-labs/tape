//! TaskScheduler — diffs desired vs running tasks based on FSM state changes.
//!
//! The scheduler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the task_runner. It maintains a view of what tasks *should*
//! be running and tells the task_runner to schedule or cancel tasks accordingly.

use std::collections::HashSet;
use std::sync::Arc;

use rpc::Rpc;
use solana_sdk::signer::Signer;
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
pub struct TaskScheduler<S: Store, R: Rpc> {
    /// Shared node state (store, RPC client, identity, config).
    pub context: Arc<NodeContext<S, R>>,
    /// Tasks that SHOULD be running given current state.
    pub desired: HashSet<Task>,
    /// Tasks we've told the task_runner to schedule (and haven't completed/cancelled).
    pub scheduled: HashSet<Task>,
    /// Epoch-scoped lifecycle task planner.
    pub lifecycle: LifecyclePlanner,
    /// Snapshot pipeline planner.
    pub snapshot: SnapshotPlanner,
}

impl<S: Store, R: Rpc> TaskScheduler<S, R> {
    pub fn new(context: Arc<NodeContext<S, R>>) -> Self {
        Self {
            context,
            desired: HashSet::new(),
            scheduled: HashSet::new(),
            lifecycle: LifecyclePlanner::new(),
            snapshot: SnapshotPlanner::new(),
        }
    }

    /// Main event loop. Selects over FSM state changes, task_runner task results,
    /// and a periodic timer. Each event recomputes `desired` and emits the diff
    /// as Schedule/Cancel actions to the task_runner.
    pub async fn run(
        mut self,
        mut change_rx: mpsc::Receiver<Vec<StateChange>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        action_tx: mpsc::Sender<Action>,
        cancel: CancellationToken,
    ) {

        // On startup, reconcile spools and schedule lifecycle from current ChainState
        // (seeded by runtime before scheduler starts).
        {
            let cs = self.context.chain_state.load();
            if cs.has_epoch() {
                SpoolPlanner::reconcile(
                    &*self.context.store,
                    self.node_status(),
                    &mut self.desired,
                );
                self.lifecycle.schedule(
                    self.chain_phase(),
                    self.node_status(),
                    cs.epoch,
                    &mut self.desired,
                );
                if self.needs_bootstrap() {
                    self.desired.insert(Task::SnapshotBootstrap);
                }
            }
        }

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
                StateChange::EpochAdvanced { epoch } => {
                    tracing::trace!(epoch = epoch.0, "scheduler handling epoch advanced");

                    let cs = self.context.chain_state.load();
                    LifecyclePlanner::log_member_index(
                        &cs.committee,
                        self.context.keypair.pubkey(),
                        *epoch,
                    );

                    self.lifecycle.state_mut().reset(*epoch);
                    self.snapshot.progress_mut().reset(*epoch);

                    tracing::trace!(epoch = epoch.0, "scheduler reconciling spools after epoch advance");

                    SpoolPlanner::reconcile(
                        &*self.context.store,
                        self.node_status(),
                        &mut self.desired,
                    );

                    SpoolPlanner::prune_recoveries(&*self.context.store, &mut self.desired);

                    if self.needs_bootstrap() {
                        self.desired.insert(Task::SnapshotBootstrap);
                    }

                    tracing::trace!(epoch = epoch.0, "scheduler scheduling lifecycle for new epoch");

                    self.lifecycle.schedule(
                        self.chain_phase(),
                        self.node_status(),
                        *epoch,
                        &mut self.desired,
                    );
                }

                StateChange::SpoolAssignmentChanged => {
                    tracing::trace!("scheduler reconciling spools after spool assignment change");
                    SpoolPlanner::reconcile(
                        &*self.context.store,
                        self.node_status(),
                        &mut self.desired,
                    );
                }

                StateChange::TrackCertified { track } => {
                    tracing::trace!(track = %track, "scheduler checking slices after track certified");
                    SpoolPlanner::check_slices(
                        &*self.context.store,
                        self.node_status(),
                        track,
                        &mut self.desired,
                    );
                }

                StateChange::NodeJoinedCommittee { node } => {
                    if *node == self.context.keypair.pubkey() {
                        tracing::trace!(node = %node, "scheduler handling join event for local node");
                        SpoolPlanner::reconcile(
                            &*self.context.store,
                            self.node_status(),
                            &mut self.desired,
                        );
                    }
                }

                StateChange::NodeSynced { node } => {
                    if *node == self.context.keypair.pubkey() {
                        tracing::trace!(node = %node, "scheduler dropping local sync task after local node synced");
                        self.desired
                            .retain(|key| !matches!(key, Task::SyncEpoch { .. }));
                    }
                }

                StateChange::TrackDeleted { track }
                | StateChange::TrackInvalidated { track } => {
                    tracing::trace!(track = %track, "scheduler removing recoveries for deleted/invalidated track");
                    SpoolPlanner::remove_recoveries(&*self.context.store, track);
                }
                
                StateChange::PhaseAdvanced { phase } => {
                    tracing::trace!(?phase, "scheduler handling phase advance");
                    if matches!(phase, EpochPhase::Settling) {
                        SpoolPlanner::cleanup_locked(&*self.context.store);
                    }
                    let cs = self.context.chain_state.load();
                    let epoch = cs.epoch;
                    if !epoch.is_zero() {
                        self.lifecycle.schedule(
                            Some(*phase),
                            self.node_status(),
                            epoch,
                            &mut self.desired,
                        );
                    }
                }

                StateChange::PoolAdvanced { node } => {
                    if *node == self.context.keypair.pubkey() {
                        tracing::trace!(node = %node, "scheduler dropping local advance task after local node advanced");
                        self.desired
                            .retain(|key| !matches!(key, Task::AdvancePool { .. }));
                    }
                }

                StateChange::TrackRegistered { .. }
                | StateChange::TapeReserved { .. }
                | StateChange::TapeDestroyed { .. }
                | StateChange::NodeRegistered { .. } => {}
            }
        }
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

    /// Task succeeded. Marks lifecycle state, removes one-shot tasks from desired,
    /// and triggers follow-up scheduling (refresh → lifecycle, sync → lifecycle,
    /// bootstrap → refresh, snapshot stages).
    fn handle_success(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "scheduler handling task success");

        self.scheduled.remove(key);
        self.lifecycle.state_mut().mark_done(key);
        if key.is_one_shot() {
            self.desired.remove(key);
        }

        self.handle_sync_success(key);
        self.handle_bootstrap_success(key);
        self.handle_snapshot_success(key);
    }

    /// After SyncEpoch succeeds, re-run lifecycle scheduling to unlock the
    /// Settling-phase tasks (AdvancePool, JoinNetwork).
    fn handle_sync_success(&mut self, key: &Task) {
        if !matches!(key, Task::SyncEpoch { .. }) {
            return;
        }
        tracing::trace!(task = ?key, "scheduler handling sync success");
        let cs = self.context.chain_state.load();
        if cs.has_epoch() {
            self.lifecycle.schedule(
                self.chain_phase(),
                self.node_status(),
                cs.epoch,
                &mut self.desired,
            );
        }
    }

    /// After bootstrap completes, reconcile spools and schedule lifecycle.
    fn handle_bootstrap_success(&mut self, key: &Task) {
        if !matches!(key, Task::SnapshotBootstrap) {
            return;
        }
        tracing::trace!("scheduler handling snapshot bootstrap success");
        SpoolPlanner::reconcile(
            &*self.context.store,
            self.node_status(),
            &mut self.desired,
        );
        let cs = self.context.chain_state.load();
        if cs.has_epoch() {
            self.lifecycle.schedule(
                self.chain_phase(),
                self.node_status(),
                cs.epoch,
                &mut self.desired,
            );
        }
    }

    /// Advance snapshot pipeline progress when a snapshot stage completes, then
    /// re-run `schedule_snapshot` to unlock the next stage.
    fn handle_snapshot_success(&mut self, key: &Task) {
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

    /// Current chain phase from in-memory ChainState. Returns None for Unknown.
    fn chain_phase(&self) -> Option<EpochPhase> {
        let cs = self.context.chain_state.load();
        match cs.phase {
            EpochPhase::Unknown => None,
            phase => Some(phase),
        }
    }

    /// Read the node's current status from in-memory ChainState.
    pub fn node_status(&self) -> NodeStatus {
        self.context.chain_state.load().node_status.clone()
    }

    /// Whether the on-chain epoch phase is Active (all nodes synced/settled).
    fn is_onchain_phase_active(&self) -> bool {
        matches!(self.context.chain_state.load().phase, EpochPhase::Active)
    }

    /// True if the task's epoch doesn't match the current chain epoch (the task
    /// was for a previous epoch that has since advanced).
    fn is_stale_epoch(&self, key: &Task) -> bool {
        let Some(task_epoch) = key.scheduled_epoch() else {
            return false;
        };
        let cs = self.context.chain_state.load();
        if !cs.has_epoch() {
            return true;
        }
        task_epoch != cs.epoch
    }

    /// True if this node is Active, at epoch >= 2, and has no sync cursor yet
    /// (meaning it needs to bootstrap state from a snapshot before syncing).
    fn needs_bootstrap(&self) -> bool {
        if !matches!(self.node_status(), NodeStatus::Active) {
            return false;
        }
        let current_epoch = self.context.chain_state.load().epoch;
        let sync_cursor = self.context.store.get_sync_cursor().ok().flatten();
        current_epoch >= EpochNumber(2) && sync_cursor.is_none()
    }

    fn should_drop_epoch_task(&self, current_epoch: EpochNumber, task_epoch: EpochNumber) -> bool {
        task_epoch.0.saturating_add(2) < current_epoch.0
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
        let current_epoch = self.context.chain_state.load().epoch;
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
    fn handle_permanent(&mut self, key: &Task) {
        tracing::trace!(task = ?key, "scheduler dropping permanent failure task");
        self.scheduled.remove(key);
        self.desired.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::sync::Arc;

    use bytemuck::Zeroable;
    use rpc::Rpc;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signer::Signer;
    use store::Store;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::snapshot::{ReplayableEvent, SnapshotEntry, SnapshotLog};
    use tape_core::system::EpochPhase;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_core::types::network::NetworkAddress;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_crypto::Hash as CryptoHash;
    use tokio::sync::mpsc;
    use tape_store::ops::{CommitteeOps, MetaOps, ObjectInfoOps, SliceOps, TrackOps};
    use tape_store::types::{
        NodeInfo,
        NodeStatus,
        ObjectInfo,
        Pubkey as StorePubkey,
        SnapshotCertResult,
        SnapshotChunkMeta,
        SpoolStatus,
        TrackInfo,
    };

    use crate::state::ChainState;
    use crate::fsm::{Fsm, StateChange};
    use crate::core::NodeContext;
    use crate::core::test_utils::test_context;
    use crate::{Task, TaskResult};

    fn seed_state<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        phase: EpochPhase,
        status: NodeStatus,
    ) {
        ctx.chain_state.store(ChainState {
            epoch,
            phase,
            nonce: tape_crypto::Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            node_status: status,
            spools: HashSet::new(),
        });
    }

    fn mark_snapshot_build_complete<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
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

    fn mark_snapshot_group_ready<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
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

    fn put_our_committee<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        spools: Vec<u16>,
    ) {
        let (node_address, _) = node_pda(ctx.keypair.pubkey());

        let members = vec![NodeInfo {
            node_address: StorePubkey::new(node_address.to_bytes()),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: StorePubkey::new([0u8; 32]),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
            spools,
        }];

        ctx.store.put_committee(epoch, members).unwrap();
    }

    fn put_non_our_committee<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        spools: Vec<u16>,
    ) {
        let members = vec![NodeInfo {
            node_address: StorePubkey::new([9u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: StorePubkey::new([0u8; 32]),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 9000),
            spools,
        }];
        ctx.store.put_committee(epoch, members).unwrap();
    }

    #[tokio::test]
    async fn epoch_advance() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);

        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
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
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = TaskScheduler::new(ctx.clone());
        let (action_tx, mut action_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&action_tx);

        while action_rx.try_recv().is_ok() {}

        ctx.store.remove_spool_status(10).unwrap();

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
    async fn oneshot_cleared() {
        let ctx = test_context();
        let mut scheduler = TaskScheduler::new(ctx.clone());

        let key = Task::AdvanceEpoch { epoch: EpochNumber(0) };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler.handle_result(&TaskResult::Success(key.clone()));

        assert!(!scheduler.desired.contains(&key));
        assert!(!scheduler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn retryable_kept() {
        let ctx = test_context();
        let mut scheduler = TaskScheduler::new(ctx.clone());

        let key = Task::AdvanceEpoch { epoch: EpochNumber(0) };
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
            .set_spool_status(30, SpoolStatus::ActiveRecover)
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
            .set_spool_status(15, SpoolStatus::ActiveSync)
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
            spool_group,
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
            .set_spool_status(5, SpoolStatus::Active)
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
            .set_spool_status(5, SpoolStatus::Active)
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
            .set_spool_status(5, SpoolStatus::Active)
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
        ctx.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

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
            .set_spool_status(10, SpoolStatus::ActiveSync)
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
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
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

        ctx.chain_state.update_phase(EpochPhase::Settling);
        scheduler.desired.insert(Task::SyncEpoch { epoch });
        scheduler.scheduled.insert(Task::SyncEpoch { epoch });
        scheduler.handle_result(&TaskResult::Success(Task::SyncEpoch { epoch }));

        assert!(scheduler.desired.contains(&Task::AdvancePool { epoch }));
    }

    #[tokio::test]
    async fn standby_blocks() {
        let ctx = test_context();
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
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

        scheduler.context.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        assert!(!scheduler.desired.contains(&Task::SpoolSync { spool: 10 }));
        assert!(!scheduler.desired.contains(&Task::SyncEpoch { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn epoch_reconcile() {
        let ctx = test_context();
        seed_state(&ctx, EpochNumber(0), EpochPhase::Unknown, NodeStatus::Active);
        ctx.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

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
            .set_spool_status(5, SpoolStatus::Active)
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
            .set_spool_status(5, SpoolStatus::Active)
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
}
