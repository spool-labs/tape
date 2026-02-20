//! Scheduler — diffs desired vs running tasks based on FSM state changes.
//!
//! The scheduler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the supervisor. It maintains a view of what tasks *should*
//! be running and tells the supervisor to schedule or cancel tasks accordingly.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use solana_sdk::{pubkey::Pubkey, signer::Signer};
use store::Store;
use tape_api::program::tapedrive::EPOCH_DURATION;
use tape_core::system::EpochPhase;
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use tape_core::erasure::spool_in_group;
use tape_core::types::EpochNumber;
use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::{ChunkIndex, NodeStatus, Pubkey as StorePubkey, SpoolStatus};

use crate::runtime::NodeContext;
use crate::runtime::committee::{our_member_index, our_snapshot_groups};
use crate::fsm::StateChange;
use crate::snapshot::{derive_snapshot_local_epoch, is_snapshot_build_complete, is_snapshot_chunk_ready};
use crate::state::{GroupState, LifecycleEpochState, RefreshThrottle, SnapshotProgress};
use crate::supervisor::{TaskKey, TaskResult};

/// A directive from the scheduler to the supervisor.
#[derive(Debug, Clone)]
pub enum Directive {
    /// Schedule a new task.
    Schedule(TaskKey),
    /// Cancel a running task.
    Cancel(TaskKey),
}

/// Diffs desired state against running tasks to produce scheduling directives.
///
/// Maintains two core sets — `desired` (what SHOULD run) and `scheduled`
/// (what we've told the supervisor to run). Each tick, the diff between them
/// produces Schedule and Cancel directives. State changes from the FSM mutate
/// `desired`; task results from the supervisor remove keys from `scheduled`.
pub struct Scheduler<S: Store, R: Rpc> {
    /// Shared node state (store, RPC client, identity, config).
    context: Arc<NodeContext<S, R>>,
    /// Tasks that SHOULD be running given current state.
    desired: HashSet<TaskKey>,
    /// Tasks we've told the supervisor to schedule (and haven't completed/cancelled).
    scheduled: HashSet<TaskKey>,
    /// Tracks which one-shot lifecycle tasks completed for the current epoch.
    lifecycle: LifecycleEpochState,
    /// In-memory snapshot pipeline state for the current epoch.
    snapshot_progress: SnapshotProgress,
    /// Rate-limits RefreshOnchainState scheduling to avoid RPC spam.
    refresh_throttle: RefreshThrottle,
}

impl<S: Store, R: Rpc> Scheduler<S, R> {
    pub fn new(context: Arc<NodeContext<S, R>>) -> Self {
        Self {
            context,
            desired: HashSet::new(),
            scheduled: HashSet::new(),
            lifecycle: LifecycleEpochState::new(EpochNumber(0)),
            snapshot_progress: SnapshotProgress::new(EpochNumber(0)),
            refresh_throttle: RefreshThrottle::new(),
        }
    }

    /// Main event loop. Selects over FSM state changes, supervisor task results,
    /// and a periodic timer. Each event recomputes `desired` and emits the diff
    /// as Schedule/Cancel directives to the supervisor.
    pub async fn run(
        mut self,
        mut change_rx: mpsc::Receiver<Vec<StateChange>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        directive_tx: mpsc::Sender<Directive>,
        cancel: CancellationToken,
    ) {
        // Bootstrap: schedule RefreshOnchainState immediately on startup
        self.request_refresh(true);
        self.flush(&directive_tx);
        tracing::trace!("scheduler bootstrapped");

        // Refresh often enough to observe committee/epoch transitions in local/test
        // while capping cadence for production.
        let refresh_secs = (EPOCH_DURATION / 2).clamp(1, 30) as u64;
        let mut ticker = interval(Duration::from_secs(refresh_secs));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

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
                            self.flush(&directive_tx);
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
                            self.flush(&directive_tx);
                        }
                        None => break,
                    }
                }

                _ = ticker.tick() => {
                    tracing::trace!(
                        received_changes,
                        handled_results,
                        desired = self.desired.len(),
                        scheduled = self.scheduled.len(),
                        "scheduler periodic tick"
                    );
                    self.periodic_tasks();
                    self.flush(&directive_tx);
                }

                _ = cancel.cancelled() => {
                    tracing::trace!("scheduler received cancel signal");
                    break;
                }
            }
        }
    }

    /// Translate FSM state changes into additions/removals in the `desired` set.
    fn update_desired(&mut self, changes: &[StateChange]) {
        for change in changes {
            tracing::trace!(change = ?change, "scheduler applying state change");
            match change {
                StateChange::EpochAdvanced { epoch } => {
                    tracing::trace!(epoch = epoch.0, "scheduler handling epoch advanced");
                    self.log_member_index_for_epoch(*epoch);
                    self.lifecycle.reset(*epoch);
                    self.snapshot_progress.reset(*epoch);
                    tracing::trace!(epoch = epoch.0, "scheduler reconciling spools after epoch advance");
                    self.reconcile_spools();
                    // Always refresh after epoch transitions so Standby nodes can
                    // observe new committee membership before lifecycle scheduling.
                    tracing::trace!(epoch = epoch.0, "scheduler requesting refresh after epoch advance");
                    self.request_refresh(true);
                    tracing::trace!(epoch = epoch.0, "scheduler scheduling lifecycle for new epoch");
                    self.schedule_lifecycle(*epoch);
                }
                StateChange::SpoolAssignmentChanged => {
                    tracing::trace!("scheduler reconciling spools after spool assignment change");
                    self.reconcile_spools();
                }
                StateChange::TrackCertified { track } => {
                    tracing::trace!(track = %track, "scheduler checking slices after track certified");
                    self.check_slices(track);
                }
                StateChange::NodeJoinedCommittee { node } => {
                    // If this is our node, refresh on-chain state
                    if *node == self.context.keypair.pubkey() {
                        tracing::trace!(node = %node, "scheduler refreshing after join event for local node");
                        self.request_refresh(true);
                    }
                }
                StateChange::NodeSynced { node } => {
                    // If this is our node, SyncEpoch completed on-chain
                    if *node == self.context.keypair.pubkey() {
                        tracing::trace!(node = %node, "scheduler dropping local sync task after local node synced");
                        self.desired
                            .retain(|key| !matches!(key, TaskKey::SyncEpoch { .. }));
                    }
                }
                StateChange::TrackDeleted { track }
                | StateChange::TrackInvalidated { track } => {
                    tracing::trace!(track = %track, "scheduler removing recoveries for deleted/invalidated track");
                    self.remove_recoveries(track);
                }
                // No scheduler action needed for these events
                StateChange::TrackRegistered { .. }
                | StateChange::TapeReserved { .. }
                | StateChange::TapeDestroyed { .. }
                | StateChange::NodeRegistered { .. } => {}
            }
        }
    }

    /// After a track is certified, check owned spools for missing slices and
    /// enqueue SpoolRecovery tasks for any gaps.
    fn check_slices(&mut self, track: &Pubkey) {
        tracing::trace!(track = %track, "checking slices for track");
        if matches!(self.node_status(), NodeStatus::Standby) {
            tracing::trace!(track = %track, "check_slices skipped for standby node");
            return;
        }

        let store_track: StorePubkey = track.into();

        let track_info = match self.context.store.get_track(store_track) {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::trace!(track = %track, "check_slices skipped: track not found");
                return;
            }
            Err(error) => {
                tracing::trace!(
                    track = %track,
                    error = ?error,
                    "check_slices skipped: failed to read track"
                );
                return;
            }
        };

        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(s) => s,
            Err(error) => {
                tracing::trace!(
                    track = %track,
                    error = ?error,
                    "check_slices skipped: failed to read owned spools"
                );
                return;
            }
        };

        for (spool_id, status) in &owned_spools {
            tracing::trace!(
                track = %track,
                spool_id,
                spool_status = ?status,
                "evaluating spool recovery scheduling"
            );
            if !matches!(status, SpoolStatus::Active | SpoolStatus::ActiveRecover) {
                continue;
            }
            if !spool_in_group(*spool_id, track_info.spool_group) {
                continue;
            }
            match self.context.store.has_slice(*spool_id, store_track) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::trace!(spool_id, track = %track, "scheduling spool recovery for missing slice");
                    let _ = self.context.store.add_pending_recovery(*spool_id, store_track);
                    self.desired.insert(TaskKey::SpoolRecovery { spool: *spool_id });
                }
                Err(_) => {}
            }
        }
    }

    /// Called on the timer tick. Schedules RefreshOnchainState (throttled) and
    /// AdvanceEpoch if the chain is in the Active phase.
    fn periodic_tasks(&mut self) {
        tracing::trace!("scheduler periodic task pass");
        self.request_refresh(false);
        let epoch = self.scheduling_epoch();
        let node_status = self.node_status();
        let chain_phase = self.context.store.get_chain_epoch_phase().ok().flatten();
        let lifecycle_done = self.lifecycle.is_done(&TaskKey::AdvanceEpoch { epoch });
        tracing::trace!(
            epoch = epoch.0,
            node_status = ?node_status,
            chain_phase = ?chain_phase,
            lifecycle_done,
            "periodic lifecycle scheduling check"
        );

        if !matches!(node_status, NodeStatus::Active) {
            tracing::trace!(
                epoch = epoch.0,
                node_status = ?node_status,
                "periodic lifecycle scheduling skipped: node not active"
            );
            return;
        }

        if lifecycle_done {
            tracing::trace!(
                epoch = epoch.0,
                "periodic lifecycle scheduling skipped: AdvanceEpoch already done for epoch"
            );
            return;
        }

        /*
        PHASE1:DISABLED — chain phase gate for periodic tasks
        if !matches!(chain_phase, Some(EpochPhase::Active)) {
            tracing::trace!(
                epoch = epoch.0,
                chain_phase = ?chain_phase,
                "periodic lifecycle scheduling skipped: chain phase not active"
            );
            return;
        }
        */

        tracing::trace!(epoch = epoch.0, "periodic scheduler adding AdvanceEpoch task");
        self.desired.insert(TaskKey::AdvanceEpoch { epoch });
    }

    /// Log this node's committee index for the epoch when available.
    fn log_member_index_for_epoch(&self, epoch: EpochNumber) {
        let committee = match self.context.store.get_committee(epoch).ok().flatten() {
            Some(committee) => committee,
            None => {
                tracing::warn!(
                    epoch = epoch.0,
                    "cannot resolve committee when logging member index"
                );
                return;
            }
        };

        match our_member_index(&committee, self.context.keypair.pubkey()) {
            Ok(member_index) => {
                tracing::info!(
                    epoch = epoch.0,
                    member_index,
                    committee_size = committee.len(),
                    "node member index for epoch"
                );
            }
            Err(error) => {
                tracing::warn!(
                    epoch = epoch.0,
                    error = %error,
                    "node not found in committee for epoch"
                );
            }
        }
    }

    /// How often to poll on-chain state. Committee members poll more aggressively
    /// (3s) since they need to observe phase transitions promptly.
    fn refresh_interval(&self) -> Duration {
        if self.in_committee() {
            Duration::from_secs(3)
        } else {
            Duration::from_secs(30)
        }
    }

    /// Whether this node is a member of the current epoch's committee.
    fn in_committee(&self) -> bool {
        let Some(epoch) = self.context.store.get_chain_epoch().ok().flatten() else {
            return false;
        };
        let Some(committee) = self.context.store.get_committee(epoch).ok().flatten() else {
            return false;
        };
        our_member_index(&committee, self.context.keypair.pubkey()).is_ok()
    }

    /// Add RefreshOnchainState to `desired` if the throttle allows it.
    /// `force` bypasses the throttle (used after epoch transitions and startup).
    fn request_refresh(&mut self, force: bool) {
        if self.desired.contains(&TaskKey::RefreshOnchainState)
            || self.scheduled.contains(&TaskKey::RefreshOnchainState)
        {
            tracing::trace!("refresh already scheduled");
            return;
        }

        let current_epoch = self.context.store.get_chain_epoch().ok().flatten();
        let interval = self.refresh_interval();
        let status = self.node_status();
        let should_schedule = force
            || !self.refresh_throttle.should_skip(interval)
            || current_epoch
                .map(|epoch| self.refresh_throttle.epoch_changed(epoch))
                .unwrap_or(false);

        if should_schedule {
            tracing::trace!(
                force,
                epoch = ?current_epoch,
                node_status = ?status,
                interval_secs = interval.as_secs(),
                "scheduling refresh onchain state"
            );
            self.desired.insert(TaskKey::RefreshOnchainState);
            self.refresh_throttle.record(current_epoch);
        } else {
            tracing::trace!(
                force,
                epoch = ?current_epoch,
                node_status = ?status,
                interval_secs = interval.as_secs(),
                "skipping refresh due to throttle"
            );
        }
    }

    /// Read the node's current status from the store. Defaults to Standby if unset.
    fn node_status(&self) -> NodeStatus {
        self.context.store.get_node_status().ok().flatten().unwrap_or(NodeStatus::Standby)
    }

    /// Whether the on-chain epoch phase is Active (all nodes synced/settled).
    fn chain_phase_is_active(&self) -> bool {
        matches!(
            self.context.store.get_chain_epoch_phase().ok().flatten(),
            Some(EpochPhase::Active)
        )
    }

    /// True if this node is Active, at epoch >= 2, and has no sync cursor yet
    /// (meaning it needs to bootstrap state from a snapshot before syncing).
    fn needs_bootstrap(&self) -> bool {
        if !matches!(self.node_status(), NodeStatus::Active) {
            return false;
        }
        let current_epoch = self.context.store.get_chain_epoch().ok().flatten();
        let sync_cursor = self.context.store.get_sync_cursor().ok().flatten();
        matches!((current_epoch, sync_cursor), (Some(epoch), None) if epoch.0 >= 2)
    }

    /// Remove pending recovery entries for a track that was deleted or invalidated.
    fn remove_recoveries(&self, track: &Pubkey) {
        let store_track: StorePubkey = track.into();
        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };
        for (spool_id, _) in &owned_spools {
            let _ = self.context.store.remove_pending_recovery(*spool_id, store_track);
        }
    }

    /// Sync the desired set with current spool ownership. Removes tasks for
    /// spools we no longer own and adds SpoolSync/SpoolRecovery for new ones.
    fn reconcile_spools(&mut self) {
        if matches!(self.node_status(), NodeStatus::Standby) {
            tracing::trace!("reconcile_spools skipped for standby node");
            return;
        }
        tracing::trace!("reconciling spools in active execution path");

        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("failed to read spool status: {e}");
                return;
            }
        };
        tracing::trace!(owned_spools = owned_spools.len(), "reconciling spool tasks");

        // Remove SpoolSync/SpoolRecovery/RecoveryScan for spools we no longer own
        self.desired.retain(|key| match key {
            TaskKey::SpoolSync { spool }
            | TaskKey::SpoolRecovery { spool }
            | TaskKey::RecoveryScan { spool } => owned_spools.iter().any(|(id, _)| *id == *spool),
            _ => true,
        });

        // Add tasks for owned spools based on their status
        for (spool_id, status) in &owned_spools {
            if matches!(status, SpoolStatus::ActiveSync) {
                tracing::trace!(spool_id, status = ?status, "scheduling spool sync");
                self.desired
                    .insert(TaskKey::SpoolSync { spool: *spool_id });
            }
            if matches!(status, SpoolStatus::ActiveRecover) {
                tracing::trace!(spool_id, status = ?status, "scheduling spool recovery");
                self.desired
                    .insert(TaskKey::SpoolRecovery { spool: *spool_id });
            }
        }
    }

    /// Recompute the desired set for epoch-scoped lifecycle tasks based on the
    /// current chain phase. Syncing → SyncEpoch, Settling → AdvancePool + JoinNetwork,
    /// Active → AdvanceEpoch. Also drives the snapshot pipeline via `schedule_snapshot`.
    fn schedule_lifecycle(&mut self, epoch: EpochNumber) {
        tracing::trace!(epoch = epoch.0, "executing lifecycle scheduling");
        if !matches!(self.node_status(), NodeStatus::Active) {
            tracing::trace!(epoch = epoch.0, "schedule_lifecycle skipped for non-active node");
            return;
        }

        // Keep local lifecycle epoch (scheduler-owned) aligned to chain epoch,
        // even when epoch changes arrive via refresh/replay without EpochAdvanced state changes.
        if self.lifecycle.epoch() != epoch {
            self.lifecycle.reset(epoch);
            self.snapshot_progress.reset(epoch);
        }
        self.desired
            .retain(|key| !matches!(key.scheduled_epoch(), Some(x) if x != epoch));
        let chain_phase = self.context.store.get_chain_epoch_phase().ok().flatten();
        tracing::trace!(
            epoch = epoch.0,
            chain_phase = ?chain_phase,
            in_standby_lifecycle_epoch = self.lifecycle.epoch().0,
            "schedule_lifecycle phase snapshot"
        );

        /*
        PHASE1:DISABLED — phase-based lifecycle selection
        // Recompute lifecycle desired-set from phase each time to avoid stale keys.
        self.desired.remove(&TaskKey::SyncEpoch { epoch });
        self.desired.remove(&TaskKey::AdvancePool { epoch });
        self.desired.remove(&TaskKey::JoinNetwork { epoch });

        let phase = self.context.store.get_chain_epoch_phase().ok().flatten();
        match phase {
            Some(EpochPhase::Syncing) | Some(EpochPhase::Unknown) | None => {
                if !self.lifecycle.is_done(&TaskKey::SyncEpoch { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling SyncEpoch in lifecycle");
                    self.desired.insert(TaskKey::SyncEpoch { epoch });
                } else {
                    tracing::trace!(epoch = epoch.0, "schedule_lifecycle: SyncEpoch already done for epoch");
                }
            }
            Some(EpochPhase::Settling) => {
                if !self.lifecycle.is_done(&TaskKey::AdvancePool { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling AdvancePool in lifecycle");
                    self.desired.insert(TaskKey::AdvancePool { epoch });
                }
                if !self.lifecycle.is_done(&TaskKey::JoinNetwork { epoch }) {
                    tracing::trace!(epoch = epoch.0, "scheduling JoinNetwork in lifecycle");
                    self.desired.insert(TaskKey::JoinNetwork { epoch });
                } else {
                    tracing::trace!(epoch = epoch.0, "schedule_lifecycle: JoinNetwork already done for epoch");
                }
            }
            Some(EpochPhase::Active) => {
                tracing::trace!(epoch = epoch.0, "schedule_lifecycle: chain phase active, waiting for Refresh/AdvanceEpoch loop");
            }
        }
        if self.chain_phase_is_active() && !self.lifecycle.is_done(&TaskKey::AdvanceEpoch { epoch }) {
            tracing::trace!(epoch = epoch.0, "scheduling AdvanceEpoch in lifecycle");
            self.desired.insert(TaskKey::AdvanceEpoch { epoch });
        } else if self.lifecycle.is_done(&TaskKey::AdvanceEpoch { epoch }) {
            tracing::trace!(
                epoch = epoch.0,
                "schedule_lifecycle: AdvanceEpoch already done for epoch"
            );
        } else {
            tracing::trace!(
                epoch = epoch.0,
                "schedule_lifecycle: chain not active for AdvanceEpoch schedule"
            );
        }
        */

        // PHASE1: unconditionally schedule AdvanceEpoch in lifecycle
        if !self.lifecycle.is_done(&TaskKey::AdvanceEpoch { epoch }) {
            tracing::trace!(epoch = epoch.0, "scheduling AdvanceEpoch in lifecycle (phase1)");
            self.desired.insert(TaskKey::AdvanceEpoch { epoch });
        }

        /*
        PHASE1:DISABLED — snapshot scheduling
        self.schedule_snapshot(epoch);
        */
    }

    /// Drive the snapshot pipeline: Build → Collect → Register → Submit.
    /// Reads per-group readiness from the store and advances tasks through the
    /// pipeline stages. Only schedules tasks for spool groups this node owns.
    fn schedule_snapshot(&mut self, epoch: EpochNumber) {
        tracing::trace!(epoch = epoch.0, "scheduling snapshot pipeline");
        let snapshot_build = TaskKey::SnapshotBuild { epoch };
        let snapshot_collect = TaskKey::SnapshotCollect { epoch };
        let register_snapshot = TaskKey::RegisterSnapshot { epoch };
        let snapshot_submit = TaskKey::SnapshotSubmit { epoch };

        let Some(local_epoch) = derive_snapshot_local_epoch(epoch) else {
            tracing::trace!(epoch = epoch.0, "snapshot scheduling skipped: no local epoch");
            self.desired.remove(&snapshot_build);
            self.desired.remove(&snapshot_collect);
            self.desired.remove(&register_snapshot);
            self.desired.remove(&snapshot_submit);
            return;
        };

        if self.snapshot_progress.epoch() != epoch {
            self.snapshot_progress.reset(epoch);
        }

        let all_built = match is_snapshot_build_complete(&self.context, local_epoch) {
            Ok(built) => built,
            Err(e) => {
                tracing::warn!("snapshot pipeline: failed to read build state: {e}");
                false
            }
        };
        tracing::trace!(
            epoch = epoch.0,
            local_epoch = local_epoch.0,
            all_built,
            "snapshot build state checked"
        );

        if !all_built {
            tracing::trace!(epoch = epoch.0, "scheduling snapshot build");
            self.desired.insert(snapshot_build.clone());
        }

        let owned_groups: HashSet<u64> = match self.context.store.get_committee(epoch) {
                Ok(Some(committee)) => {
                match our_snapshot_groups(&committee, self.context.keypair.pubkey()) {
                    Ok(groups) => groups,
                    Err(e) => {
                        tracing::warn!("snapshot pipeline: {e}");
                        tracing::trace!(epoch = epoch.0, "no snapshot groups due to committee resolution error");
                        HashSet::new()
                    }
                }
            }
            Ok(None) => {
                tracing::warn!("snapshot pipeline: missing committee for epoch {}", epoch.0);
                tracing::trace!(epoch = epoch.0, "snapshot ownership unknown: missing committee");
                HashSet::new()
            }
            Err(e) => {
                tracing::warn!("snapshot pipeline: failed to read committee: {e}");
                tracing::trace!(epoch = epoch.0, "snapshot ownership unknown: committee read failed");
                HashSet::new()
            }
        };

        if owned_groups.is_empty() {
            self.desired.remove(&snapshot_collect);
            self.desired.remove(&register_snapshot);
            self.desired.remove(&snapshot_submit);
            tracing::trace!(epoch = epoch.0, owned_groups = 0, "snapshot collect/register/submit unschedulable");
            if !all_built {
                // Cannot yet determine owned groups; keep build running until committee is known.
            } else {
                self.desired.remove(&snapshot_build);
            }
            return;
        }

        let mut ready_groups: Vec<usize> = Vec::new();

        // Rebuild per-group progress from store state.
        for &group in &owned_groups {
            let ready = match is_snapshot_chunk_ready(&self.context, local_epoch, group) {
                Ok(ready) => ready,
                Err(e) => {
                    tracing::warn!("snapshot pipeline: failed to read group readiness: {e}");
                    false
                }
            };
            tracing::trace!(
                epoch = epoch.0,
                group = group,
                ready,
                "snapshot group readiness"
            );
            if ready {
                ready_groups.push(group as usize);
            }
            if !ready {
                continue;
            }

            let ci = ChunkIndex(group);
            let has_cert = self
                .context
                .store
                .get_snapshot_cert(local_epoch, ci)
                .ok()
                .flatten()
                .is_some();
            if has_cert {
                self.snapshot_progress
                    .advance(group as usize, GroupState::Certified);
            }
        }

        let owned_vec: Vec<usize> = owned_groups.iter().map(|&g| g as usize).collect();
        let all_certified = self.snapshot_progress.all_local_cert(&owned_vec);
        let all_onchain = self.snapshot_progress.all_done_onchain(&owned_vec);

        if all_onchain {
            self.desired.remove(&register_snapshot);
            self.desired.remove(&snapshot_submit);
        } else {
            if !ready_groups.is_empty() {
                self.desired.insert(register_snapshot);
            } else {
                self.desired.remove(&register_snapshot);
            }
            if !ready_groups.is_empty() && self.snapshot_progress.any_local_cert(&owned_vec) {
                self.desired.insert(snapshot_submit);
            } else {
                self.desired.remove(&snapshot_submit);
            }
        }

        if all_certified {
            self.desired.remove(&snapshot_collect);
        } else if !ready_groups.is_empty() {
            self.desired.insert(snapshot_collect);
        } else {
            self.desired.remove(&snapshot_collect);
        }

        // Advance-wait gap fix: when all owned groups have completed on-chain,
        // force-reschedule AdvanceEpoch so we don't wait for the next tick.
        if all_onchain {
            tracing::trace!(epoch = epoch.0, "snapshot all groups onchain -> forcing advance epoch reschedule");
            self.scheduled.remove(&TaskKey::AdvanceEpoch { epoch });
            if !self.lifecycle.is_done(&TaskKey::AdvanceEpoch { epoch }) && self.chain_phase_is_active() {
                self.desired.insert(TaskKey::AdvanceEpoch { epoch });
            }
        }
    }

    /// Process a task completion from the supervisor. Stale epoch results are
    /// silently dropped. Otherwise delegates to type-specific handlers.
    fn handle_result(&mut self, result: &TaskResult) {
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
    fn handle_cancelled(&mut self, key: &TaskKey) {
        tracing::trace!(task = ?key, "scheduler removing canceled task from scheduled set");
        self.scheduled.remove(key);
    }

    /// Task succeeded. Marks lifecycle state, removes one-shot tasks from desired,
    /// and triggers follow-up scheduling (refresh → lifecycle, sync → lifecycle,
    /// bootstrap → refresh, snapshot stages).
    fn handle_success(&mut self, key: &TaskKey) {
        tracing::trace!(task = ?key, "scheduler handling task success");
        self.scheduled.remove(key);
        self.lifecycle.mark_done(key);
        if key.is_one_shot() {
            self.desired.remove(key);
        }
        self.handle_refresh_success(key);
        self.handle_sync_success(key);
        self.handle_bootstrap_success(key);
        self.handle_snapshot_success(key);
    }

    /// After RefreshOnchainState succeeds, re-evaluate spool ownership, prune
    /// stale recoveries, reschedule lifecycle tasks, and check if bootstrap is needed.
    fn handle_refresh_success(&mut self, key: &TaskKey) {
        if !matches!(key, TaskKey::RefreshOnchainState) {
            return;
        }
        let before_status = self.node_status();
        let before_phase = self.context.store.get_chain_epoch_phase().ok().flatten();
        let sync_cursor = self.context.store.get_sync_cursor().ok().flatten();
        tracing::trace!("scheduler handling refresh success");
        tracing::trace!(
            before_status = ?before_status,
            before_phase = ?before_phase,
            sync_cursor = sync_cursor.map(|cursor| cursor.0),
            "refresh success: bootstrap state snapshot"
        );
        let epoch = self.context.store.get_chain_epoch().ok().flatten();
        tracing::trace!(epoch = ?epoch, "refresh success: recorded epoch for throttle");
        self.refresh_throttle
            .record(epoch);
        tracing::trace!("refresh success: pruning stale recoveries");
        self.prune_recoveries();
        tracing::trace!("refresh success: reconciling spools");
        self.reconcile_spools();
        if let Some(epoch) = epoch {
            tracing::trace!(epoch = epoch.0, "refresh success: scheduling lifecycle for chain epoch");
            tracing::trace!(
                before_status = ?before_status,
                before_phase = ?before_phase,
                node_status = ?self.node_status(),
                chain_phase = ?self.context
                    .store
                    .get_chain_epoch_phase()
                    .ok()
                    .flatten(),
                "refresh success: refreshed status and lifecycle scheduling"
            );
            self.schedule_lifecycle(epoch);
        } else {
            tracing::trace!("refresh success: unable to read chain epoch for lifecycle scheduling");
        }
        let needs_bootstrap = self.needs_bootstrap();
        if needs_bootstrap {
            tracing::trace!(
                epoch = ?self.context.store.get_chain_epoch().ok().flatten(),
                sync_cursor = sync_cursor.map(|cursor| cursor.0),
                "refresh success: bootstrap required, scheduling snapshot bootstrap"
            );
            tracing::trace!("refresh success: bootstrap required, scheduling snapshot bootstrap");
            self.desired.insert(TaskKey::SnapshotBootstrap);
        } else {
            tracing::trace!(
                sync_cursor = sync_cursor.map(|cursor| cursor.0),
                "refresh success: bootstrap not required"
            );
        }
    }

    /// Remove pending recovery entries whose tracks no longer exist in the store
    /// (e.g. deleted or invalidated). Clears SpoolRecovery from desired when a
    /// spool has no remaining pending recoveries.
    fn prune_recoveries(&mut self) {
        let spools = match self.context.store.iter_all_spools() {
            Ok(spools) => spools,
            Err(_) => return,
        };

        for (spool, status) in &spools {
            let pending = match self.context.store.iter_pending_recoveries(*spool, 1024) {
                Ok(pending) => pending,
                Err(_) => continue,
            };

            for track in &pending {
                let missing = match self.context.store.get_track(*track) {
                    Ok(track_info) => track_info.is_none(),
                    Err(_) => false,
                };
                if missing {
                    let _ = self.context.store.remove_pending_recovery(*spool, *track);
                }
            }

            let has_pending = !pending.is_empty();

            if !has_pending && !matches!(status, SpoolStatus::ActiveRecover) {
                self.desired.remove(&TaskKey::SpoolRecovery { spool: *spool });
            }
        }
    }

    /// After SyncEpoch succeeds, re-run lifecycle scheduling to unlock the
    /// Settling-phase tasks (AdvancePool, JoinNetwork).
    fn handle_sync_success(&mut self, _key: &TaskKey) {
        /*
        PHASE1:DISABLED — no SyncEpoch scheduling
        if !matches!(key, TaskKey::SyncEpoch { .. }) {
            return;
        }
        tracing::trace!(task = ?key, "scheduler handling sync success");
        if let Ok(Some(epoch)) = self.context.store.get_chain_epoch() {
            self.schedule_lifecycle(epoch);
        }
        */
    }

    /// After bootstrap completes, trigger a refresh to pick up the replayed state.
    fn handle_bootstrap_success(&mut self, key: &TaskKey) {
        if matches!(key, TaskKey::SnapshotBootstrap) {
            tracing::trace!("scheduler handling snapshot bootstrap success");
            self.desired.insert(TaskKey::RefreshOnchainState);
        }
    }

    /// Advance snapshot pipeline progress when a snapshot stage completes, then
    /// re-run `schedule_snapshot` to unlock the next stage.
    fn handle_snapshot_success(&mut self, _key: &TaskKey) {
        /*
        PHASE1:DISABLED — no snapshot scheduling
        if !matches!(
            key,
            TaskKey::SnapshotCollect { .. }
                | TaskKey::RegisterSnapshot { .. }
                | TaskKey::SnapshotSubmit { .. }
        ) {
            return;
        }
        tracing::trace!(task = ?key, "scheduler handling snapshot stage success");
        let Some(epoch) = self.context.store.get_chain_epoch().ok().flatten() else {
            return;
        };
        if self.snapshot_progress.epoch() == epoch {
            match key {
                TaskKey::SnapshotCollect { .. } => {
                    self.mark_groups_store(epoch, GroupState::Certified);
                }
                TaskKey::RegisterSnapshot { .. } => {
                    self.mark_owned_groups(epoch, GroupState::Registered);
                }
                TaskKey::SnapshotSubmit { .. } => {
                    self.mark_owned_groups(epoch, GroupState::CertifiedOnchain);
                }
                _ => {}
            }
        }
        self.schedule_snapshot(epoch);
        */
    }

    /// No-op: the supervisor handles retries internally. The key stays in
    /// `scheduled` so the scheduler doesn't re-issue a duplicate Schedule directive.
    fn handle_retry(&self) {
        tracing::trace!("scheduler received retryable result");
    }

    /// Task failed permanently — remove from both sets so it is never retried.
    fn handle_permanent(&mut self, key: &TaskKey) {
        tracing::trace!(task = ?key, "scheduler dropping permanent failure task");
        self.scheduled.remove(key);
        self.desired.remove(key);
    }

    /// Snapshot spool groups this node owns for the given epoch's committee.
    fn groups_for_epoch(&self, epoch: EpochNumber) -> HashSet<u64> {
        match self.context.store.get_committee(epoch) {
            Ok(Some(committee)) => {
                our_snapshot_groups(&committee, self.context.keypair.pubkey()).unwrap_or_default()
            }
            _ => HashSet::new(),
        }
    }

    /// Advance snapshot progress for all groups this node owns.
    fn mark_owned_groups(&mut self, epoch: EpochNumber, state: GroupState) {
        for group in self.groups_for_epoch(epoch) {
            self.snapshot_progress.advance(group as usize, state);
        }
    }

    /// Advance snapshot progress for owned groups that have a cert in the store.
    fn mark_groups_store(&mut self, epoch: EpochNumber, state: GroupState) {
        let Some(local_epoch) = derive_snapshot_local_epoch(epoch) else {
            return;
        };

        for group in self.groups_for_epoch(epoch) {
            if self
                .context
                .store
                .get_snapshot_cert(local_epoch, ChunkIndex(group))
                .ok()
                .flatten()
                .is_some()
            {
                self.snapshot_progress.advance(group as usize, state);
            }
        }
    }

    /// True if the task's epoch doesn't match the current chain epoch (the task
    /// was for a previous epoch that has since advanced).
    fn is_stale_epoch(&self, key: &TaskKey) -> bool {
        let Some(task_epoch) = key.scheduled_epoch() else {
            return false;
        };
        match self.context.store.get_chain_epoch().ok().flatten() {
            Some(current_epoch) => task_epoch != current_epoch,
            None => true,
        }
    }

    fn should_drop_epoch_task(&self, current_epoch: EpochNumber, task_epoch: EpochNumber) -> bool {
        task_epoch.0.saturating_add(2) < current_epoch.0
    }

    fn task_below_retention(&self, current_epoch: EpochNumber, key: &TaskKey) -> bool {
        match key.scheduled_epoch() {
            Some(task_epoch) => self.should_drop_epoch_task(current_epoch, task_epoch),
            None => false,
        }
    }

    /// The epoch to use for new task keys. Prefers the on-chain epoch, falling
    /// back to the lifecycle epoch during bootstrap or when chain state is stale.
    fn scheduling_epoch(&self) -> EpochNumber {
        self.context
            .store
            .get_chain_epoch()
            .ok()
            .flatten()
            .unwrap_or(self.lifecycle.epoch())
    }

    /// Diff `desired` vs `scheduled` and send Schedule/Cancel directives.
    ///
    /// Uses `try_send` so we never block waiting for the supervisor to drain
    /// directives — if the channel is full we break and let the unsent items
    /// be picked up on the next pass (they remain in `desired \ scheduled`).
    /// This prevents a bidirectional deadlock where the scheduler blocks on
    /// directive sends while the supervisor blocks on result sends.
    fn flush(&mut self, tx: &mpsc::Sender<Directive>) {
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
    /// `desired` and `scheduled`. Sends Cancel directives for scheduled ones.
    fn prune_stale(&mut self, tx: &mpsc::Sender<Directive>) {
        let Some(current_epoch) = self.context.store.get_chain_epoch().ok().flatten() else {
            return;
        };

        let stale_scheduled: Vec<_> = self
            .scheduled
            .iter()
            .filter(|key| self.task_below_retention(current_epoch, key))
            .cloned()
            .collect();
        for key in stale_scheduled {
            tracing::trace!(task = ?key, "cancelling stale scheduled task");
            match tx.try_send(Directive::Cancel(key.clone())) {
                Ok(()) => {
                    self.scheduled.remove(&key);
                    self.desired.remove(&key);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        task = ?key,
                        "directive channel full, deferring stale cancel"
                    );
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

    /// Send Schedule directives for tasks in `desired` but not `scheduled`.
    /// Returns `(sent, deferred)`.
    fn send_schedules(&mut self, tx: &mpsc::Sender<Directive>) -> (usize, usize) {
        let to_schedule: Vec<_> = self.desired.difference(&self.scheduled).cloned().collect();
        let total = to_schedule.len();
        let mut sent: usize = 0;

        for key in &to_schedule {
            match tx.try_send(Directive::Schedule(key.clone())) {
                Ok(()) => {
                    sent += 1;
                    self.scheduled.insert(key.clone());
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        task = ?key,
                        sent,
                        remaining = total - sent,
                        "directive channel full, deferring remaining schedules"
                    );
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }

        (sent, total - sent)
    }

    /// Send Cancel directives for tasks in `scheduled` but not `desired`.
    /// Returns `(sent, deferred)`.
    fn send_cancels(&mut self, tx: &mpsc::Sender<Directive>) -> (usize, usize) {
        let to_cancel: Vec<_> = self.scheduled.difference(&self.desired).cloned().collect();
        let total = to_cancel.len();
        let mut sent: usize = 0;

        for key in &to_cancel {
            match tx.try_send(Directive::Cancel(key.clone())) {
                Ok(()) => {
                    sent += 1;
                    self.scheduled.remove(key);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(
                        task = ?key,
                        sent,
                        remaining = total - sent,
                        "directive channel full, deferring remaining cancels"
                    );
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }

        (sent, total - sent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::erasure::SPOOL_GROUP_COUNT;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::snapshot::{ReplayableEvent, SnapshotEntry, SnapshotLog};
    use tape_core::system::EpochPhase;
    use tape_core::types::SlotNumber;
    use tape_core::types::network::NetworkAddress;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_crypto::Hash as CryptoHash;
    use tape_store::ops::{CommitteeOps, MetaOps, ObjectInfoOps, SliceOps, TrackOps};
    use tape_store::types::{
        NodeInfo,
        ObjectInfo,
        Pubkey as StorePubkey,
        SnapshotCertResult,
        SnapshotChunkMeta,
        TrackInfo,
    };

    use crate::fsm::Fsm;
    use crate::runtime::test_utils::test_context;

    fn mark_snapshot_build_complete<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        local_epoch: EpochNumber,
    ) {
        for group in 0..SPOOL_GROUP_COUNT {
            let chunk_index = ChunkIndex(group as u64);
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
        let chunk_index = ChunkIndex(group);
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
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Pre-populate spool state
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx.clone());
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 10 }));
        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 20 }));
        // Epoch advance also schedules one-shot on-chain tasks
        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(scheduled.contains(&TaskKey::SyncEpoch { epoch: EpochNumber(1) }));
        // AdvancePool/JoinNetwork wait for SyncEpoch to complete
        assert!(!scheduled.contains(&TaskKey::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&TaskKey::JoinNetwork { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn spool_removed() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Start with a spool
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx.clone());
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        // First reconciliation — schedules the spool
        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&directive_tx);

        // Drain directives
        while directive_rx.try_recv().is_ok() {}

        // Remove the spool from store
        ctx.store.remove_spool_status(10).unwrap();

        // Second reconciliation — should cancel the spool task
        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(2),
        }]);
        scheduler.flush(&directive_tx);

        let mut cancelled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Cancel(key) = d {
                cancelled.insert(key);
            }
        }

        assert!(cancelled.contains(&TaskKey::SpoolSync { spool: 10 }));
    }

    #[tokio::test]
    async fn oneshot_cleared() {
        let ctx = test_context();
        let mut scheduler = Scheduler::new(ctx.clone());

        let key = TaskKey::AdvanceEpoch { epoch: EpochNumber(0) };
        scheduler.desired.insert(key.clone());
        scheduler.scheduled.insert(key.clone());

        scheduler.handle_result(&TaskResult::Success(key.clone()));

        assert!(!scheduler.desired.contains(&key));
        assert!(!scheduler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn retryable_kept() {
        let ctx = test_context();
        let mut scheduler = Scheduler::new(ctx.clone());

        let key = TaskKey::AdvanceEpoch { epoch: EpochNumber(0) };
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
        let mut scheduler = Scheduler::new(ctx);

        let key = TaskKey::SpoolSync { spool: 42 };
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
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(30, SpoolStatus::ActiveRecover)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolRecovery { spool: 30 }));
    }

    #[tokio::test]
    async fn spool_changed() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(15, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::SpoolAssignmentChanged]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 15 }));
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
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Spool 5 is in group 0 (spools 0-19)
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        // No slice stored → missing

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolRecovery { spool: 5 }));
    }

    #[tokio::test]
    async fn cert_present() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        ctx.store.put_slice(5, store_track, vec![1, 2, 3]).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&directive_tx);

        // No recovery needed — we have the slice
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn cert_group() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Spool 5 is in group 0, but track is in group 1
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(1)).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::TrackCertified { track }]);
        scheduler.flush(&directive_tx);

        // No action — spool not in this track's group
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn joined_ours() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::NodeJoinedCommittee { node: our_pubkey }]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn joined_other() {
        let ctx = test_context();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::NodeJoinedCommittee {
            node: Pubkey::new_unique(),
        }]);
        scheduler.flush(&directive_tx);

        // No directives expected for another node joining
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn sync_clears() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();

        let mut scheduler = Scheduler::new(ctx);
        let epoch = EpochNumber(0);
        scheduler.desired.insert(TaskKey::SyncEpoch { epoch });
        scheduler.scheduled.insert(TaskKey::SyncEpoch { epoch });

        scheduler.update_desired(&[StateChange::NodeSynced { node: our_pubkey }]);

        assert!(!scheduler.desired.contains(&TaskKey::SyncEpoch { epoch }));
    }

    #[tokio::test]
    async fn closed_directive() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        // Drop the receiver — sends will fail
        drop(directive_rx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&directive_tx);

        // scheduled must stay empty — sends failed, no mutation
        assert!(scheduler.scheduled.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_trigger() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        // No sync cursor → needs bootstrap

        let mut scheduler = Scheduler::new(ctx);
        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(scheduler.desired.contains(&TaskKey::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_skip() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_sync_cursor(SlotNumber(500))
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(!scheduler.desired.contains(&TaskKey::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_refresh() {
        let ctx = test_context();
        let mut scheduler = Scheduler::new(ctx);

        scheduler.desired.insert(TaskKey::SnapshotBootstrap);
        scheduler.scheduled.insert(TaskKey::SnapshotBootstrap);

        scheduler.handle_result(&TaskResult::Success(TaskKey::SnapshotBootstrap));

        // SnapshotBootstrap is one-shot, so removed from desired
        assert!(!scheduler.desired.contains(&TaskKey::SnapshotBootstrap));
        // RefreshOnchainState should be scheduled after bootstrap
        assert!(scheduler.desired.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn epoch_derive() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        // 2 SpoolSync + RefreshOnchainState + SyncEpoch (AdvancePool/JoinNetwork gated on SyncEpoch)
        assert_eq!(scheduler.desired.len(), 4);
    }

    #[tokio::test]
    async fn schedules_pool() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(2)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Syncing)
            .unwrap();
        let epoch = EpochNumber(2);

        let mut scheduler = Scheduler::new(ctx.clone());
        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        // SyncEpoch must complete before AdvancePool is scheduled
        assert!(scheduler.desired.contains(&TaskKey::SyncEpoch { epoch }));
        assert!(!scheduler.desired.contains(&TaskKey::AdvancePool { epoch }));

        // Complete SyncEpoch — AdvancePool unlocks
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Settling)
            .unwrap();
        scheduler.desired.insert(TaskKey::SyncEpoch { epoch });
        scheduler.scheduled.insert(TaskKey::SyncEpoch { epoch });
        scheduler.handle_result(&TaskResult::Success(TaskKey::SyncEpoch { epoch }));

        assert!(scheduler.desired.contains(&TaskKey::AdvancePool { epoch }));
    }

    #[tokio::test]
    async fn standby_blocks() {
        let ctx = test_context();
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        // Standby still refreshes on epoch transitions, but does not schedule lifecycle tasks.
        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(!scheduled.contains(&TaskKey::SyncEpoch { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&TaskKey::AdvancePool { epoch: EpochNumber(1) }));
        assert!(!scheduled.contains(&TaskKey::JoinNetwork { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn periodic_refresh() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Active)
            .unwrap();
        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.periodic_tasks();
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(scheduled.contains(&TaskKey::AdvanceEpoch { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn periodic_phase() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Syncing)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.periodic_tasks();
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(!scheduled.contains(&TaskKey::AdvanceEpoch { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn lifecycle_reset() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        let mut scheduler = Scheduler::new(ctx.clone());
        scheduler.lifecycle.reset(EpochNumber(3));
        scheduler
            .lifecycle
            .mark_done(&TaskKey::SyncEpoch { epoch: EpochNumber(3) });
        assert!(scheduler.lifecycle.is_done(&TaskKey::SyncEpoch { epoch: EpochNumber(3) }));

        ctx.store.set_chain_epoch(EpochNumber(4)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Syncing)
            .unwrap();

        scheduler.schedule_lifecycle(EpochNumber(4));

        assert_eq!(scheduler.lifecycle.epoch(), EpochNumber(4));
        assert!(!scheduler.lifecycle.is_done(&TaskKey::SyncEpoch { epoch: EpochNumber(4) }));
        assert!(scheduler.desired.contains(&TaskKey::SyncEpoch { epoch: EpochNumber(4) }));
    }

    #[tokio::test]
    async fn mismatch_resets() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(4)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Active)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        // Simulate an in-flight epoch-scoped task carrying old retry state.
        scheduler.lifecycle.reset(EpochNumber(3));
        let old_epoch = EpochNumber(3);
        let new_epoch = EpochNumber(4);
        scheduler
            .scheduled
            .insert(TaskKey::AdvanceEpoch { epoch: old_epoch });
        scheduler
            .desired
            .insert(TaskKey::AdvanceEpoch { epoch: old_epoch });

        scheduler.schedule_lifecycle(new_epoch);

        let (directive_tx, mut directive_rx) = mpsc::channel(16);
        scheduler.flush(&directive_tx);

        let mut saw_cancel = false;
        let mut saw_schedule = false;
        while let Ok(d) = directive_rx.try_recv() {
            match d {
                Directive::Cancel(TaskKey::AdvanceEpoch { epoch }) if epoch == old_epoch => {
                    saw_cancel = true
                }
                Directive::Schedule(TaskKey::AdvanceEpoch { epoch }) if epoch == new_epoch => {
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
    async fn periodic_standby_skip() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        scheduler.periodic_tasks();
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(!scheduled.contains(&TaskKey::AdvanceEpoch { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn startup_refresh() {
        let ctx = test_context();
        let scheduler = Scheduler::new(ctx);

        // RefreshOnchainState should be in desired before any events arrive
        // (the bootstrap happens in run(), so we verify new() + manual insert)
        let mut r = scheduler;
        r.desired.insert(TaskKey::RefreshOnchainState);
        assert!(r.desired.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn stale_success() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Syncing)
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        scheduler.lifecycle.reset(EpochNumber(3));
        let stale_epoch = EpochNumber(2);
        let current_epoch = EpochNumber(3);
        scheduler
            .desired
            .insert(TaskKey::SyncEpoch { epoch: stale_epoch });
        scheduler
            .scheduled
            .insert(TaskKey::SyncEpoch { epoch: stale_epoch });

        scheduler.handle_result(&TaskResult::Success(TaskKey::SyncEpoch {
            epoch: stale_epoch,
        }));

        assert!(!scheduler.lifecycle.is_done(&TaskKey::SyncEpoch { epoch: current_epoch }));
        assert!(scheduler
            .desired
            .contains(&TaskKey::SyncEpoch { epoch: stale_epoch }));
    }

    #[tokio::test]
    async fn default_standby() {
        let ctx = test_context();
        // No NodeStatus set — default should be Standby
        let mut scheduler = Scheduler::new(ctx);

        // Spools exist but Standby gates spool task scheduling
        scheduler.context.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        // No spool tasks should be scheduled in Standby
        assert!(!scheduler.desired.contains(&TaskKey::SpoolSync { spool: 10 }));
        // No on-chain tasks either
        assert!(!scheduler.desired.contains(&TaskKey::SyncEpoch { epoch: EpochNumber(1) }));
    }

    #[tokio::test]
    async fn refresh_reconcile() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        // Simulate RefreshOnchainState completing
        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));
        scheduler.flush(&directive_tx);

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        // Spool tasks should appear after refresh triggers reconcile_spools()
        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 10 }));
    }

    #[tokio::test]
    async fn refresh_lifecycle() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();

        let mut scheduler = Scheduler::new(ctx);
        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(scheduler.desired.contains(&TaskKey::SyncEpoch { epoch: EpochNumber(3) }));
        // AdvancePool/JoinNetwork gated on SyncEpoch completion
        assert!(!scheduler
            .desired
            .contains(&TaskKey::AdvancePool { epoch: EpochNumber(3) }));
        assert!(!scheduler
            .desired
            .contains(&TaskKey::JoinNetwork { epoch: EpochNumber(3) }));
        assert!(scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch: EpochNumber(3) }));
    }

    #[tokio::test]
    async fn epoch_build() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        let epoch = EpochNumber(3);

        let mut scheduler = Scheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        assert!(scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch }));
    }

    #[tokio::test]
    async fn epoch_skip() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        let epoch = EpochNumber(1);

        let mut scheduler = Scheduler::new(ctx);

        scheduler.update_desired(&[StateChange::EpochAdvanced {
            epoch,
        }]);

        assert!(!scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch }));
    }

    #[tokio::test]
    async fn built_certify() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);

        let mut scheduler = Scheduler::new(ctx);
        scheduler.schedule_lifecycle(epoch);
        assert!(scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
        assert!(!scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
    }

    #[tokio::test]
    async fn built_no_groups() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_non_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);

        let mut scheduler = Scheduler::new(ctx);
        scheduler.schedule_lifecycle(epoch);
        assert!(!scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
        assert!(!scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
        assert!(!scheduler.desired.contains(&TaskKey::SnapshotSubmit { epoch }));
    }

    #[tokio::test]
    async fn cert_onchain() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        scheduler.schedule_lifecycle(epoch);
        assert!(scheduler.desired.contains(&TaskKey::SnapshotSubmit { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
        assert!(!scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn partial_onchain() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5, 25]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);
        let epoch = EpochNumber(3);
        ctx.store
            .set_snapshot_cert(
                local_epoch,
                ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: local_epoch.0,
                },
            )
            .unwrap();

        let mut scheduler = Scheduler::new(ctx);
        scheduler.schedule_lifecycle(epoch);
        assert!(scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::SnapshotSubmit { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn refresh_rebuild() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(3)).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let local_epoch = EpochNumber(2);
        mark_snapshot_build_complete(&ctx, local_epoch);

        let mut scheduler = Scheduler::new(ctx);
        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));
        let epoch = EpochNumber(3);

        assert!(!scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
    }

    #[tokio::test]
    async fn partial_build_register() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);

        let local_epoch = EpochNumber(2);
        let group = 0u64;
        mark_snapshot_group_ready(&ctx, local_epoch, group);
        let epoch = EpochNumber(3);

        let mut scheduler = Scheduler::new(ctx);
        scheduler.schedule_lifecycle(epoch);

        assert!(scheduler.desired.contains(&TaskKey::SnapshotBuild { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::RegisterSnapshot { epoch }));
        assert!(scheduler.desired.contains(&TaskKey::SnapshotCollect { epoch }));
    }

    #[tokio::test]
    async fn delete_recovery() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = Pubkey::new_unique();
        let store_track: StorePubkey = (&track).into();

        // Add pending recovery for this track
        ctx.store.add_pending_recovery(5, store_track).unwrap();

        let mut scheduler = Scheduler::new(ctx.clone());

        // TrackDeleted should remove pending recovery
        scheduler.update_desired(&[StateChange::TrackDeleted { track }]);

        // Verify pending recovery was removed
        let pending = ctx.store.iter_pending_recoveries(5, 100).unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn replay_cancel_recovery() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();
        ctx.store.set_chain_epoch(EpochNumber(2)).unwrap();
        ctx.store
            .set_chain_epoch_phase(EpochPhase::Syncing)
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

        let mut scheduler = Scheduler::new(ctx.clone());
        scheduler.desired.insert(TaskKey::SpoolRecovery { spool: 5 });
        scheduler.scheduled.insert(TaskKey::SpoolRecovery { spool: 5 });

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

        scheduler.desired.insert(TaskKey::RefreshOnchainState);
        scheduler.scheduled.insert(TaskKey::RefreshOnchainState);
        scheduler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        let (directive_tx, mut directive_rx) = mpsc::channel(32);
        scheduler.flush(&directive_tx);

        let mut saw_cancel = false;
        let mut saw_schedule = false;
        while let Ok(dir) = directive_rx.try_recv() {
            match dir {
                Directive::Cancel(TaskKey::SpoolRecovery { spool }) if spool == 5 => saw_cancel = true,
                Directive::Schedule(TaskKey::SpoolRecovery { spool }) if spool == 5 => {
                    saw_schedule = true
                }
                _ => {}
            }
        }

        assert!(saw_cancel);
        assert!(!saw_schedule);
    }
}
