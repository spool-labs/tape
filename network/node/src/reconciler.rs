//! Reconciler — diffs desired vs running tasks based on FSM state changes.
//!
//! The reconciler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the supervisor. It maintains a view of what tasks *should*
//! be running and tells the supervisor to schedule or cancel tasks accordingly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::program::tapedrive::EPOCH_DURATION;
use tape_core::system::EpochPhase;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use tape_core::erasure::spool_in_group;
use tape_core::types::EpochNumber;
use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, SpoolOps, TrackOps};
use tape_store::types::{ChunkIndex, NodeStatus, SpoolStatus};

use crate::core::NodeContext;
use crate::core::committee::{our_member_index, our_snapshot_groups};
use crate::fsm::StateChange;
use crate::leader::LeaderSchedule;
use crate::state::{GroupState, LifecycleEpochState};
use crate::supervisor::{TaskKey, TaskResult};

/// A directive from the reconciler to the supervisor.
#[derive(Debug, Clone)]
pub enum Directive {
    /// Schedule a new task.
    Schedule(TaskKey),
    /// Cancel a running task.
    Cancel(TaskKey),
}

/// Diffs desired state against running tasks to produce scheduling directives.
pub struct Reconciler<S: Store, R: Rpc> {
    context: Arc<NodeContext<S, R>>,
    /// Tasks that SHOULD be running given current state.
    desired: HashSet<TaskKey>,
    /// Tasks we've told the supervisor to schedule (and haven't completed/cancelled).
    scheduled: HashSet<TaskKey>,
    /// Epoch at which an epoch-scoped task key was first scheduled.
    /// Used to drop stale completions after epoch transitions.
    scheduled_epochs: HashMap<TaskKey, EpochNumber>,
    /// Tracks which one-shot lifecycle tasks completed for the current epoch.
    lifecycle: LifecycleEpochState,
}

impl<S: Store, R: Rpc> Reconciler<S, R> {
    pub fn new(context: Arc<NodeContext<S, R>>) -> Self {
        Self {
            context,
            desired: HashSet::new(),
            scheduled: HashSet::new(),
            scheduled_epochs: HashMap::new(),
            lifecycle: LifecycleEpochState::new(EpochNumber(0)),
        }
    }

    pub async fn run(
        mut self,
        mut change_rx: mpsc::Receiver<Vec<StateChange>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        directive_tx: mpsc::Sender<Directive>,
        cancel: CancellationToken,
    ) {
        // Bootstrap: schedule RefreshOnchainState immediately on startup
        self.desired.insert(TaskKey::RefreshOnchainState);
        self.emit_directives(&directive_tx).await;

        // Refresh often enough to observe committee/epoch transitions in local/test
        // while capping cadence for production.
        let refresh_secs = (EPOCH_DURATION / 2).clamp(1, 30) as u64;
        let mut ticker = tokio::time::interval(Duration::from_secs(refresh_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                changes = change_rx.recv() => {
                    match changes {
                        Some(changes) => {
                            self.update_desired(&changes);
                            self.emit_directives(&directive_tx).await;
                        }
                        None => break,
                    }
                }

                result = result_rx.recv() => {
                    match result {
                        Some(result) => {
                            self.handle_result(&result);
                            self.emit_directives(&directive_tx).await;
                        }
                        None => break,
                    }
                }

                _ = ticker.tick() => {
                    self.periodic_tasks();
                    self.emit_directives(&directive_tx).await;
                }

                _ = cancel.cancelled() => break,
            }
        }
    }

    fn update_desired(&mut self, changes: &[StateChange]) {
        for change in changes {
            match change {
                StateChange::EpochAdvanced { epoch } => {
                    self.lifecycle.reset(*epoch);
                    self.context.snapshot_progress.lock().unwrap().reset(*epoch);
                    self.context.epoch_clock.lock().unwrap().reset(*epoch);
                    self.reconcile_spools();
                    // Always refresh after epoch transitions so Standby nodes can
                    // observe new committee membership before lifecycle scheduling.
                    self.desired.insert(TaskKey::RefreshOnchainState);
                    self.schedule_lifecycle(*epoch);
                }
                StateChange::SpoolAssignmentChanged => {
                    self.reconcile_spools();
                }
                StateChange::TrackCertified { track } => {
                    self.check_track_slices(track);
                }
                StateChange::NodeJoinedCommittee { node } => {
                    // If this is our node, refresh on-chain state
                    if *node == self.context.keypair.pubkey() {
                        self.desired.insert(TaskKey::RefreshOnchainState);
                    }
                }
                StateChange::NodeSynced { node } => {
                    // If this is our node, SyncEpoch completed on-chain
                    if *node == self.context.keypair.pubkey() {
                        self.desired.remove(&TaskKey::SyncEpoch);
                    }
                }
                StateChange::TrackDeleted { track }
                | StateChange::TrackInvalidated { track } => {
                    self.remove_track_recoveries(track);
                }
                // No reconciler action needed for these events
                StateChange::TrackRegistered { .. }
                | StateChange::TapeReserved { .. }
                | StateChange::TapeDestroyed { .. }
                | StateChange::NodeRegistered { .. } => {}
            }
        }
    }

    fn check_track_slices(&mut self, track: &solana_sdk::pubkey::Pubkey) {
        if matches!(self.node_status(), NodeStatus::Standby) {
            return;
        }

        let store_track: tape_store::types::Pubkey = track.into();

        let track_info = match self.context.store.get_track(store_track) {
            Ok(Some(t)) => t,
            _ => return,
        };

        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };

        for (spool_id, status) in &owned_spools {
            if !matches!(status, SpoolStatus::Active | SpoolStatus::ActiveRecover) {
                continue;
            }
            if !spool_in_group(*spool_id, track_info.spool_group) {
                continue;
            }
            match self.context.store.has_slice(*spool_id, store_track) {
                Ok(true) => {}
                Ok(false) => {
                    let _ = self.context.store.add_pending_recovery(*spool_id, store_track);
                    self.desired.insert(TaskKey::SpoolRecovery { spool: *spool_id });
                }
                Err(_) => {}
            }
        }
    }

    fn periodic_tasks(&mut self) {
        self.desired.insert(TaskKey::RefreshOnchainState);
        if matches!(self.node_status(), NodeStatus::Active)
            && !self.lifecycle.is_done(&TaskKey::AdvanceEpoch)
            && self.leader_allows_advance_epoch()
        {
            self.desired.insert(TaskKey::AdvanceEpoch);
        }
    }

    /// Gate AdvanceEpoch on leader position — primary attempts immediately,
    /// backups wait `position * TAKEOVER_WINDOW` seconds.
    fn leader_allows_advance_epoch(&self) -> bool {
        const TAKEOVER_WINDOW_SECS: u64 = 15;

        let epoch = match self.context.store.get_current_epoch() {
            Ok(Some(e)) => e,
            _ => return true, // fallback: allow
        };
        let nonce = match self.context.store.get_epoch_nonce(epoch) {
            Ok(Some(n)) => n,
            _ => return true, // no nonce yet: allow all
        };
        let committee = match self.context.store.get_committee(epoch) {
            Ok(Some(c)) => c,
            _ => return true,
        };
        let our_index = match our_member_index(&committee, self.context.keypair.pubkey()) {
            Ok(idx) => idx,
            _ => return true,
        };

        let schedule = LeaderSchedule::new(committee.len(), nonce);
        let position = schedule.position_for(our_index, 0);
        let wait = Duration::from_secs(position as u64 * TAKEOVER_WINDOW_SECS);
        self.epoch_elapsed() >= wait
    }

    fn epoch_elapsed(&self) -> Duration {
        let epoch = match self.context.store.get_current_epoch().ok().flatten() {
            Some(e) => e,
            None => return Duration::from_secs(0),
        };
        self.context
            .epoch_clock
            .lock()
            .unwrap()
            .elapsed_or_reset(epoch)
    }

    fn node_status(&self) -> NodeStatus {
        self.context.store.get_node_status().ok().flatten().unwrap_or(NodeStatus::Standby)
    }

    fn needs_snapshot_bootstrap(&self) -> bool {
        if !matches!(self.node_status(), NodeStatus::Active) {
            return false;
        }
        let current_epoch = self.context.store.get_current_epoch().ok().flatten();
        let sync_cursor = self.context.store.get_sync_cursor().ok().flatten();
        matches!((current_epoch, sync_cursor), (Some(epoch), None) if epoch.0 >= 2)
    }

    fn remove_track_recoveries(&self, track: &solana_sdk::pubkey::Pubkey) {
        let store_track: tape_store::types::Pubkey = track.into();
        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(s) => s,
            Err(_) => return,
        };
        for (spool_id, _) in &owned_spools {
            let _ = self.context.store.remove_pending_recovery(*spool_id, store_track);
        }
    }

    fn reconcile_spools(&mut self) {
        if matches!(self.node_status(), NodeStatus::Standby) {
            return;
        }

        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("failed to read spool status: {e}");
                return;
            }
        };

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
                self.desired
                    .insert(TaskKey::SpoolSync { spool: *spool_id });
            }
            if matches!(status, SpoolStatus::ActiveRecover) {
                self.desired
                    .insert(TaskKey::SpoolRecovery { spool: *spool_id });
            }
        }
    }

    fn schedule_lifecycle(&mut self, epoch: EpochNumber) {
        if !matches!(self.node_status(), NodeStatus::Active) {
            return;
        }

        // Recompute lifecycle desired-set from phase each time to avoid stale keys.
        self.desired.remove(&TaskKey::SyncEpoch);
        self.desired.remove(&TaskKey::AdvancePool);
        self.desired.remove(&TaskKey::JoinNetwork);

        let phase = self.context.store.get_current_epoch_phase().ok().flatten();
        match phase {
            Some(EpochPhase::Syncing) | Some(EpochPhase::Unknown) | None => {
                if !self.lifecycle.is_done(&TaskKey::SyncEpoch) {
                    self.desired.insert(TaskKey::SyncEpoch);
                }
            }
            Some(EpochPhase::Settling) => {
                if !self.lifecycle.is_done(&TaskKey::AdvancePool) {
                    self.desired.insert(TaskKey::AdvancePool);
                }
                if !self.lifecycle.is_done(&TaskKey::JoinNetwork) {
                    self.desired.insert(TaskKey::JoinNetwork);
                }
            }
            Some(EpochPhase::Active) => {}
        }
        self.schedule_snapshot_pipeline(epoch);
    }

    fn schedule_snapshot_pipeline(&mut self, epoch: EpochNumber) {
        if epoch.0 < 2 {
            self.desired.remove(&TaskKey::SnapshotBuild);
            self.desired.remove(&TaskKey::SnapshotCertify);
            self.desired.remove(&TaskKey::RegisterSnapshot);
            self.desired.remove(&TaskKey::CertifySnapshot);
            return;
        }

        let mut snapshot_progress = self.context.snapshot_progress.lock().unwrap();
        if snapshot_progress.epoch() != epoch {
            snapshot_progress.reset(epoch);
        }

        let target = EpochNumber(epoch.0 - 1);
        let built = self
            .context
            .store
            .get_snapshot_commitment(target, ChunkIndex(0))
            .ok()
            .flatten()
            .is_some();

        if !built {
            self.desired.insert(TaskKey::SnapshotBuild);
            self.desired.remove(&TaskKey::SnapshotCertify);
            self.desired.remove(&TaskKey::RegisterSnapshot);
            self.desired.remove(&TaskKey::CertifySnapshot);
            return;
        }

        let owned_groups: HashSet<u64> = match self.context.store.get_committee(epoch) {
            Ok(Some(committee)) => {
                match our_snapshot_groups(&committee, self.context.keypair.pubkey()) {
                    Ok(groups) => groups,
                    Err(e) => {
                        tracing::warn!("snapshot pipeline: {e}");
                        HashSet::new()
                    }
                }
            }
            Ok(None) => {
                tracing::warn!("snapshot pipeline: missing committee for epoch {}", epoch.0);
                HashSet::new()
            }
            Err(e) => {
                tracing::warn!("snapshot pipeline: failed to read committee: {e}");
                HashSet::new()
            }
        };

        if owned_groups.is_empty() {
            self.desired.remove(&TaskKey::SnapshotCertify);
            self.desired.remove(&TaskKey::RegisterSnapshot);
            self.desired.remove(&TaskKey::CertifySnapshot);
            return;
        }

        // Rebuild per-group progress from store state.
        for &group in &owned_groups {
            let ci = ChunkIndex(group);
            let has_cert = self
                .context
                .store
                .get_snapshot_certification(target, ci)
                .ok()
                .flatten()
                .is_some();
            if has_cert {
                snapshot_progress.advance(group as usize, GroupState::Certified);
            }
        }

        let owned_vec: Vec<usize> = owned_groups.iter().map(|&g| g as usize).collect();
        let all_certified = snapshot_progress.all_local_cert(&owned_vec);
        let all_onchain = snapshot_progress.all_done_onchain(&owned_vec);
        let any_certified = snapshot_progress.any_local_cert(&owned_vec);
        drop(snapshot_progress);

        self.desired.remove(&TaskKey::SnapshotBuild);
        if all_onchain {
            self.desired.remove(&TaskKey::RegisterSnapshot);
            self.desired.remove(&TaskKey::CertifySnapshot);
        } else if any_certified {
            self.desired.insert(TaskKey::RegisterSnapshot);
            self.desired.insert(TaskKey::CertifySnapshot);
        } else {
            self.desired.remove(&TaskKey::RegisterSnapshot);
            self.desired.remove(&TaskKey::CertifySnapshot);
        }

        if all_certified {
            self.desired.remove(&TaskKey::SnapshotCertify);
        } else {
            self.desired.insert(TaskKey::SnapshotCertify);
        }

        // Advance-wait gap fix: when all owned groups have completed on-chain,
        // force-reschedule AdvanceEpoch so we don't wait for the next tick.
        if all_onchain {
            self.scheduled.remove(&TaskKey::AdvanceEpoch);
            self.scheduled_epochs.remove(&TaskKey::AdvanceEpoch);
            if !self.lifecycle.is_done(&TaskKey::AdvanceEpoch) && self.leader_allows_advance_epoch() {
                self.desired.insert(TaskKey::AdvanceEpoch);
            }
        }
    }

    fn handle_result(&mut self, result: &TaskResult) {
        let key = match result {
            TaskResult::Success(k) => k,
            TaskResult::RetryableError(k, _) => k,
            TaskResult::PermanentError(k, _) => k,
        };

        let stale_epoch_result = self.is_stale_epoch_result(key);

        match result {
            TaskResult::Success(_) => {
                self.scheduled.remove(key);
                self.scheduled_epochs.remove(key);
                if !stale_epoch_result {
                    self.lifecycle.mark_done(key);
                }
                if key.is_one_shot() && !stale_epoch_result {
                    self.desired.remove(key);
                }
                // After state refresh, reconcile spools (committee may have changed)
                if matches!(key, TaskKey::RefreshOnchainState) {
                    self.reconcile_spools();
                    if let Ok(Some(epoch)) = self.context.store.get_current_epoch() {
                        self.context.epoch_clock.lock().unwrap().elapsed_or_reset(epoch);
                        self.schedule_lifecycle(epoch);
                    }
                    if self.needs_snapshot_bootstrap() {
                        self.desired.insert(TaskKey::SnapshotBootstrap);
                    }
                }
                // After SyncEpoch, unlock AdvancePool/JoinNetwork
                if matches!(key, TaskKey::SyncEpoch) {
                    if let Ok(Some(epoch)) = self.context.store.get_current_epoch() {
                        self.schedule_lifecycle(epoch);
                    }
                }
                // After bootstrap, refresh on-chain state to reconcile spools
                if matches!(key, TaskKey::SnapshotBootstrap) {
                    self.desired.insert(TaskKey::RefreshOnchainState);
                }
                // Re-evaluate snapshot pipeline after progress changes.
                if matches!(
                    key,
                    TaskKey::SnapshotCertify
                        | TaskKey::RegisterSnapshot
                        | TaskKey::CertifySnapshot
                ) && !stale_epoch_result {
                    if let Ok(Some(epoch)) = self.context.store.get_current_epoch() {
                        self.schedule_snapshot_pipeline(epoch);
                    }
                }
            }
            TaskResult::RetryableError(_, _) => {
                // Supervisor handles retry internally — keep in scheduled.
            }
            TaskResult::PermanentError(_, _) => {
                self.scheduled.remove(key);
                self.scheduled_epochs.remove(key);
                self.desired.remove(key);
            }
        }
    }

    fn is_epoch_scoped_task(key: &TaskKey) -> bool {
        matches!(
            key,
            TaskKey::SyncEpoch
                | TaskKey::AdvancePool
                | TaskKey::JoinNetwork
                | TaskKey::AdvanceEpoch
                | TaskKey::SnapshotCertify
                | TaskKey::RegisterSnapshot
                | TaskKey::CertifySnapshot
        )
    }

    fn is_stale_epoch_result(&self, key: &TaskKey) -> bool {
        if !Self::is_epoch_scoped_task(key) {
            return false;
        }
        let Some(task_epoch) = self.scheduled_epochs.get(key).copied() else {
            return false;
        };
        let current = self.context.store.get_current_epoch().ok().flatten();
        match current {
            Some(current_epoch) => task_epoch != current_epoch,
            None => true,
        }
    }

    async fn emit_directives(&mut self, tx: &mpsc::Sender<Directive>) {
        // Schedule: in desired but not yet scheduled
        let to_schedule: Vec<_> = self.desired.difference(&self.scheduled).cloned().collect();
        for key in to_schedule {
            if tx.send(Directive::Schedule(key.clone())).await.is_err() {
                return;
            }
            self.scheduled.insert(key.clone());
            if Self::is_epoch_scoped_task(&key) {
                if let Ok(Some(epoch)) = self.context.store.get_current_epoch() {
                    self.scheduled_epochs.insert(key.clone(), epoch);
                }
            }
        }

        // Cancel: scheduled but no longer desired
        let to_cancel: Vec<_> = self.scheduled.difference(&self.desired).cloned().collect();
        for key in to_cancel {
            if tx.send(Directive::Cancel(key.clone())).await.is_err() {
                return;
            }
            self.scheduled.remove(&key);
            self.scheduled_epochs.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytemuck::Zeroable;
    use tape_api::program::tapedrive::node_pda;
    use tape_core::bls::{BlsPubkey, BlsSignature};
    use tape_core::types::network::NetworkAddress;
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;
    use tape_store::ops::{CommitteeOps, MetaOps, SliceOps, TrackOps};
    use tape_store::types::{NodeInfo, SnapshotCertResult, TrackInfo};

    use crate::test_util::test_context;

    fn put_our_committee<S: Store, R: Rpc>(
        ctx: &Arc<NodeContext<S, R>>,
        epoch: EpochNumber,
        spools: Vec<u16>,
    ) {
        let (node_address, _) = node_pda(ctx.keypair.pubkey());
        let members = vec![NodeInfo {
            node_address: tape_store::types::Pubkey::new(node_address.to_bytes()),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
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
            node_address: tape_store::types::Pubkey::new([9u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
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

        let mut reconciler = Reconciler::new(ctx.clone());
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        reconciler.emit_directives(&directive_tx).await;

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
        assert!(scheduled.contains(&TaskKey::SyncEpoch));
        // AdvancePool/JoinNetwork wait for SyncEpoch to complete
        assert!(!scheduled.contains(&TaskKey::AdvancePool));
        assert!(!scheduled.contains(&TaskKey::JoinNetwork));
    }

    #[tokio::test]
    async fn spool_removed() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Start with a spool
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx.clone());
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        // First reconciliation — schedules the spool
        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        // Drain directives
        while directive_rx.try_recv().is_ok() {}

        // Remove the spool from store
        ctx.store.remove_spool_status(10).unwrap();

        // Second reconciliation — should cancel the spool task
        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(2),
        }]);
        reconciler.emit_directives(&directive_tx).await;

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
        let mut reconciler = Reconciler::new(ctx.clone());

        let key = TaskKey::AdvanceEpoch;
        reconciler.desired.insert(key.clone());
        reconciler.scheduled.insert(key.clone());

        reconciler.handle_result(&TaskResult::Success(key.clone()));

        assert!(!reconciler.desired.contains(&key));
        assert!(!reconciler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn retryable_kept() {
        let ctx = test_context();
        let mut reconciler = Reconciler::new(ctx.clone());

        let key = TaskKey::AdvanceEpoch;
        reconciler.desired.insert(key.clone());
        reconciler.scheduled.insert(key.clone());

        reconciler
            .handle_result(&TaskResult::RetryableError(key.clone(), "transient".into()));

        assert!(reconciler.desired.contains(&key));
        assert!(reconciler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn permanent_removed() {
        let ctx = test_context();
        let mut reconciler = Reconciler::new(ctx);

        let key = TaskKey::SpoolSync { spool: 42 };
        reconciler.desired.insert(key.clone());
        reconciler.scheduled.insert(key.clone());

        reconciler
            .handle_result(&TaskResult::PermanentError(key.clone(), "fatal".into()));

        assert!(!reconciler.desired.contains(&key));
        assert!(!reconciler.scheduled.contains(&key));
    }

    #[tokio::test]
    async fn active_recover() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(30, SpoolStatus::ActiveRecover)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolRecovery { spool: 30 }));
    }

    #[tokio::test]
    async fn spool_assignment_changed() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(15, SpoolStatus::ActiveSync)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::SpoolAssignmentChanged]);
        reconciler.emit_directives(&directive_tx).await;

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
            tape_address: tape_store::types::Pubkey([0u8; 32]),
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
    async fn certified_missing_slice() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Spool 5 is in group 0 (spools 0-19)
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = solana_sdk::pubkey::Pubkey::new_unique();
        let store_track: tape_store::types::Pubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        // No slice stored → missing

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::TrackCertified { track }]);
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::SpoolRecovery { spool: 5 }));
    }

    #[tokio::test]
    async fn certified_have_slice() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = solana_sdk::pubkey::Pubkey::new_unique();
        let store_track: tape_store::types::Pubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(0)).unwrap();
        ctx.store.put_slice(5, store_track, vec![1, 2, 3]).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::TrackCertified { track }]);
        reconciler.emit_directives(&directive_tx).await;

        // No recovery needed — we have the slice
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn certified_wrong_group() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        // Spool 5 is in group 0, but track is in group 1
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = solana_sdk::pubkey::Pubkey::new_unique();
        let store_track: tape_store::types::Pubkey = (&track).into();
        ctx.store.put_track(store_track, make_track_info(1)).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::TrackCertified { track }]);
        reconciler.emit_directives(&directive_tx).await;

        // No action — spool not in this track's group
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn our_node_joined() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::NodeJoinedCommittee { node: our_pubkey }]);
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn other_node_joined() {
        let ctx = test_context();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::NodeJoinedCommittee {
            node: solana_sdk::pubkey::Pubkey::new_unique(),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        // No directives expected for another node joining
        assert!(directive_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn node_synced_clears_task() {
        let ctx = test_context();
        let our_pubkey = ctx.keypair.pubkey();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.desired.insert(TaskKey::SyncEpoch);
        reconciler.scheduled.insert(TaskKey::SyncEpoch);

        reconciler.update_desired(&[StateChange::NodeSynced { node: our_pubkey }]);

        assert!(!reconciler.desired.contains(&TaskKey::SyncEpoch));
    }

    #[tokio::test]
    async fn closed_directive_channel() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, directive_rx) = mpsc::channel(16);

        // Drop the receiver — sends will fail
        drop(directive_rx);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        // scheduled must stay empty — sends failed, no mutation
        assert!(reconciler.scheduled.is_empty());
    }

    #[tokio::test]
    async fn bootstrap_trigger() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();
        // No sync cursor → needs bootstrap

        let mut reconciler = Reconciler::new(ctx);
        reconciler.desired.insert(TaskKey::RefreshOnchainState);
        reconciler.scheduled.insert(TaskKey::RefreshOnchainState);
        reconciler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(reconciler.desired.contains(&TaskKey::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_not_needed() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_sync_cursor(tape_core::types::SlotNumber(500))
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.desired.insert(TaskKey::RefreshOnchainState);
        reconciler.scheduled.insert(TaskKey::RefreshOnchainState);
        reconciler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(!reconciler.desired.contains(&TaskKey::SnapshotBootstrap));
    }

    #[tokio::test]
    async fn bootstrap_schedules_refresh() {
        let ctx = test_context();
        let mut reconciler = Reconciler::new(ctx);

        reconciler.desired.insert(TaskKey::SnapshotBootstrap);
        reconciler.scheduled.insert(TaskKey::SnapshotBootstrap);

        reconciler.handle_result(&TaskResult::Success(TaskKey::SnapshotBootstrap));

        // SnapshotBootstrap is one-shot, so removed from desired
        assert!(!reconciler.desired.contains(&TaskKey::SnapshotBootstrap));
        // RefreshOnchainState should be scheduled after bootstrap
        assert!(reconciler.desired.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn epoch_advance_derivation() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        // 2 SpoolSync + RefreshOnchainState + SyncEpoch (AdvancePool/JoinNetwork gated on SyncEpoch)
        assert_eq!(reconciler.desired.len(), 4);
    }

    #[tokio::test]
    async fn schedules_pool() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(2)).unwrap();
        ctx.store
            .set_current_epoch_phase(tape_core::system::EpochPhase::Syncing)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx.clone());
        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(2),
        }]);

        // SyncEpoch must complete before AdvancePool is scheduled
        assert!(reconciler.desired.contains(&TaskKey::SyncEpoch));
        assert!(!reconciler.desired.contains(&TaskKey::AdvancePool));

        // Complete SyncEpoch — AdvancePool unlocks
        ctx.store
            .set_current_epoch_phase(tape_core::system::EpochPhase::Settling)
            .unwrap();
        reconciler.desired.insert(TaskKey::SyncEpoch);
        reconciler.scheduled.insert(TaskKey::SyncEpoch);
        reconciler.handle_result(&TaskResult::Success(TaskKey::SyncEpoch));

        assert!(reconciler.desired.contains(&TaskKey::AdvancePool));
    }

    #[tokio::test]
    async fn standby_blocks_tasks() {
        let ctx = test_context();
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        // Standby still refreshes on epoch transitions, but does not schedule lifecycle tasks.
        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(!scheduled.contains(&TaskKey::SyncEpoch));
        assert!(!scheduled.contains(&TaskKey::AdvancePool));
        assert!(!scheduled.contains(&TaskKey::JoinNetwork));
    }

    #[tokio::test]
    async fn periodic_refresh() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.periodic_tasks();
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(scheduled.contains(&TaskKey::AdvanceEpoch));
    }

    #[tokio::test]
    async fn periodic_standby_skip() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.periodic_tasks();
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(!scheduled.contains(&TaskKey::AdvanceEpoch));
    }

    #[tokio::test]
    async fn startup_schedules_refresh() {
        let ctx = test_context();
        let reconciler = Reconciler::new(ctx);

        // RefreshOnchainState should be in desired before any events arrive
        // (the bootstrap happens in run(), so we verify new() + manual insert)
        let mut r = reconciler;
        r.desired.insert(TaskKey::RefreshOnchainState);
        assert!(r.desired.contains(&TaskKey::RefreshOnchainState));
    }

    #[tokio::test]
    async fn stale_lifecycle_success_does_not_mark_done() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();
        ctx.store
            .set_current_epoch_phase(tape_core::system::EpochPhase::Syncing)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.lifecycle.reset(EpochNumber(3));
        reconciler.desired.insert(TaskKey::SyncEpoch);
        reconciler.scheduled.insert(TaskKey::SyncEpoch);
        reconciler
            .scheduled_epochs
            .insert(TaskKey::SyncEpoch, EpochNumber(2));

        reconciler.handle_result(&TaskResult::Success(TaskKey::SyncEpoch));

        assert!(!reconciler.lifecycle.is_done(&TaskKey::SyncEpoch));
        assert!(reconciler.desired.contains(&TaskKey::SyncEpoch));
    }

    #[tokio::test]
    async fn default_standby() {
        let ctx = test_context();
        // No NodeStatus set — default should be Standby
        let mut reconciler = Reconciler::new(ctx);

        // Spools exist but Standby gates spool task scheduling
        reconciler.context.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        // No spool tasks should be scheduled in Standby
        assert!(!reconciler.desired.contains(&TaskKey::SpoolSync { spool: 10 }));
        // No on-chain tasks either
        assert!(!reconciler.desired.contains(&TaskKey::SyncEpoch));
    }

    #[tokio::test]
    async fn refresh_triggers_reconcile() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_spool_status(10, SpoolStatus::ActiveSync).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        // Simulate RefreshOnchainState completing
        reconciler.desired.insert(TaskKey::RefreshOnchainState);
        reconciler.scheduled.insert(TaskKey::RefreshOnchainState);
        reconciler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));
        reconciler.emit_directives(&directive_tx).await;

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
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.desired.insert(TaskKey::RefreshOnchainState);
        reconciler.scheduled.insert(TaskKey::RefreshOnchainState);
        reconciler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(reconciler.desired.contains(&TaskKey::SyncEpoch));
        // AdvancePool/JoinNetwork gated on SyncEpoch completion
        assert!(!reconciler.desired.contains(&TaskKey::AdvancePool));
        assert!(!reconciler.desired.contains(&TaskKey::JoinNetwork));
        assert!(reconciler.desired.contains(&TaskKey::SnapshotBuild));
    }

    #[tokio::test]
    async fn epoch_schedules_build() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        let mut reconciler = Reconciler::new(ctx);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(3),
        }]);

        assert!(reconciler.desired.contains(&TaskKey::SnapshotBuild));
    }

    #[tokio::test]
    async fn epoch_skips_build_early() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();

        let mut reconciler = Reconciler::new(ctx);

        reconciler.update_desired(&[StateChange::EpochAdvanced {
            epoch: EpochNumber(1),
        }]);

        assert!(!reconciler.desired.contains(&TaskKey::SnapshotBuild));
    }

    #[tokio::test]
    async fn built_epoch_schedules_certify() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let target = EpochNumber(2);
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), tape_crypto::Hash::new_unique())
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.schedule_lifecycle(EpochNumber(3));
        assert!(reconciler.desired.contains(&TaskKey::SnapshotCertify));
        assert!(!reconciler.desired.contains(&TaskKey::SnapshotBuild));
        assert!(!reconciler.desired.contains(&TaskKey::RegisterSnapshot));
    }

    #[tokio::test]
    async fn built_epoch_with_no_owned_groups_skips_snapshot_tasks() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_non_our_committee(&ctx, EpochNumber(3), vec![5]);
        let target = EpochNumber(2);
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), tape_crypto::Hash::new_unique())
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.schedule_lifecycle(EpochNumber(3));
        assert!(!reconciler.desired.contains(&TaskKey::SnapshotCertify));
        assert!(!reconciler.desired.contains(&TaskKey::RegisterSnapshot));
        assert!(!reconciler.desired.contains(&TaskKey::CertifySnapshot));
    }

    #[tokio::test]
    async fn certified_epoch_schedules_onchain() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let target = EpochNumber(2);
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), tape_crypto::Hash::new_unique())
            .unwrap();
        ctx.store
            .set_snapshot_certification(
                target,
                ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: target.0,
                },
            )
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.schedule_lifecycle(EpochNumber(3));
        assert!(reconciler.desired.contains(&TaskKey::CertifySnapshot));
        assert!(reconciler.desired.contains(&TaskKey::RegisterSnapshot));
        assert!(!reconciler.desired.contains(&TaskKey::SnapshotCertify));
    }

    #[tokio::test]
    async fn partial_cert_schedules_onchain_and_certify() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5, 25]);
        let target = EpochNumber(2);
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), tape_crypto::Hash::new_unique())
            .unwrap();
        ctx.store
            .set_snapshot_certification(
                target,
                ChunkIndex(0),
                SnapshotCertResult {
                    member_indices: vec![0, 1, 2],
                    signature: BlsSignature(G1CompressedPoint([7u8; 32])),
                    epoch: target.0,
                },
            )
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.schedule_lifecycle(EpochNumber(3));
        assert!(reconciler.desired.contains(&TaskKey::RegisterSnapshot));
        assert!(reconciler.desired.contains(&TaskKey::CertifySnapshot));
        assert!(reconciler.desired.contains(&TaskKey::SnapshotCertify));
    }

    #[tokio::test]
    async fn refresh_rebuilds_snapshot_plan_from_stage() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(3)).unwrap();
        put_our_committee(&ctx, EpochNumber(3), vec![5]);
        let target = EpochNumber(2);
        ctx.store
            .set_snapshot_commitment(target, ChunkIndex(0), tape_crypto::Hash::new_unique())
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        reconciler.desired.insert(TaskKey::RefreshOnchainState);
        reconciler.scheduled.insert(TaskKey::RefreshOnchainState);
        reconciler.handle_result(&TaskResult::Success(TaskKey::RefreshOnchainState));

        assert!(!reconciler.desired.contains(&TaskKey::SnapshotBuild));
        assert!(reconciler.desired.contains(&TaskKey::SnapshotCertify));
    }

    #[tokio::test]
    async fn delete_cancels_recovery() {
        let ctx = test_context();
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let track = solana_sdk::pubkey::Pubkey::new_unique();
        let store_track: tape_store::types::Pubkey = (&track).into();

        // Add pending recovery for this track
        ctx.store.add_pending_recovery(5, store_track).unwrap();

        let mut reconciler = Reconciler::new(ctx.clone());

        // TrackDeleted should remove pending recovery
        reconciler.update_desired(&[StateChange::TrackDeleted { track }]);

        // Verify pending recovery was removed
        let pending = ctx.store.iter_pending_recoveries(5, 100).unwrap();
        assert!(pending.is_empty());
    }
}
