//! Reconciler — diffs desired vs running tasks based on FSM state changes.
//!
//! The reconciler receives `StateChange` events from the FSM and `TaskResult`
//! completions from the supervisor. It maintains a view of what tasks *should*
//! be running and tells the supervisor to schedule or cancel tasks accordingly.

use std::collections::HashSet;
use std::sync::Arc;

use solana_sdk::signer::Signer;
use store::Store;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use tape_store::ops::SpoolOps;
use tape_store::types::SpoolStatus;

use crate::core::NodeContext;
use crate::fsm::StateChange;
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
pub struct Reconciler<S: Store> {
    context: Arc<NodeContext<S>>,
    /// Tasks that SHOULD be running given current state.
    desired: HashSet<TaskKey>,
    /// Tasks we've told the supervisor to schedule (and haven't completed/cancelled).
    scheduled: HashSet<TaskKey>,
}

impl<S: Store> Reconciler<S> {
    pub fn new(context: Arc<NodeContext<S>>) -> Self {
        Self {
            context,
            desired: HashSet::new(),
            scheduled: HashSet::new(),
        }
    }

    pub async fn run(
        mut self,
        mut change_rx: mpsc::Receiver<Vec<StateChange>>,
        mut result_rx: mpsc::Receiver<TaskResult>,
        directive_tx: mpsc::Sender<Directive>,
        cancel: CancellationToken,
    ) {
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

                _ = cancel.cancelled() => break,
            }
        }
    }

    fn update_desired(&mut self, changes: &[StateChange]) {
        for change in changes {
            match change {
                StateChange::EpochAdvanced { .. } => {
                    self.reconcile_spools();
                    // Schedule one-shot on-chain tasks for the new epoch
                    self.desired.insert(TaskKey::RefreshOnchainState);
                    self.desired.insert(TaskKey::SyncEpoch);
                    self.desired.insert(TaskKey::JoinNetwork);
                }
                StateChange::SpoolAssignmentChanged => {
                    self.reconcile_spools();
                }
                StateChange::TrackCertified { .. } => {
                    // A new track was certified — schedule recovery scans for
                    // owned spools so we pick up any missing slices.
                    self.schedule_recovery_scans();
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
                // No reconciler action needed for these events
                StateChange::TrackRegistered { .. }
                | StateChange::TrackDeleted { .. }
                | StateChange::TrackInvalidated { .. }
                | StateChange::TapeReserved { .. }
                | StateChange::TapeDestroyed { .. }
                | StateChange::NodeRegistered { .. } => {}
            }
        }
    }

    fn schedule_recovery_scans(&mut self) {
        let owned_spools = match self.context.store.iter_all_spools() {
            Ok(spools) => spools,
            Err(e) => {
                tracing::error!("failed to read spool status for recovery scan: {e}");
                return;
            }
        };

        for (spool_id, status) in &owned_spools {
            // Only scan spools that are fully active (not mid-sync)
            if matches!(status, SpoolStatus::Active | SpoolStatus::ActiveRecover) {
                self.desired
                    .insert(TaskKey::RecoveryScan { spool: *spool_id });
            }
        }
    }

    fn reconcile_spools(&mut self) {
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

    fn handle_result(&mut self, result: &TaskResult) {
        let key = match result {
            TaskResult::Success(k) => k,
            TaskResult::RetryableError(k, _) => k,
            TaskResult::PermanentError(k, _) => k,
        };

        match result {
            TaskResult::Success(_) => {
                self.scheduled.remove(key);
                if key.is_one_shot() {
                    self.desired.remove(key);
                }
            }
            TaskResult::RetryableError(_, _) => {
                // Supervisor handles retry internally — keep in scheduled.
            }
            TaskResult::PermanentError(_, _) => {
                self.scheduled.remove(key);
                self.desired.remove(key);
            }
        }
    }

    async fn emit_directives(&mut self, tx: &mpsc::Sender<Directive>) {
        // Schedule: in desired but not yet scheduled
        let to_schedule: Vec<_> = self.desired.difference(&self.scheduled).cloned().collect();
        for key in &to_schedule {
            let _ = tx.send(Directive::Schedule(key.clone())).await;
            self.scheduled.insert(key.clone());
        }

        // Cancel: scheduled but no longer desired
        let to_cancel: Vec<_> = self.scheduled.difference(&self.desired).cloned().collect();
        for key in &to_cancel {
            let _ = tx.send(Directive::Cancel(key.clone())).await;
            self.scheduled.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::EpochNumber;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, NodeContext, TlsConfig};

    fn test_config() -> NodeConfig {
        NodeConfig {
            version: 1,
            name: "test-node".to_string(),
            tls_keypair: PathBuf::from("/dev/null"),
            bls_keypair: PathBuf::from("/dev/null"),
            node_keypair: String::new(),
            bind_address: "127.0.0.1:0".parse().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 0,
            tls: TlsConfig::default(),
            storage_path: "/tmp".to_string(),
            poll_interval_ms: None,
            sync_concurrency: None,
            sync_batch_size: None,
            commission: None,
            recovery: RecoveryConfig::default(),
            node_api: NodeApiConfig::default(),
        }
    }

    fn test_context() -> Arc<NodeContext<MemoryStore>> {
        let config = test_config();
        let keypair = solana_sdk::signature::Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(config, keypair, bls_keypair, store)
    }

    #[tokio::test]
    async fn epoch_advance() {
        let ctx = test_context();

        // Pre-populate spool state
        ctx.store
            .set_spool_status(10, SpoolStatus::ActiveSync)
            .unwrap();
        ctx.store
            .set_spool_status(20, SpoolStatus::ActiveSync)
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

        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 10 }));
        assert!(scheduled.contains(&TaskKey::SpoolSync { spool: 20 }));
        // Epoch advance also schedules one-shot on-chain tasks
        assert!(scheduled.contains(&TaskKey::RefreshOnchainState));
        assert!(scheduled.contains(&TaskKey::SyncEpoch));
        assert!(scheduled.contains(&TaskKey::JoinNetwork));
    }

    #[tokio::test]
    async fn spool_removed() {
        let ctx = test_context();

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
        let mut reconciler = Reconciler::new(ctx);

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
        let mut reconciler = Reconciler::new(ctx);

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

    #[tokio::test]
    async fn track_certified_triggers_scan() {
        let ctx = test_context();

        ctx.store
            .set_spool_status(5, SpoolStatus::Active)
            .unwrap();

        let mut reconciler = Reconciler::new(ctx);
        let (directive_tx, mut directive_rx) = mpsc::channel(16);

        reconciler.update_desired(&[StateChange::TrackCertified {
            track: solana_sdk::pubkey::Pubkey::new_unique(),
        }]);
        reconciler.emit_directives(&directive_tx).await;

        let mut scheduled = HashSet::new();
        while let Ok(d) = directive_rx.try_recv() {
            if let Directive::Schedule(key) = d {
                scheduled.insert(key);
            }
        }

        assert!(scheduled.contains(&TaskKey::RecoveryScan { spool: 5 }));
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
    async fn continuous_requeued() {
        let ctx = test_context();
        let mut reconciler = Reconciler::new(ctx);

        // SpoolSync is NOT one-shot
        let key = TaskKey::SpoolSync { spool: 5 };
        reconciler.desired.insert(key.clone());
        reconciler.scheduled.insert(key.clone());

        reconciler.handle_result(&TaskResult::Success(key.clone()));

        // Should still be desired (continuous task), but removed from scheduled
        assert!(reconciler.desired.contains(&key));
        assert!(!reconciler.scheduled.contains(&key));
    }
}
