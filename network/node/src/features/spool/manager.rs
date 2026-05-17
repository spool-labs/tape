use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{SPOOL_COUNT, GROUP_SIZE};
use tape_core::prelude::{EpochNumber, GroupIndex, SpoolIndex, SpoolState, SpoolStatus};
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::types::{Action, ScanResult, TaskDone, TaskResult};
use crate::features::spool::{recover, repair, scan, sync};

const SPOOL_MANAGER_HEARTBEAT: Duration = Duration::from_secs(1);
const LOCKED_SPOOL_RETENTION_EPOCHS: u64 = 4;

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: RecoveryConfig,
    cancel: CancellationToken,
    workers: HashMap<SpoolIndex, CancellationToken>,
    join_set: JoinSet<TaskDone>,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SpoolManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: RecoveryConfig,
        cancel: CancellationToken,
    ) -> Self {
        let workers = HashMap::new();
        let join_set = JoinSet::new();

        Self {
            context,
            config,
            cancel,
            workers, 
            join_set,
        }
    }

    /// Spawn tasks based on state changes and task completions.
    pub async fn run(mut self) -> Result<(), NodeError> {

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;

        self.advance(observed_epoch)?;
        self.try_spawn(observed_epoch)?;

        loop {
            tokio::select! {
                // Shutdown signal
                _ = self.cancel.cancelled() => {
                    info!("spool: shutdown signal received, exiting");

                    self.stop().await;

                    return Ok(());
                }

                // Worker completion
                Some(result) = self.join_set.join_next() => {
                    let done = match result {
                        Ok(done) => done,
                        Err(e) => {
                            if e.is_cancelled() {
                                debug!("spool: task was aborted");
                            } else {
                                warn!(?e, "spool: task panicked");
                            }
                            continue;
                        }
                    };

                    self.handle_done(done, observed_epoch)?;
                    self.try_spawn(observed_epoch)?;
                }

                // State changed ("replan" signal)
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        warn!("spool: state channel closed");

                        self.stop().await;

                        return Ok(());
                    }

                    let state = state_rx.borrow().clone();

                    if state.epoch != observed_epoch {
                        info!(
                            old_epoch = observed_epoch.0,
                            new_epoch = state.epoch.0,
                            "spool: epoch advanced, resetting"
                        );

                        self.stop().await;

                        observed_epoch = state.epoch;

                        self.advance(observed_epoch)?;
                    }

                    self.try_spawn(observed_epoch)?;
                }

                // Periodic heartbeat
                _ = tokio::time::sleep(SPOOL_MANAGER_HEARTBEAT) => {
                    self.try_spawn(observed_epoch)?;
                }
            }
        }
    }

    /// Determine the next action based on current state.
    pub fn next_action(
        &self,
        epoch: EpochNumber,
    ) -> Result<Option<Action>, NodeError> {
        if self.workers.len() >= self.config.max_workers {
            return Ok(None);
        }
    
        let mut spools = self
            .context
            .store
            .iter_all_spools()
            .map_err(|e| NodeError::Store(format!("iter_all_spools: {e}")))?;
    
        spools
            .sort_unstable_by_key(
                |(spool, state)| (status_priority(state.status), *spool)
        );
    
        for (spool, state) in spools {
            if state.epoch != epoch {
                continue;
            }

            if self.is_running(spool) {
                continue;
            }

            if let Some(action) = action_for_status(spool, epoch, state.status) {
                return Ok(Some(action));
            }

            // Active spools: check if pending work appeared (e.g. from CertifyTrack hook)
            if state.status == SpoolStatus::Active {
                let (has_repair, has_recovery) = has_pending_work(&self.context.store, spool)?;
                if has_repair {
                    return Ok(Some(Action::Repair { spool, epoch }));
                } else if has_recovery {
                    return Ok(Some(Action::Recover { spool, epoch }));
                }
            }
        }

        Ok(None)
    }

    /// Try to spawn new tasks up to the parallel limit.
    fn try_spawn(
        &mut self,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        while self.workers.len() < self.config.max_workers {
            let Some(action) = self.next_action(epoch)? else {
                break;
            };

            // Check if we're already running a task for this spool
            let spool = action.spool();
            if self.is_running(spool) {
                continue;
            }

            // If the spool is Active but we're about to run work, persist the transition.
            if let Some(mut state) = self
                .context
                .store
                .get_spool_state(spool)
                .map_err(|e| NodeError::Store(format!("get_spool_state({spool}): {e}")))?
            {
                if state.status == SpoolStatus::Active {
                    let next_status = match action {
                        Action::Repair { .. } => SpoolStatus::Repair,
                        Action::Recover { .. } => SpoolStatus::Recover,
                        _ => state.status,
                    };
                    if next_status != state.status {
                        state.set_status(next_status);
                        self.context
                            .store
                            .set_spool_state(spool, state)
                            .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;
                    }
                }
            }

            let ctx = self.context.clone();
            let config = self.config.clone();
            let token = self.cancel.child_token();

            self.workers.insert(spool, token.clone());

            info!(?action, epoch = epoch.0, "spool: spawning task");

            self.join_set.spawn(async move {
                let result = match action {
                    Action::Sync { spool, .. } => {
                        TaskResult::Sync(sync::run(ctx, &config, spool, &token).await)
                    }
                    Action::Scan { spool, .. } => {
                        TaskResult::Scan(scan::run(ctx, &config, spool, &token).await)
                    }
                    Action::Repair { spool, .. } => {
                        TaskResult::Repair(repair::run(ctx, &config, spool, &token).await)
                    }
                    Action::Recover { spool, .. } => {
                        TaskResult::Recover(recover::run(ctx, &config, spool, &token).await)
                    }
                };

                if token.is_cancelled() {
                    TaskDone::Cancelled(action, result)
                } else {
                    TaskDone::Done(action, result)
                }
            });
        }

        Ok(())
    }

    fn handle_done(
        &mut self,
        done: TaskDone,
        observed_epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        let action = done.action();
        let spool = action.spool();
        let worker_epoch = action.epoch();

        self.workers.remove(&spool);

        if worker_epoch != observed_epoch {
            debug!(
                ?action,
                observed_epoch = observed_epoch.0,
                "spool: ignoring stale task completion"
            );
            return Ok(());
        }

        match done {
            TaskDone::Done(action, result) => {
                let Some(mut state) = self
                    .context
                    .store
                    .get_spool_state(spool)
                    .map_err(|e| NodeError::Store(format!("get_spool_state({spool}): {e}")))?
                else {
                    debug!(spool, "spool: missing state for completed task");
                    return Ok(());
                };

                let Some(next_status) = transition_status(&self.context.store, spool, action, result)? else {
                    warn!(?action, ?result, "spool: invalid task completion");
                    return Ok(());
                };

                state.set_status(next_status);

                self.context
                    .store
                    .set_spool_state(spool, state)
                    .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;

                info!(spool, epoch = observed_epoch.0, status = ?next_status, "spool: task completed");
            }
            TaskDone::Cancelled(action, result) => {
                debug!(?action, ?result, "spool: task cancelled");
            }
            TaskDone::Rejected(action, result) => {
                warn!(?action, ?result, "spool: task rejected");
            }
        }

        Ok(())
    }

    /// Update the current spool states based on the assignements
    pub fn advance(
        &self,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {

        // Get the spools we've been assigned for the current epoch
        let assignments : HashSet<SpoolIndex> = self.context.my_spools();

        // iterate over all spools
        for spool in 0..SPOOL_COUNT {
            let spool : SpoolIndex = spool as u16;

            let is_assigned = assignments.contains(&spool);
            let state = self.context.store.get_spool_state(spool)
                .map_err(|e| NodeError::Store(format!("get_spool_state: {e}")))?;

            match (is_assigned, state) {

                // Not assigned, no state → nothing to do
                (false, None) => {
                    // If we don't have a state for a spool and it's not assigned to us, we can
                    // just ignore it.
                }

                // Not assigned, have state → lost ownership -> (lock)
                (false, Some(state)) => {
                    if state.is_locked() {
                        if check_expiry(
                            state.epoch,
                            epoch,
                            LOCKED_SPOOL_RETENTION_EPOCHS,
                        ) {
                            info!(spool, epoch = epoch.0, "spool: purging locked spool after retention period");

                            purge_locked_spool(self.context.as_ref(), spool)?;
                        }
                    } else {
                        info!(spool, epoch = epoch.0, "spool: locking spool due to lost ownership");

                        reset_spool_state(self.context.as_ref(), spool)?;

                        let mut state = state;
                        state.set_status(SpoolStatus::LockedToMove);
                        state.set_epoch(epoch);

                        self.context
                            .store
                            .set_spool_state(spool, state)
                            .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;
                    }
                }

                // Assigned, no state → create state -> (sync)
                (true, None) => {
                    info!(spool, epoch = epoch.0, "spool: creating state for newly assigned spool");

                    // If we don't have a state for a spool but it's assigned to us, we need to
                    // create an initial state and start syncing it. The initial state will have
                    // the previous owner set, which allows the sync task to know where to sync
                    // from.

                    let state = make_sync_state(self.context.as_ref(), spool, epoch);

                    self.context
                        .store
                        .set_spool_state(spool, state)
                        .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;
                }

                // Assigned, have state AND epoch matches -> resume or continue
                (true, Some(_state)) if _state.epoch == epoch => {}

                // Assigned, have state AND epoch doesn't match -> (sync)
                (true, Some(_state)) => {
                    info!(spool, epoch = epoch.0, "spool: refreshing assigned spool for new epoch");

                    reset_spool_state(self.context.as_ref(), spool)?;

                    let state = make_sync_state(self.context.as_ref(), spool, epoch);

                    self.context
                        .store
                        .set_spool_state(spool, state)
                        .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;
                }
            }
        }

        Ok(())
    }

    /// Check if the spool manager already has a running task for the given spool index.
    fn is_running(&self, spool: SpoolIndex) -> bool {
        self.workers.contains_key(&spool)
    }

    /// Stop all workers
    async fn stop(&mut self) {
        for (_, token) in self.workers.drain() {
         token.cancel();
        }

        while self.join_set.join_next().await.is_some() {}
    }
}


pub(crate) fn has_pending_work<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    spool: SpoolIndex,
) -> Result<(bool, bool), NodeError> {
    let has_repair = !store
        .iter_pending_repairs(spool, 1)
        .map_err(|e| NodeError::Store(format!("iter_pending_repairs({spool}): {e}")))?
        .is_empty();
    let has_recovery = !store
        .iter_pending_recoveries(spool, 1)
        .map_err(|e| NodeError::Store(format!("iter_pending_recoveries({spool}): {e}")))?
        .is_empty();
    Ok((has_repair, has_recovery))
}

fn action_for_status(
    spool: SpoolIndex,
    epoch: EpochNumber,
    status: SpoolStatus,
) -> Option<Action> {
    match status {
        SpoolStatus::Sync => Some(Action::Sync { spool, epoch }),
        SpoolStatus::Scan => Some(Action::Scan { spool, epoch }),
        SpoolStatus::Repair => Some(Action::Repair { spool, epoch }),
        SpoolStatus::Recover => Some(Action::Recover { spool, epoch }),
        SpoolStatus::Active | SpoolStatus::LockedToMove => None,
    }
}

fn status_priority(status: SpoolStatus) -> u8 {
    match status {
        SpoolStatus::Sync => 0,
        SpoolStatus::Scan => 1,
        SpoolStatus::Repair => 2,
        SpoolStatus::Recover => 3,
        SpoolStatus::Active => 4,
        SpoolStatus::LockedToMove => 5,
    }
}

fn transition_status<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    spool: SpoolIndex,
    action: Action,
    result: TaskResult,
) -> Result<Option<SpoolStatus>, NodeError> {
    match (action, result) {
        (Action::Sync { .. }, TaskResult::Sync(_)) => {
            Ok(Some(SpoolStatus::Scan))
        }

        (Action::Scan { .. }, TaskResult::Scan(ScanResult::Retry)) => {
            Ok(Some(SpoolStatus::Scan))
        }

        (Action::Scan { .. }, TaskResult::Scan(ScanResult::Done { .. })) => {
            reconcile(store, spool).map(Some)
        }

        (Action::Repair { .. }, TaskResult::Repair(_)) => {
            reconcile(store, spool).map(Some)
        }

        (Action::Recover { .. }, TaskResult::Recover(_)) => {
            reconcile(store, spool).map(Some)
        }

        _ => Ok(None),
    }
}

fn reconcile<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    spool: SpoolIndex,
) -> Result<SpoolStatus, NodeError> {
    let (has_repair, has_recovery) = has_pending_work(store, spool)?;
    if has_repair {
        Ok(SpoolStatus::Repair)
    } else if has_recovery {
        Ok(SpoolStatus::Recover)
    } else {
        Ok(SpoolStatus::Active)
    }
}

fn make_sync_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    epoch: EpochNumber,
) -> SpoolState {
    let protocol = ctx.state();
    let group = GroupIndex::containing(spool);
    let mut state = SpoolState::new(SpoolStatus::Sync, epoch);

    state.prev_owner = protocol.spool_owner_prev(spool);
    for slice in 0..GROUP_SIZE {
        state.prev_helpers[slice] = protocol.spool_owner_prev(group.spool_at(slice));
    }

    state
}

fn reset_spool_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
) -> Result<(), NodeError> {

    ctx.store
        .clear_all_pending_repairs(spool)
        .map_err(|e| NodeError::Store(format!("clear_all_pending_repairs({spool}): {e}")))?;

    ctx.store
        .clear_all_pending_recoveries(spool)
        .map_err(|e| NodeError::Store(format!("clear_all_pending_recoveries({spool}): {e}")))?;

    ctx.store
        .remove_spool_sync_cursor(spool)
        .map_err(|e| NodeError::Store(format!("remove_spool_sync_cursor({spool}): {e}")))?;

    Ok(())
}

fn purge_locked_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
) -> Result<(), NodeError> {

    ctx.store
        .delete_all_slices_for_spool(spool)
        .map_err(|e| NodeError::Store(format!("delete_all_slices_for_spool({spool}): {e}")))?;

    reset_spool_state(ctx, spool)?;

    ctx.store
        .remove_spool_state(spool)
        .map_err(|e| NodeError::Store(format!("remove_spool_state({spool}): {e}")))?;

    Ok(())
}

fn check_expiry(
    locked_epoch: EpochNumber,
    current_epoch: EpochNumber,
    retention_epochs: u64,
) -> bool {
    current_epoch
        .saturating_sub(locked_epoch)
        .as_u64() >= retention_epochs
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;
    use tape_core::spooler::{SpoolAssignment, SpoolIndex};
    use tape_core::system::{CommitteeMember, EpochPhase};
    use tape_core::types::EpochNumber;
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::NodeId;
    use tape_protocol::ProtocolState;
    use tape_store::ops::SpoolOps;
    use tape_core::system::{SpoolState, SpoolStatus};
    use tokio_util::sync::CancellationToken;

    use super::SpoolManager;
    use crate::config::recovery::RecoveryConfig;
    use crate::context::test_utils::test_context;
    use crate::features::spool::types::{Action, RepairResult, ScanResult, SyncResult, TaskDone, TaskResult};
    use tape_core::erasure::SPOOL_COUNT;
    const EPOCH: EpochNumber = EpochNumber(2);

    fn owned_state(spools: &[SpoolIndex]) -> ProtocolState {
        let mut state = ProtocolState::default();
        state.epoch = EPOCH;
        state.phase = EpochPhase::Syncing;
        state
            .committee
            .push(CommitteeMember::new(NodeId(0), Coin::<TAPE>::new(1000)));

        let mut mapping = [255u8; SPOOL_COUNT];
        for &spool in spools {
            mapping[spool as usize] = 0;
        }
        state.spools = SpoolAssignment::new(mapping);
        state
    }

    #[test]
    fn advance_creates_sync_state_for_new_spool() {
        let ctx = test_context();
        ctx.set_state(owned_state(&[5])).unwrap();

        let manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.advance(EPOCH).unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Sync);
        assert_eq!(state.epoch, EPOCH);
    }

    #[test]
    fn next_action_prefers_sync_then_lowest_spool() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(7, SpoolState::new(SpoolStatus::Repair, EPOCH))
            .unwrap();
        ctx.store
            .set_spool_state(6, SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();

        let manager = SpoolManager::new(
            ctx,
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        assert_eq!(
            manager.next_action(EPOCH).unwrap(),
            Some(Action::Sync {
                spool: 5,
                epoch: EPOCH,
            })
        );
    }

    #[test]
    fn handle_task_done_advances_state() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Sync {
                        spool: 5,
                        epoch: EPOCH,
                    },
                    TaskResult::Sync(SyncResult::Done {
                        synced_tracks: 0,
                        synced_slices: 3,
                    }),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Scan);
        assert_eq!(state.epoch, EPOCH);
    }

    #[test]
    fn active_pending_repair() {
        let ctx = test_context();
        ctx.set_state(owned_state(&[5])).unwrap();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store.add_pending_repair(5, Address::from([1; 32])).unwrap();

        let manager = SpoolManager::new(ctx, RecoveryConfig::default(), CancellationToken::new());
        assert_eq!(
            manager.next_action(EPOCH).unwrap(),
            Some(Action::Repair { spool: 5, epoch: EPOCH })
        );
    }

    #[test]
    fn active_pending_recovery() {
        let ctx = test_context();
        ctx.set_state(owned_state(&[5])).unwrap();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store.add_pending_recovery(5, Address::from([1; 32])).unwrap();

        let manager = SpoolManager::new(ctx, RecoveryConfig::default(), CancellationToken::new());
        assert_eq!(
            manager.next_action(EPOCH).unwrap(),
            Some(Action::Recover { spool: 5, epoch: EPOCH })
        );
    }

    #[test]
    fn reconcile_stays_repair_with_pending() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Scan, EPOCH))
            .unwrap();
        ctx.store.add_pending_repair(5, Address::from([1; 32])).unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Scan { spool: 5, epoch: EPOCH },
                    TaskResult::Scan(ScanResult::Done { gaps: 0 }),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Repair);
    }

    #[test]
    fn scan_retry_stays_scan() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Scan, EPOCH))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Scan { spool: 5, epoch: EPOCH },
                    TaskResult::Scan(ScanResult::Retry),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Scan);
    }

    #[test]
    fn reconcile_active_when_empty() {
        let ctx = test_context();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Repair, EPOCH))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Repair { spool: 5, epoch: EPOCH },
                    TaskResult::Repair(RepairResult::Done { unrepairable: 0 }),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Active);
    }

    #[tokio::test]
    async fn try_spawn_persists_repair_before_worker() {
        let ctx = test_context();
        ctx.set_state(owned_state(&[5])).unwrap();
        ctx.store
            .set_spool_state(5, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store.add_pending_repair(5, Address::from([1; 32])).unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.try_spawn(EPOCH).unwrap();

        let state = ctx.store.get_spool_state(5).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Repair);
        assert!(manager.is_running(5));
    }
}
