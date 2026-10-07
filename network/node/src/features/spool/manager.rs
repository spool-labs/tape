use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_core::prelude::{EpochNumber, VirtualGroup, SpoolIndex, SpoolState, SpoolStatus};
use tape_core::spooler::GroupFold;
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::types::{Action, ScanResult, TaskDone, TaskResult, TaskSpool};
use crate::features::spool::{recover, repair, scan, sync};

const SPOOL_MANAGER_HEARTBEAT: Duration = Duration::from_secs(1);
const LOCKED_SPOOL_RETENTION_EPOCHS: u64 = 4;

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: RecoveryConfig,
    cancel: CancellationToken,
    workers: HashMap<TaskSpool, CancellationToken>,
    join_set: JoinSet<TaskDone>,
    queued: VecDeque<Action>,
    has_advanced: bool,
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
        let queued = VecDeque::new();
        let has_advanced = false;

        Self {
            context,
            config,
            cancel,
            workers,
            join_set,
            queued,
            has_advanced,
        }
    }

    /// Spawn tasks based on state changes and task completions.
    pub async fn run(mut self) -> Result<(), NodeError> {

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch();

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

                    if state.epoch() != observed_epoch {
                        info!(
                            old_epoch = observed_epoch.0,
                            new_epoch = state.epoch().0,
                            "spool: epoch advanced, resetting"
                        );

                        self.stop().await;

                        observed_epoch = state.epoch();

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

    fn due_actions(
        &self,
        epoch: EpochNumber,
    ) -> Result<Vec<Action>, NodeError> {
        let fold = self.context.state().fold_at(epoch);

        let mut spools = self
            .context
            .store
            .iter_all_spools()
            .map_err(|e| NodeError::Store(format!("iter_all_spools: {e}")))?;

        spools
            .sort_unstable_by_key(
                |(spool, state)| (status_priority(state.status), *spool)
        );

        let mut listed = HashSet::new();
        let mut actions = Vec::new();
        for (spool, state) in spools {
            if state.epoch != epoch {
                continue;
            }

            // Sync and Scan run once for a live spool. Rows sort Sync first, so
            // a live spool with rows in both states syncs before it scans.
            let task_spool = match state.status {
                SpoolStatus::Sync | SpoolStatus::Scan => {
                    let Some(fold) = fold else {
                        continue;
                    };

                    TaskSpool::Live(fold.live_spool(spool))
                }
                SpoolStatus::Repair
                | SpoolStatus::Recover
                | SpoolStatus::Active
                | SpoolStatus::LockedToMove => TaskSpool::Virtual(spool),
            };

            if self.is_running(task_spool) || !listed.insert(task_spool) {
                continue;
            }

            if let Some(action) = action_for_status(task_spool.index(), epoch, state.status) {
                actions.push(action);
                continue;
            }

            // Active spools: check if pending work appeared (e.g. from CertifyTrack hook)
            if state.status == SpoolStatus::Active {
                let (has_repair, has_recovery) = has_pending_work(&self.context.store, spool)?;
                if has_repair {
                    actions.push(Action::Repair { spool, epoch });
                } else if has_recovery {
                    actions.push(Action::Recover { spool, epoch });
                }
            }
        }

        Ok(actions)
    }

    fn next_queued(
        &mut self,
        epoch: EpochNumber,
    ) -> Result<Option<Action>, NodeError> {
        if self.queued.is_empty() {
            self.queued = self.due_actions(epoch)?.into();
        }

        Ok(self.queued.pop_front())
    }

    /// Try to spawn new tasks up to the parallel limit.
    fn try_spawn(
        &mut self,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        while self.workers.len() < self.config.max_workers {
            let Some(action) = self.next_queued(epoch)? else {
                break;
            };

            // Check if we're already running a task for this spool
            let task_spool = action.task_spool();
            if self.is_running(task_spool) {
                continue;
            }

            // The list was read a while ago, so the rows are read again: a spool
            // whose rows no longer ask for this action is left alone.
            let rows = self.rows_for(action)?;
            if !rows.iter().any(|(_, state)| is_due(action, epoch, state)) {
                continue;
            }

            // If the spool is Active but we're about to run work, persist the transition.
            for (spool, mut state) in rows {
                if state.status != SpoolStatus::Active {
                    continue;
                }

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

            let ctx = self.context.clone();
            let config = self.config.clone();
            let token = self.cancel.child_token();

            self.workers.insert(task_spool, token.clone());

            info!(?action, epoch = epoch.0, "spool: spawning task");

            self.join_set.spawn(
                async move {
                    let result = match action {
                        Action::Sync { spool, epoch } => {
                            TaskResult::Sync(sync::run(ctx, &config, spool, epoch, &token).await)
                        }
                        Action::Scan { spool, epoch } => {
                            TaskResult::Scan(scan::run(ctx, &config, spool, epoch, &token).await)
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
                }
                .in_current_span(),
            );
        }

        Ok(())
    }

    fn handle_done(
        &mut self,
        done: TaskDone,
        observed_epoch: EpochNumber,
    ) -> Result<(), NodeError> {
        let action = done.action();
        let worker_epoch = action.epoch();

        self.workers.remove(&action.task_spool());

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
                let rows = self.rows_for(action)?;
                if rows.is_empty() {
                    debug!(?action, "spool: missing state for completed task");
                    return Ok(());
                }

                let spools = rows.len();
                for (spool, mut state) in rows {
                    let Some(next_status) = transition_status(&self.context.store, spool, action, result)? else {
                        warn!(?action, ?result, "spool: invalid task completion");
                        return Ok(());
                    };

                    state.set_status(next_status);

                    self.context
                        .store
                        .set_spool_state(spool, state)
                        .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;

                    debug!(spool = %spool, status = ?next_status, "spool: row updated");
                }

                info!(?action, epoch = observed_epoch.0, spools, "spool: task completed");
            }
            TaskDone::Cancelled(action, result) => {
                debug!(?action, ?result, "spool: task cancelled");
            }
        }

        Ok(())
    }

    /// Update the spool states for the epoch; the first pass after start sends every assigned spool to Sync
    pub fn advance(
        &mut self,
        epoch: EpochNumber,
    ) -> Result<(), NodeError> {

        // The rows are rewritten below, so a list read from them earlier is void.
        self.queued.clear();

        // Get the spools we've been assigned for the current epoch
        let assignments: HashSet<SpoolIndex> = self.context.my_spools();
        let persisted = self
            .context
            .store
            .iter_all_spools()
            .map_err(|e| NodeError::Store(format!("iter_all_spools: {e}")))?;

        let mut spools = assignments.clone();
        spools.extend(persisted.iter().map(|(spool, _)| *spool));

        for spool in spools {

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
                            info!(spool = %spool, epoch = epoch.0, "spool: purging locked spool after retention period");

                            purge_locked_spool(self.context.as_ref(), spool)?;
                        }
                    } else {
                        info!(spool = %spool, epoch = epoch.0, "spool: locking spool due to lost ownership");

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
                    info!(spool = %spool, epoch = epoch.0, "spool: creating state for newly assigned spool");

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
                (true, Some(state)) if self.has_advanced && state.epoch == epoch => {}

                // Assigned, have state -> (sync, or keep what we hold)
                (true, Some(state)) => {
                    let next = match self.has_advanced {
                        true => refresh_state(self.context.as_ref(), spool, epoch, state.status)?,
                        false => make_sync_state(self.context.as_ref(), spool, epoch),
                    };

                    info!(spool = %spool, epoch = epoch.0, status = ?next.status, "spool: refreshing assigned spool for new epoch");

                    self.context
                        .store
                        .set_spool_state(spool, next)
                        .map_err(|e| NodeError::Store(format!("set_spool_state({spool}): {e}")))?;
                }
            }
        }

        self.has_advanced = true;

        Ok(())
    }

    /// List the rows a task for this action works on.
    fn rows_for(
        &self,
        action: Action,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>, NodeError> {
        let status = match action {
            Action::Sync { .. } => SpoolStatus::Sync,
            Action::Scan { .. } => SpoolStatus::Scan,
            Action::Repair { spool, .. } | Action::Recover { spool, .. } => {
                let state = self
                    .context
                    .store
                    .get_spool_state(spool)
                    .map_err(|e| NodeError::Store(format!("get_spool_state({spool}): {e}")))?;

                return Ok(state.map(|state| (spool, state)).into_iter().collect());
            }
        };

        let Some(fold) = self.context.state().fold_at(action.epoch()) else {
            return Ok(Vec::new());
        };

        carried_rows(&self.context.store, fold, action.spool(), action.epoch(), status)
    }

    /// Check if the spool manager already has a running task for the given spool.
    fn is_running(&self, task_spool: TaskSpool) -> bool {
        self.workers.contains_key(&task_spool)
    }

    /// Stop all workers
    async fn stop(&mut self) {
        for (_, token) in self.workers.drain() {
         token.cancel();
        }

        while self.join_set.join_next().await.is_some() {}
    }
}


/// The stored spools a live spool carries whose rows are in `status` this epoch
pub fn carried_rows<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    fold: GroupFold,
    spool: SpoolIndex,
    epoch: EpochNumber,
    status: SpoolStatus,
) -> Result<Vec<(SpoolIndex, SpoolState)>, NodeError> {
    let mut rows = Vec::new();

    for stored in fold.carried_spools(spool) {
        let state = store
            .get_spool_state(stored)
            .map_err(|e| NodeError::Store(format!("get_spool_state({stored}): {e}")))?;

        match state {
            Some(state) if state.epoch == epoch && state.status == status => {
                rows.push((stored, state));
            }
            Some(_) | None => {}
        }
    }

    Ok(rows)
}

/// The work a spool still owes
pub fn has_pending_work<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    spool: SpoolIndex,
) -> Result<(bool, bool), NodeError> {
    let has_repair = has_repair_due(store, spool)?;
    let has_recovery = !store
        .iter_pending_recoveries(spool, 1)
        .map_err(|e| NodeError::Store(format!("iter_pending_recoveries({spool}): {e}")))?
        .is_empty();
    Ok((has_repair, has_recovery))
}

fn has_repair_due<Db: Store>(
    store: &tape_store::TapeStore<Db>,
    spool: SpoolIndex,
) -> Result<bool, NodeError> {
    let queued = store
        .iter_pending_repairs(spool, usize::MAX)
        .map_err(|e| NodeError::Store(format!("iter_pending_repairs({spool}): {e}")))?;

    for track in queued {
        let is_held = store
            .has_slice(spool, track)
            .map_err(|e| NodeError::Store(format!("has_slice({spool}): {e}")))?;

        if !is_held {
            return Ok(true);
        }
    }

    Ok(false)
}

fn is_due(action: Action, epoch: EpochNumber, state: &SpoolState) -> bool {
    if action.epoch() != epoch || state.epoch != epoch {
        return false;
    }

    // An Active spool's work comes from its pending queues, so its row only
    // has to still be Active.
    let is_pending_work = state.status == SpoolStatus::Active
        && matches!(action, Action::Repair { .. } | Action::Recover { .. });

    is_pending_work || action_for_status(action.spool(), epoch, state.status) == Some(action)
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
    let group = VirtualGroup::containing(spool);
    let mut state = SpoolState::new(SpoolStatus::Sync, epoch);

    state.prev_owner = protocol.spool_owner_prev(spool);
    for slice in 0..ctx.params().n() {
        state.prev_helpers[slice] = protocol.spool_owner_prev(group.spool_at(slice));
    }

    state
}

fn refresh_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    epoch: EpochNumber,
    status: SpoolStatus,
) -> Result<SpoolState, NodeError> {
    let has_synced = match status {
        SpoolStatus::Active | SpoolStatus::Repair | SpoolStatus::Recover => true,
        SpoolStatus::Sync | SpoolStatus::Scan | SpoolStatus::LockedToMove => false,
    };
    let is_same_owner = ctx.state().spool_owner_prev(spool) == Some(ctx.node_address());

    let mut state = make_sync_state(ctx, spool, epoch);

    if has_synced && is_same_owner {
        state.set_status(reconcile(&ctx.store, spool)?);
    }

    Ok(state)
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
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::VirtualGroup;
    use tape_core::system::{EpochPhase, SpoolState, SpoolStatus};
    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use tape_core::types::{EpochNumber, SpoolIndex};
    use tape_crypto::address::Address;
    use tape_store::ops::{SliceOps, SpoolOps};
    use tape_store_memory::MemoryStore;
    use tokio_util::sync::CancellationToken;

    use super::SpoolManager;
    use crate::config::recovery::RecoveryConfig;
    use crate::features::spool::types::{
        Action, RepairResult, ScanResult, SyncResult, TaskDone, TaskResult, TaskSpool,
    };
    use crate::harness::{NodeHarness, TestContext};

    type TestManager = SpoolManager<MemoryStore, MemoryApi, LiteSvmRpc>;

    const EPOCH: EpochNumber = EpochNumber(2);
    const SPOOL: SpoolIndex = SpoolIndex(5);

    /// Spools in a tapenet group.
    const WIDTH: usize = TAPENET_PROFILE.n() as usize;

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    async fn context_with_prev_owners(prev_owners: Vec<usize>) -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .prev_committee_nodes(prev_owners)
            .prev_group_count(1)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(SPOOL.as_usize())
    }

    /// Forty nodes. Last epoch ran two groups, nodes 0 to 19 in group 0 and
    /// 20 to 39 in group 1. This epoch runs one group on nodes 0 to 19.
    async fn shrunk_harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(2 * WIDTH)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .prev_committee_nodes(0..2 * WIDTH)
            .prev_group_count(2)
            .current_group_count(1)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
    }

    /// The stored spool of the retired group that the node at `SPOOL` now carries.
    fn absorbed_spool() -> SpoolIndex {
        VirtualGroup(1).spool_at(SPOOL.as_usize())
    }

    // after the group count drops, a node pulls the retired group's spool at
    // its position from the node that held it, and keeps its own
    #[tokio::test]
    async fn absorbed_spool_syncs_from_old_holder() {
        let harness = shrunk_harness().await;
        let ctx = harness.ctx_for(SPOOL.as_usize());
        let old_holder = harness.ctx_for(WIDTH + SPOOL.as_usize()).node_address();

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());
        manager.advance(EPOCH).unwrap();

        let own = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(own.prev_owner, Some(ctx.node_address()));

        let absorbed = ctx.store.get_spool_state(absorbed_spool()).unwrap().unwrap();
        assert_eq!(absorbed.status, SpoolStatus::Sync);
        assert_eq!(absorbed.prev_owner, Some(old_holder));
        assert_eq!(
            absorbed.prev_helpers[0],
            Some(harness.ctx_for(WIDTH).node_address()),
        );
    }

    // the node that held a retired group's spool keeps it locked for the new holder to pull
    #[tokio::test]
    async fn old_holder_locks_retired_spool() {
        let harness = shrunk_harness().await;
        let ctx = harness.ctx_for(WIDTH + SPOOL.as_usize());
        let track = Address::from([7; 32]);

        ctx.store
            .set_spool_state(absorbed_spool(), SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        ctx.store.put_slice(absorbed_spool(), track, vec![1, 2, 3]).unwrap();

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());
        manager.advance(EPOCH).unwrap();

        let state = ctx.store.get_spool_state(absorbed_spool()).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::LockedToMove);
        assert!(ctx.store.has_slice(absorbed_spool(), track).unwrap());
    }

    // the epoch after the count dropped, a carried spool was this node's own
    // last epoch too, so it keeps what it holds and syncs nothing
    #[tokio::test]
    async fn carried_spool_kept_next_epoch() {
        let harness = NodeHarness::builder()
            .nodes(2 * WIDTH)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .prev_committee_nodes(0..WIDTH)
            .prev_group_count(1)
            .current_group_count(1)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(SPOOL.as_usize());

        // One group is live, so every other virtual group is folded onto
        // group 0 in this epoch and the last.
        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());
        manager.advance(EPOCH).unwrap();

        for spool in [SPOOL, absorbed_spool()] {
            ctx.store
                .set_spool_state(spool, SpoolState::new(SpoolStatus::Active, EPOCH))
                .unwrap();
        }

        manager.advance(EPOCH.next()).unwrap();

        for spool in [SPOOL, absorbed_spool()] {
            let state = ctx.store.get_spool_state(spool).unwrap().unwrap();
            assert_eq!(state.status, SpoolStatus::Active, "spool {spool}");
            assert_eq!(state.epoch, EPOCH.next());
        }
    }

    #[tokio::test]
    async fn advance_creates_sync_state_for_new_spool() {
        let ctx = test_context().await;

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.advance(EPOCH).unwrap();

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Sync);
        assert_eq!(state.epoch, EPOCH);
    }

    #[tokio::test]
    async fn advance_refresh_preserves_pending_queues() {
        let ctx = test_context().await;
        let track = Address::from([7; 32]);

        // Assigned spool persisted under a stale epoch, with detected repair work
        // queued.
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .unwrap();
        ctx.store.add_pending_repair(SPOOL, track).unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.advance(EPOCH).unwrap();

        // Re-entered Sync for the new epoch, but the repair queue survives the
        // refresh.
        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Sync);
        assert_eq!(state.epoch, EPOCH);
        assert!(ctx.store.has_pending_repair(SPOOL, track).unwrap());
    }

    // a sync is listed ahead of a repair, and the lowest spool first
    #[tokio::test]
    async fn sync_listed_first() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SpoolIndex(7), SpoolState::new(SpoolStatus::Repair, EPOCH))
            .unwrap();
        ctx.store
            .set_spool_state(SpoolIndex(6), SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Sync, EPOCH))
            .unwrap();

        let manager = SpoolManager::new(
            ctx,
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        assert_eq!(
            manager.due_actions(EPOCH).unwrap(),
            [
                Action::Sync { spool: SPOOL, epoch: EPOCH },
                Action::Sync { spool: SpoolIndex(6), epoch: EPOCH },
                Action::Repair { spool: SpoolIndex(7), epoch: EPOCH },
            ],
        );
    }

    #[tokio::test]
    async fn handle_task_done_advances_state() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Sync, EPOCH))
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
                        spool: SPOOL,
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

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Scan);
        assert_eq!(state.epoch, EPOCH);
    }

    #[tokio::test]
    async fn active_pending_repair() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store
            .add_pending_repair(SPOOL, Address::from([1; 32]))
            .unwrap();

        let manager = SpoolManager::new(ctx, RecoveryConfig::default(), CancellationToken::new());
        assert_eq!(
            manager.due_actions(EPOCH).unwrap().first().copied(),
            Some(Action::Repair { spool: SPOOL, epoch: EPOCH })
        );
    }

    // a repair queued past the first page of rows still puts the spool in Repair
    #[tokio::test]
    async fn deep_pending_repair() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EPOCH))
            .expect("spool state stored");

        // A page of rows whose slice has already arrived, and one behind them
        // that has not.
        for byte in 1..=64u8 {
            let track = Address::from([byte; 32]);
            ctx.store.add_pending_repair(SPOOL, track).expect("repair queued");
            ctx.store
                .put_slice(SPOOL, track, vec![byte; 32])
                .expect("slice stored");
        }
        let due = Address::from([200; 32]);
        ctx.store.add_pending_repair(SPOOL, due).expect("repair queued");

        let manager = SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());

        assert_eq!(
            manager.due_actions(EPOCH).expect("action read").first().copied(),
            Some(Action::Repair { spool: SPOOL, epoch: EPOCH })
        );
        // The read leaves the queue alone; the repair worker drains it.
        assert!(ctx.store.has_pending_repair(SPOOL, Address::from([1; 32])).expect("row read"));
    }

    #[tokio::test]
    async fn active_pending_recovery() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store
            .add_pending_recovery(SPOOL, Address::from([1; 32]))
            .unwrap();

        let manager = SpoolManager::new(ctx, RecoveryConfig::default(), CancellationToken::new());
        assert_eq!(
            manager.due_actions(EPOCH).unwrap().first().copied(),
            Some(Action::Recover { spool: SPOOL, epoch: EPOCH })
        );
    }

    #[tokio::test]
    async fn reconcile_stays_repair_with_pending() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Scan, EPOCH))
            .unwrap();
        ctx.store
            .add_pending_repair(SPOOL, Address::from([1; 32]))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Scan { spool: SPOOL, epoch: EPOCH },
                    TaskResult::Scan(ScanResult::Done { gaps: 0 }),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Repair);
    }

    #[tokio::test]
    async fn scan_retry_stays_scan() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Scan, EPOCH))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Scan { spool: SPOOL, epoch: EPOCH },
                    TaskResult::Scan(ScanResult::Retry),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Scan);
    }

    #[tokio::test]
    async fn reconcile_active_when_empty() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Repair, EPOCH))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Repair { spool: SPOOL, epoch: EPOCH },
                    TaskResult::Repair(RepairResult::Done { unrepairable: 0 }),
                ),
                EPOCH,
            )
            .unwrap();

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Active);
    }

    #[tokio::test]
    async fn try_spawn_persists_repair_before_worker() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EPOCH))
            .unwrap();
        ctx.store
            .add_pending_repair(SPOOL, Address::from([1; 32]))
            .unwrap();

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.try_spawn(EPOCH).unwrap();

        let state = ctx.store.get_spool_state(SPOOL).unwrap().unwrap();
        assert_eq!(state.status, SpoolStatus::Repair);
        assert!(manager.is_running(TaskSpool::Virtual(SPOOL)));
    }

    /// A manager that runs one task at a time over three spools waiting to sync.
    async fn single_worker() -> (TestContext, TestManager, [SpoolIndex; 3]) {
        let ctx = test_context().await;
        let spools = [SpoolIndex(5), SpoolIndex(6), SpoolIndex(7)];
        for spool in spools {
            ctx.store
                .set_spool_state(spool, SpoolState::new(SpoolStatus::Sync, EPOCH))
                .expect("set spool state");
        }

        let config = RecoveryConfig { max_workers: 1, ..RecoveryConfig::default() };
        let manager = SpoolManager::new(ctx.clone(), config, CancellationToken::new());

        (ctx, manager, spools)
    }

    fn finish_sync(manager: &mut TestManager, spool: SpoolIndex) {
        let action = Action::Sync { spool, epoch: EPOCH };
        let result = TaskResult::Sync(SyncResult::Done { synced_tracks: 0, synced_slices: 0 });

        manager
            .handle_done(TaskDone::Done(action, result), EPOCH)
            .expect("handle done");
    }

    // one read of the rows lists every spool's work, and tasks start from that
    // list in order as workers come free
    #[tokio::test]
    async fn tasks_start_from_one_read() {
        let (_ctx, mut manager, spools) = single_worker().await;

        manager.try_spawn(EPOCH).expect("try spawn");
        assert!(manager.is_running(TaskSpool::Live(spools[0])));
        assert_eq!(
            manager.queued,
            [
                Action::Sync { spool: spools[1], epoch: EPOCH },
                Action::Sync { spool: spools[2], epoch: EPOCH },
            ],
        );

        finish_sync(&mut manager, spools[0]);
        manager.try_spawn(EPOCH).expect("try spawn");

        assert!(manager.is_running(TaskSpool::Live(spools[1])));
        assert_eq!(manager.queued, [Action::Sync { spool: spools[2], epoch: EPOCH }]);
    }

    // a spool whose row changed while its action waited in the list is left alone
    #[tokio::test]
    async fn changed_row_skipped() {
        let (ctx, mut manager, spools) = single_worker().await;
        manager.try_spawn(EPOCH).expect("try spawn");

        ctx.store
            .set_spool_state(spools[1], SpoolState::new(SpoolStatus::Active, EPOCH))
            .expect("set spool state");
        finish_sync(&mut manager, spools[0]);
        manager.try_spawn(EPOCH).expect("try spawn");

        assert!(!manager.is_running(TaskSpool::Live(spools[1])));
        assert!(manager.is_running(TaskSpool::Live(spools[2])));
    }

    // one task syncs every stored spool a live spool carries, and its end moves all their rows on
    #[tokio::test]
    async fn one_live_sync() {
        let ctx = test_context().await;
        for spool in [SPOOL, absorbed_spool()] {
            ctx.store
                .set_spool_state(spool, SpoolState::new(SpoolStatus::Sync, EPOCH))
                .expect("set spool state");
        }

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());

        assert_eq!(
            manager.due_actions(EPOCH).expect("due actions"),
            [Action::Sync { spool: SPOOL, epoch: EPOCH }],
        );

        finish_sync(&mut manager, SPOOL);

        for spool in [SPOOL, absorbed_spool()] {
            let state = ctx.store.get_spool_state(spool).expect("get spool state").expect("spool state");
            assert_eq!(state.status, SpoolStatus::Scan, "spool {spool}");
        }
    }

    // a live spool with rows still waiting to sync does that before it scans
    #[tokio::test]
    async fn sync_before_scan() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Scan, EPOCH))
            .expect("set spool state");
        ctx.store
            .set_spool_state(absorbed_spool(), SpoolState::new(SpoolStatus::Sync, EPOCH))
            .expect("set spool state");

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());

        assert_eq!(
            manager.due_actions(EPOCH).expect("due actions"),
            [Action::Sync { spool: SPOOL, epoch: EPOCH }],
        );

        finish_sync(&mut manager, SPOOL);

        assert_eq!(
            manager.due_actions(EPOCH).expect("due actions"),
            [Action::Scan { spool: SPOOL, epoch: EPOCH }],
        );
    }

    // a scan's end settles each carried row from that stored spool's own queues
    #[tokio::test]
    async fn scan_settles_rows() {
        let ctx = test_context().await;
        for spool in [SPOOL, absorbed_spool()] {
            ctx.store
                .set_spool_state(spool, SpoolState::new(SpoolStatus::Scan, EPOCH))
                .expect("set spool state");
        }
        ctx.store
            .add_pending_repair(absorbed_spool(), Address::from([1; 32]))
            .expect("add pending repair");

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());

        manager
            .handle_done(
                TaskDone::Done(
                    Action::Scan { spool: SPOOL, epoch: EPOCH },
                    TaskResult::Scan(ScanResult::Done { gaps: 1 }),
                ),
                EPOCH,
            )
            .expect("handle done");

        let own = ctx.store.get_spool_state(SPOOL).expect("get spool state").expect("spool state");
        let absorbed = ctx
            .store
            .get_spool_state(absorbed_spool())
            .expect("get spool state")
            .expect("spool state");
        assert_eq!(own.status, SpoolStatus::Active);
        assert_eq!(absorbed.status, SpoolStatus::Repair);
    }

    // a live spool's sync and a repair on the stored spool of the same number run as two tasks
    #[tokio::test]
    async fn same_number() {
        let ctx = test_context().await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EPOCH))
            .expect("set spool state");
        ctx.store
            .add_pending_repair(SPOOL, Address::from([1; 32]))
            .expect("add pending repair");
        ctx.store
            .set_spool_state(absorbed_spool(), SpoolState::new(SpoolStatus::Sync, EPOCH))
            .expect("set spool state");

        let mut manager =
            SpoolManager::new(ctx.clone(), RecoveryConfig::default(), CancellationToken::new());
        manager.try_spawn(EPOCH).expect("try spawn");

        assert!(manager.is_running(TaskSpool::Live(SPOOL)));
        assert!(manager.is_running(TaskSpool::Virtual(SPOOL)));
    }

    // a new epoch drops the list read in the old one
    #[tokio::test]
    async fn advance_drops_the_list() {
        let (_ctx, mut manager, _spools) = single_worker().await;
        manager.try_spawn(EPOCH).expect("try spawn");
        assert!(!manager.queued.is_empty());

        manager.advance(EPOCH.next()).expect("advance");

        assert!(manager.queued.is_empty());
    }

    // the first pass re-syncs a kept spool, a later epoch turn keeps what it holds
    #[tokio::test]
    async fn kept_spool_active() {
        let next_epoch = EpochNumber(3);
        let ctx = context_with_prev_owners((0..WIDTH).collect()).await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .expect("set spool state");

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );

        manager.advance(EPOCH).expect("advance");

        let first = ctx
            .store
            .get_spool_state(SPOOL)
            .expect("get spool state")
            .expect("spool state");
        assert_eq!(first.status, SpoolStatus::Sync);
        assert_eq!(first.epoch, EPOCH);

        // the sync and scan pass finished and left every stored spool Active
        for spool in ctx.state().member_spools(ctx.node_address()) {
            ctx.store
                .set_spool_state(spool, SpoolState::new(SpoolStatus::Active, EPOCH))
                .expect("set spool state");
        }

        manager.advance(next_epoch).expect("advance");

        let second = ctx
            .store
            .get_spool_state(SPOOL)
            .expect("get spool state")
            .expect("spool state");
        assert_eq!(second.status, SpoolStatus::Active);
        assert_eq!(second.epoch, next_epoch);
        assert_eq!(second.prev_owner, Some(ctx.node_address()));
        assert!(manager.due_actions(next_epoch).expect("due actions").is_empty());
    }

    // a queued repair puts the kept spool straight into Repair
    #[tokio::test]
    async fn kept_spool_repair() {
        let ctx = context_with_prev_owners((0..WIDTH).collect()).await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .expect("set spool state");
        ctx.store
            .add_pending_repair(SPOOL, Address::from([9; 32]))
            .expect("add pending repair");

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );
        // past the first pass, where every assigned spool goes back to Sync
        manager.has_advanced = true;

        manager.advance(EPOCH).expect("advance");

        let state = ctx
            .store
            .get_spool_state(SPOOL)
            .expect("get spool state")
            .expect("spool state");
        assert_eq!(state.status, SpoolStatus::Repair);
        assert_eq!(state.epoch, EPOCH);
    }

    // a spool taken over from another node starts in Sync
    #[tokio::test]
    async fn moved_spool_syncs() {
        let ctx = context_with_prev_owners((1..=WIDTH).collect()).await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Active, EpochNumber(1)))
            .expect("set spool state");

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );
        // past the first pass, where every assigned spool goes back to Sync
        manager.has_advanced = true;

        manager.advance(EPOCH).expect("advance");

        let state = ctx
            .store
            .get_spool_state(SPOOL)
            .expect("get spool state")
            .expect("spool state");
        assert_ne!(state.prev_owner, Some(ctx.node_address()));
        assert_eq!(state.status, SpoolStatus::Sync);
        assert_eq!(state.epoch, EPOCH);
    }

    // a spool the epoch turn interrupted mid-sync starts over
    #[tokio::test]
    async fn interrupted_sync_restarts() {
        let ctx = context_with_prev_owners((0..WIDTH).collect()).await;
        ctx.store
            .set_spool_state(SPOOL, SpoolState::new(SpoolStatus::Sync, EpochNumber(1)))
            .expect("set spool state");

        let mut manager = SpoolManager::new(
            ctx.clone(),
            RecoveryConfig::default(),
            CancellationToken::new(),
        );
        // past the first pass, where every assigned spool goes back to Sync
        manager.has_advanced = true;

        manager.advance(EPOCH).expect("advance");

        let state = ctx
            .store
            .get_spool_state(SPOOL)
            .expect("get spool state")
            .expect("spool state");
        assert_eq!(state.status, SpoolStatus::Sync);
        assert_eq!(state.epoch, EPOCH);
    }
}
