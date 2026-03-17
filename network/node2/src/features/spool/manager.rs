use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_api::prelude::SpoolGroup;
use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::{SliceOps, SpoolOps};
use tape_store::types::{SpoolState, SpoolStatus};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::fsm::{self, EpochAction};
use crate::features::spool::types::{TaskKind, WorkerDone};
use crate::features::spool::worker;

// Spool Manager
//
// Owns up to 50 concurrent spool workers (one per assigned spool).
// Each worker runs one step of the FSM for its spool (Sync, Scan, Repair, or Recover).
//
// Inputs:
//   - watch::Receiver<ProtocolState> — epoch/phase changes from the epoch manager.
//
// Lifecycle:
//   1. On startup, read current protocol state to determine our assigned spools.
//      For each spool, read persisted SpoolState from the store to determine
//      which FSM step to run. Spawn initial workers.
//
//   2. Main loop selects on:
//      a. cancel — shutdown.
//      b. state_rx.changed() — epoch advanced.
//         - If epoch changed: cancel all workers for the old epoch, wait for
//           them to complete (they check cancellation at batch boundaries),
//           then re-plan: compute new spool assignments, read persisted state,
//           spawn new workers.
//      c. join_set.join_next() — a worker completed.
//         - Apply the FSM transition based on the WorkerDone result.
//         - Update the spool's persisted SpoolState.
//         - If the FSM produces a follow-up task (e.g. Scan→Repair), spawn
//           a new worker for that spool immediately.
//
// Worker management:
//   - `workers: HashMap<SpoolIndex, CancellationToken>` — tracks which spools
//     have an active worker and provides a handle to cancel them individually.
//   - `join_set: JoinSet<WorkerDone>` — collects worker futures for completion
//     notification.
//   - Workers are keyed by (spool, epoch). If the epoch changes, all existing
//     workers are cancelled because spool ownership may have changed.
//
// Epoch transition:
//   1. Cancel all running workers (drop their CancellationTokens or call cancel).
//   2. Drain join_set to collect any completions (results are discarded since
//      the epoch they belong to is no longer current).
//   3. Compute new spool assignments from the updated ProtocolState.
//   4. For each newly owned spool:
//      - If we already had it last epoch and it was Active → Scan (re-verify).
//      - If it's new to us → Sync (fetch from previous owner).
//      - If it was mid-lifecycle (Scan/Repair/Recover) → restart from Scan.
//      Read the persisted SpoolState to decide. The FSM epoch event rules
//      in fsm.rs govern this.
//   5. For each spool we no longer own:
//      - Mark as LockedToMove in the store. Data is retained for
//        `locked_spool_retention_epochs` to serve repair requests from
//        the new owner.
//   6. Spawn workers for all owned spools.
//
// FSM transitions on worker completion:
//   See fsm.rs for the full table. Key transitions:
//
//   Sync  + Done          → set Scan, spawn scan worker
//   Sync  + Unavailable   → set Scan, spawn scan worker
//   Scan  + Done { 0 }    → set Active, no worker needed
//   Scan  + Done { > 0 }  → set Repair, spawn repair worker
//   Repair + Done { 0 }   → set Active, no worker needed
//   Repair + Done { > 0 } → set Recover, spawn recover worker
//   Recover + Done { 0 }  → set Active, no worker needed
//   Recover + Done { > 0 } → set Recover, spawn recover worker (retry)

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SpoolManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SpoolManagerConfig,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            max_parallel = self.config.max_parallel_spools,
            "spool manager started"
        );

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;

        let mut workers: HashMap<SpoolIndex, CancellationToken> = HashMap::new();
        let mut join_set: JoinSet<WorkerDone> = JoinSet::new();

        // Initial spawn for all currently owned spools
        plan_and_spawn(
            &self.context,
            &self.config,
            observed_epoch,
            &mut workers,
            &mut join_set,
        )?;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    // Cancel all workers and exit
                    for (_, token) in workers.drain() {
                        token.cancel();
                    }
                    while join_set.join_next().await.is_some() {}
                    return Ok(());
                }

                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return Ok(());
                    }

                    let current_epoch = state_rx.borrow().epoch;
                    if current_epoch <= observed_epoch {
                        continue;
                    }

                    info!(
                        old_epoch = observed_epoch.0,
                        new_epoch = current_epoch.0,
                        active_workers = workers.len(),
                        "epoch advanced, cancelling workers"
                    );

                    // Cancel all workers from the old epoch 
                    for (_, token) in workers.drain() {
                        token.cancel();
                    }

                    // Drain join_set to collect completions of cancelled workers 
                    // (results are discarded)
                    while join_set.join_next().await.is_some() {}

                    // Re-plan and spawn new workers for the new epoch
                    plan_and_spawn(
                        &self.context,
                        &self.config,
                        current_epoch,
                        &mut workers,
                        &mut join_set,
                    )?;

                    observed_epoch = current_epoch;
                }

                Some(result) = join_set.join_next() => {
                    let done = match result {
                        Ok(done) => done,
                        Err(error) => {
                            warn!(error = %error, "spool worker panicked");
                            continue;
                        }
                    };

                    apply_transition(
                        done,
                        &self.context,
                        &self.config,
                        observed_epoch,
                        &mut workers,
                        &mut join_set,
                    )?;
                }
            }
        }
    }
}

fn plan_and_spawn<Db, Cluster, Blockchain>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    epoch: EpochNumber,
    workers: &mut HashMap<SpoolIndex, CancellationToken>,
    join_set: &mut JoinSet<WorkerDone>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let owned_spools = ctx.my_spools();
    let mut owned_sorted: Vec<_> = owned_spools.iter().copied().collect();
    owned_sorted.sort_unstable();

    let persisted = ctx
        .store
        .iter_all_spools()
        .map_err(|error| NodeError::Store(format!("iter_all_spools: {error}")))?;
    let mut persisted_map: HashMap<SpoolIndex, SpoolState> = persisted.into_iter().collect();

    // Handle currently owned spools first
    for spool in owned_sorted {
        let persisted_state = persisted_map.remove(&spool);

        match fsm::on_epoch_event(persisted_state.as_ref(), true, epoch) {
            EpochAction::Idle => {}
            EpochAction::Lock => {
                // unreachable for owned spools
                warn!(spool, "unexpected lock action for owned spool");
            }
            EpochAction::Spawn { kind, update } => {
                // Epoch changed or new spool — clean stale task state
                if update.is_some() {
                    reset_spool_task_state(ctx, spool);
                }

                let state = if kind == TaskKind::Sync {
                    make_sync_state(ctx, spool, epoch)
                } else if let Some(s) = update {
                    s
                } else {
                    // Resume: no state update needed, just spawn
                    if workers.len() < config.max_parallel_spools && !workers.contains_key(&spool) {
                        spawn_worker(ctx, config, spool, epoch, kind, workers, join_set);
                    }
                    continue;
                };

                ctx.store
                    .set_spool_state(spool, state)
                    .map_err(|error| NodeError::Store(format!("set_spool_state({spool}): {error}")))?;

                // Spawn worker if we don't already have one for this spool and haven't hit the max
                // parallel limit

                if workers.len() >= config.max_parallel_spools || workers.contains_key(&spool) {
                    continue;
                }

                spawn_worker(ctx, config, spool, epoch, kind, workers, join_set);
            }
        }
    }

    // Handle spools we no longer own
    let mut retained: Vec<_> = persisted_map.into_iter().collect();
    retained.sort_unstable_by_key(|(spool, _)| *spool);

    for (spool, state) in retained {
        match fsm::on_epoch_event(Some(&state), false, epoch) {
            EpochAction::Lock => {
                reset_spool_task_state(ctx, spool);
                ctx.store
                    .set_spool_state(spool, SpoolState::new(SpoolStatus::LockedToMove, epoch))
                    .map_err(|error| NodeError::Store(format!("lock_spool({spool}): {error}")))?;
            }
            EpochAction::Idle => {
                // Already LockedToMove — check retention expiry
                if state.is_locked()
                    && retention_expired(state.epoch, epoch, config.locked_spool_retention_epochs)
                {
                    purge_locked_spool(ctx, spool)?;
                }
            }
            EpochAction::Spawn { .. } => {
                // unreachable for non-owned spools
                warn!(spool, "unexpected spawn action for non-owned spool");
            }
        }
    }

    Ok(())
}

fn apply_transition<Db, Cluster, Blockchain>(
    done: WorkerDone,
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    epoch: EpochNumber,
    workers: &mut HashMap<SpoolIndex, CancellationToken>,
    join_set: &mut JoinSet<WorkerDone>,
) -> Result<(), NodeError>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    let (spool, worker_epoch) = match &done {
        WorkerDone::Sync(s, e, _) => (*s, *e),
        WorkerDone::Scan(s, e, _) => (*s, *e),
        WorkerDone::Repair(s, e, _) => (*s, *e),
        WorkerDone::Recover(s, e, _) => (*s, *e),
    };

    workers.remove(&spool);

    if worker_epoch != epoch {
        return Ok(());
    }

    let Some(mut state) = ctx
        .store
        .get_spool_state(spool)
        .map_err(|error| NodeError::Store(format!("get_spool_state({spool}): {error}")))?
    else {
        return Ok(());
    };

    let (next_status, next_task) = fsm::on_task_result(&done);

    state.status = next_status;
    ctx.store
        .set_spool_state(spool, state)
        .map_err(|error| NodeError::Store(format!("set_spool_state({spool}): {error}")))?;

    if next_status == SpoolStatus::Active {
        info!(spool, epoch = epoch.0, "spool active");
    }

    if let Some(kind) = next_task {
        if workers.len() < config.max_parallel_spools && !workers.contains_key(&spool) {
            spawn_worker(ctx, config, spool, epoch, kind, workers, join_set);
        }
    }

    Ok(())
}

fn spawn_worker<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: &SpoolManagerConfig,
    spool: SpoolIndex,
    epoch: EpochNumber,
    kind: TaskKind,
    workers: &mut HashMap<SpoolIndex, CancellationToken>,
    join_set: &mut JoinSet<WorkerDone>,
) {
    let cancel = CancellationToken::new();
    workers.insert(spool, cancel.clone());
    join_set.spawn(worker::run(
        ctx.clone(),
        config.clone(),
        kind,
        spool,
        epoch,
        cancel,
    ));
}

fn make_sync_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
    epoch: EpochNumber,
) -> SpoolState {
    let protocol = ctx.state();
    let group = SpoolGroup::of(spool);
    let mut state = SpoolState::new(SpoolStatus::Sync, epoch);

    state.prev_owner = protocol.spool_owner_prev(spool);
    for slice in 0..SPOOL_GROUP_SIZE {
        state.prev_helpers[slice] = protocol
            .spool_owner_prev(group.spool_at(slice));
    }
    state
}

fn reset_spool_task_state<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
) {
    let _ = ctx.store.clear_all_pending_repairs(spool);
    let _ = ctx.store.clear_all_pending_recoveries(spool);
    let _ = ctx.store.remove_spool_sync_cursor(spool);
}

fn purge_locked_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &NodeContext<Db, Cluster, Blockchain>,
    spool: SpoolIndex,
) -> Result<(), NodeError> {
    ctx.store
        .delete_all_slices_for_spool(spool)
        .map_err(|error| NodeError::Store(format!("delete_all_slices_for_spool({spool}): {error}")))?;
    reset_spool_task_state(ctx, spool);
    ctx.store
        .remove_spool_state(spool)
        .map_err(|error| NodeError::Store(format!("remove_spool_state({spool}): {error}")))
}

fn retention_expired(
    locked_epoch: EpochNumber,
    current_epoch: EpochNumber,
    retention_epochs: u64,
) -> bool {
    current_epoch.0.saturating_sub(locked_epoch.0) >= retention_epochs
}
