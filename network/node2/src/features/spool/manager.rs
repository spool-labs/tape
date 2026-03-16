use std::collections::HashMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::SpoolOps;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::types::WorkerDone;

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

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SpoolManager<Db, Cluster, Blockchain> {
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

        // Active workers: spool → cancel token for that worker.
        let mut workers: HashMap<SpoolIndex, CancellationToken> = HashMap::new();
        let mut join_set: JoinSet<WorkerDone> = JoinSet::new();

        // Initial plan: spawn workers for all currently owned spools.
        // todo: plan_and_spawn(&self.context, &self.config, observed_epoch, &mut workers, &mut join_set)

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    // Cancel all workers and drain.
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

                    // Cancel all workers from the old epoch.
                    for (_, token) in workers.drain() {
                        token.cancel();
                    }

                    // Drain completions (results are stale, discard).
                    while join_set.join_next().await.is_some() {}

                    // Re-plan for the new epoch.
                    // todo: plan_and_spawn(&self.context, &self.config, current_epoch, &mut workers, &mut join_set)

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

                    // todo: apply_transition(done, &self.context, &self.config, observed_epoch, &mut workers, &mut join_set)
                    //
                    // 1. Extract spool and epoch from WorkerDone.
                    // 2. Remove from workers map.
                    // 3. If the worker's epoch != observed_epoch, discard (stale).
                    // 4. Match on the result variant to determine the FSM transition.
                    // 5. Update persisted SpoolState in the store.
                    // 6. If the transition produces a follow-up task, spawn a new worker.
                }
            }
        }
    }
}
