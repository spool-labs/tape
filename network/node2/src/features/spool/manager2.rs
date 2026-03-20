use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_core::system::EpochPhase;
use tape_core::types::NodeId;
use tape_core::spooler::SpoolIndex;
use tape_core::erasure::SPOOL_COUNT;
use tape_store::types::{SpoolState, SpoolStatus};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::SpoolOps;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::SpoolManagerConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::features::spool::types::{Action, TaskDone};

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    cancel: CancellationToken,

    workers: HashMap<SpoolIndex, CancellationToken>,
    join_set: JoinSet<TaskDone>,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SpoolManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SpoolManagerConfig,
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
    pub async fn run(&mut self) -> Result<(), NodeError> {

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;

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

                    //self.try_spawn(observed_epoch);
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

                    //self.try_spawn(observed_epoch);
                }

                // Periodic heartbeat
                _ = tokio::time::sleep(self.config.interval) => {
                    //self.try_spawn(observed_epoch);
                }
            }
        }
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

    /// Determine the next action based on current state.
    pub fn next_action(
        &self,
        epoch: EpochNumber,
    ) -> Result<Option<Action>, NodeError> {

        // 0) figure out if we have capacity to run a task (not too many already running)
        // 1) find spool states that are in the current epoch 
        // (we don't launch tasks for spool states that are from previous epochs)
        // 2) figure out which are not being worked on currently 
        // (no active task for that spool in the join set already)
        // 3) decide which one to spawn a task for (choose a random one for now)
        // return the action

        Ok(None)
    }


    /// Try to spawn the next pending action — but only if nothing is currently running.
    fn try_spawn(
        &self,
        epoch: EpochNumber,
    ) {
        // 1) check the state of our spool assignments and what we've done so far, and decide if we
        // need to spawn a task.
        
        // 2) ensure we don't spawn multiple tasks for the same spool at the same time, even if we get
        // multiple signals (state change, task completion, heartbeat) in quick succession.


        loop {
            let Some(action) = self.next_action(epoch).ok() else {
                return;
            };

            let ctx = self.context.clone();
            let token = self.cancel.child_token();

            info!(?action, epoch = epoch.0, "spool: spawning task");

            // tasks.spawn(async move {
            //     match action {
            //         Action::Sync    => sync::run(ctx, epoch, token).await,
            //         Action::Scan    => scan::run(ctx, epoch, token).await,
            //         Action::Repair  => repair::run(ctx, epoch, token).await,
            //         Action::Recover => recover::run(ctx, epoch, token).await,
            //     }
            // });
            //
            // *running = Some(action);
        }
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
                    // If we lost ownership of a spool, we need to lock it to prevent any ongoing
                    // or future tasks from doing work on it.

            //         info!(?spool, "spool: locking spool due to lost ownership");
            //
            //         let Some(mut state) = state.cloned() else {
            //             return Err(NodeError::Store(format!("lock_spool: no state for spool {spool}")));
            //         };
            //
            //         self.context.store.clear_all_pending_repairs(spool)
            //             .map_err(|e| NodeError::Store(format!("clear_all_pending_repairs: {e}")))?;
            //
            //         self.context.store.clear_all_pending_recoveries(spool)
            //             .map_err(|e| NodeError::Store(format!("clear_all_pending_recoveries: {e}")))?;
            //
            //         self.context.store.remove_spool_sync_cursor(spool)
            //             .map_err(|e| NodeError::Store(format!("remove_spool_sync_cursor: {e}")))?;
            //
            //         state.set_status(SpoolStatus::LockedToMove);
            //         state.set_epoch(epoch);
            //
            //         self.context.store.set_spool_state(spool, state)
            //             .map_err(|e| NodeError::Store(format!("set_spool_state: {e}")))?;
                }

                // Assigned, no state → create state -> (sync)
                (true, None) => {
                    // If we got assigned a new spool, we need to create a state for it and start
                    // syncing. This also requires finding the previous owner (if any) and helpers
                    // from the previous epoch.
                }

                // Assigned, have state AND epoch matches -> resume or continue
                (true, Some(state)) if state.epoch == epoch => {
                    // If we still own the spool and the epoch matches, we can resume or continue
                    // whatever we were doing based on the status.

                    // Dont need to reset the cursors or pending queues since we should be resuming
                    // where we left off
                }

                // Assigned, have state AND epoch doesn't match -> (sync)
                (true, Some(state)) => {
                    // If we still own the spool but the epoch doesn't match, we need to update the
                    // state to reflect the new epoch and start syncing again to ensure we're in
                    // sync with the current state of the world.

                    // Sync falls through to scan, which will either find no gaps and mark Active,
                    // or find gaps and go into repair.

                    // Reset the cursors and pending queues to be safe
                }
            }
        }

        Ok(())
    }
}

