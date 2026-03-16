use std::collections::BTreeMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::erasure::{SPOOL_COUNT, SPOOL_GROUP_SIZE};
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::{EpochNumber, NodeId};
use tape_protocol::{Api, ProtocolState};
use tape_store::ops::SpoolOps;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::{ChannelName, ServiceName};
use crate::features::spool::planner::desired_work;
use crate::features::spool::reducer::apply_event;
use crate::features::spool::types::{SpoolAssignment, SpoolEvent, SpoolTaskSummary, SpoolWorkItem};
use crate::features::spool::worker::run_spool_worker;

struct ActiveWorker {
    work: SpoolWorkItem,
    cancel: CancellationToken,
}

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    rx: mpsc::Receiver<SpoolEvent>,
    cancel: CancellationToken,
    semaphore: Arc<Semaphore>,
    workers: JoinSet<Result<(SpoolWorkItem, Option<SpoolTaskSummary>), NodeError>>,
    active: BTreeMap<SpoolIndex, ActiveWorker>,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SpoolManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SpoolManagerConfig,
        rx: mpsc::Receiver<SpoolEvent>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            semaphore: Arc::new(Semaphore::new(config.max_parallel_spools)),
            config,
            rx,
            cancel,
            workers: JoinSet::new(),
            active: BTreeMap::new(),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            max_parallel_spools = self.config.max_parallel_spools,
            "spool manager started"
        );

        let mut state_rx = self.context.subscribe_state();
        let mut observed_epoch = state_rx.borrow().epoch;

        self.reconcile_epoch(observed_epoch).await?;
        self.replan_all();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    self.shutdown_workers().await?;
                    return Ok(());
                }
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        self.shutdown_workers().await?;
                        return Ok(());
                    }

                    let current_epoch = state_rx.borrow().epoch;
                    if current_epoch > observed_epoch {
                        observed_epoch = current_epoch;
                        self.reconcile_epoch(current_epoch).await?;
                        self.replan_all();
                    }
                }
                received = self.rx.recv() => {
                    let Some(event) = received else {
                        self.shutdown_workers().await?;
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SpoolManager })
                        };
                    };

                    let spool_id = spool_id(&event);
                    apply_event(self.context.store.as_ref(), &self.config, &event)?;
                    self.replan_spool(spool_id);
                }
                joined = self.workers.join_next(), if !self.workers.is_empty() => {
                    if let Some(result) = joined {
                        self.handle_worker_exit(result)?;
                    }
                }
            }
        }
    }

    async fn reconcile_epoch(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        self.cancel_all_workers().await?;

        let state = self.context.state();
        info!(epoch = epoch.0, "reconciling spool ownership");

        for spool_id in 0..SPOOL_COUNT as SpoolIndex {
            let event = epoch_reconcile_event(state.as_ref(), self.context.node_id(), spool_id);
            apply_event(self.context.store.as_ref(), &self.config, &event)?;
        }

        Ok(())
    }

    fn replan_all(&mut self) {
        for spool_id in 0..SPOOL_COUNT as SpoolIndex {
            self.replan_spool(spool_id);
        }
    }

    fn replan_spool(&mut self, spool_id: SpoolIndex) {
        let current = match self.context.store.get_spool_state(spool_id) {
            Ok(state) => state,
            Err(error) => {
                warn!(spool_id, error = %error, "failed to read spool state for planning");
                return;
            }
        };

        let desired = desired_work(spool_id, current);
        let active = self.active.get(&spool_id).map(|worker| worker.work);

        if active == desired {
            return;
        }

        if let Some(worker) = self.active.get(&spool_id) {
            worker.cancel.cancel();
            return;
        }

        if let Some(work) = desired {
            self.spawn_worker(work);
        }
    }

    fn spawn_worker(&mut self, work: SpoolWorkItem) {
        let cancel = self.cancel.child_token();
        let assignment = SpoolAssignment {
            work,
            cancel: cancel.clone(),
        };

        self.active.insert(
            work.spool_id,
            ActiveWorker {
                work,
                cancel,
            },
        );

        let context = self.context.clone();
        let config = self.config.clone();
        let semaphore = Arc::clone(&self.semaphore);

        self.workers
            .spawn(async move { run_spool_worker(context, config, assignment, semaphore).await });
    }

    async fn cancel_all_workers(&mut self) -> Result<(), NodeError> {
        for worker in self.active.values() {
            worker.cancel.cancel();
        }

        while !self.active.is_empty() {
            let Some(result) = self.workers.join_next().await else {
                break;
            };
            self.handle_worker_exit(result)?;
        }

        Ok(())
    }

    async fn shutdown_workers(&mut self) -> Result<(), NodeError> {
        self.cancel_all_workers().await
    }

    fn handle_worker_exit(
        &mut self,
        result: Result<
            Result<(SpoolWorkItem, Option<SpoolTaskSummary>), NodeError>,
            tokio::task::JoinError,
        >,
    ) -> Result<(), NodeError> {
        match result {
            Ok(Ok((work, summary))) => {
                self.active.remove(&work.spool_id);

                if let Some(summary) = summary {
                    let event = SpoolEvent::TaskSummary { work, summary };
                    apply_event(self.context.store.as_ref(), &self.config, &event)?;
                }

                self.replan_spool(work.spool_id);
                Ok(())
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(NodeError::ServiceJoin {
                service: ServiceName::SpoolManager,
                source: error,
            }),
        }
    }
}

fn spool_id(event: &SpoolEvent) -> SpoolIndex {
    match event {
        SpoolEvent::EpochReconcile { spool_id, .. } => *spool_id,
        SpoolEvent::TaskSummary { work, .. } => work.spool_id,
        SpoolEvent::MissingCertifiedSlice { spool_id, .. } => *spool_id,
    }
}

fn epoch_reconcile_event(
    state: &ProtocolState,
    node_id: NodeId,
    spool_id: SpoolIndex,
) -> SpoolEvent {
    let group = SpoolGroup::of(spool_id);
    let mut prev_helpers = [None; SPOOL_GROUP_SIZE];

    for (peer_spool, peer_node_id) in state.group_peers_prev(group) {
        if let Some(index) = group.slice_of(peer_spool) {
            prev_helpers[index] = Some(peer_node_id);
        }
    }

    SpoolEvent::EpochReconcile {
        spool_id,
        epoch: state.epoch,
        owned: state.spool_owner(spool_id) == Some(node_id),
        prev_owner: state.spool_owner_prev(spool_id),
        prev_helpers,
    }
}

#[cfg(test)]
mod tests {
    use tape_core::system::{CommitteeMember, EpochPhase};
    use tape_core::types::coin::{Coin, TAPE};
    use tape_core::types::{EpochNumber, NodeId};
    use tape_protocol::ProtocolState;

    use super::epoch_reconcile_event;

    #[test]
    fn epoch_reconcile_populates_previous_helpers() {
        let mut state = ProtocolState {
            epoch: EpochNumber(7),
            phase: EpochPhase::Active,
            ..ProtocolState::default()
        };

        state
            .committee
            .push(CommitteeMember::new(NodeId(11), Coin::<TAPE>::new(100)));
        state
            .committee_prev
            .push(CommitteeMember::new(NodeId(21), Coin::<TAPE>::new(100)));
        state
            .committee_prev
            .push(CommitteeMember::new(NodeId(22), Coin::<TAPE>::new(100)));
        state.spools.0[5] = 0;
        state.spools_prev.0[0] = 0;
        state.spools_prev.0[1] = 1;

        let event = epoch_reconcile_event(&state, NodeId(11), 5);

        match event {
            crate::features::spool::types::SpoolEvent::EpochReconcile {
                owned,
                prev_owner,
                prev_helpers,
                ..
            } => {
                assert!(owned);
                assert_eq!(prev_owner, Some(NodeId(21)));
                assert_eq!(prev_helpers[0], Some(NodeId(21)));
                assert_eq!(prev_helpers[1], Some(NodeId(22)));
            }
            _ => panic!("unexpected event"),
        }
    }
}
