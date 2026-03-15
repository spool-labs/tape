use std::collections::BTreeMap;
use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::config::SpoolManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::{ChannelName, ServiceName};
use crate::features::block::ingestor::ParsedBlock;
use crate::features::spool::types::{SpoolAssignment, SpoolWorkerExit};
use crate::features::spool::worker::run_spool_worker;

pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SpoolManagerConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    semaphore: Arc<Semaphore>,
    workers: JoinSet<Result<SpoolWorkerExit, NodeError>>,
    active: BTreeMap<SpoolIndex, CancellationToken>,
    epoch_cancel: CancellationToken,
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static>
    SpoolManager<Db, Cluster, Blockchain>
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SpoolManagerConfig,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_parallel_spools));

        Self {
            context,
            config,
            rx,
            cancel: cancel.clone(),
            semaphore,
            workers: JoinSet::new(),
            active: BTreeMap::new(),
            epoch_cancel: cancel.child_token(),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            max_parallel_spools = self.config.max_parallel_spools,
            "spool manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    self.shutdown_workers().await?;
                    return Ok(());
                }
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        self.shutdown_workers().await?;
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SpoolManager })
                        };
                    };

                    self.handle_block(block).await?;
                }
                joined = self.workers.join_next(), if !self.workers.is_empty() => {
                    if let Some(result) = joined {
                        self.handle_worker_exit(result)?;
                    }
                }
            }
        }
    }

    async fn handle_block(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        debug!(slot = block.slot.0, "spool manager received block");

        for instruction in &block.instructions {
            match instruction {
                ParsedInstruction::AdvanceEpoch { event } => {
                    self.handle_advance_epoch(event.new_epoch).await?;
                }
                ParsedInstruction::AdvancePool { node, event } => {
                    if *node == self.context.node_address() {
                        let state = self.context.state();
                        if let Some((index, _)) = state.find_member(self.context.node_id()) {
                            for spool_id in state.member_spools(index) {
                                self.ensure_spool_running(spool_id).await?;
                            }
                        }
                    }

                    debug!(
                        node = %node,
                        epoch = event.epoch.0,
                        "advance pool observed; spool lifecycle unchanged"
                    );
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_advance_epoch(&mut self, epoch: EpochNumber) -> Result<(), NodeError> {
        self.cancel_epoch_workers().await?;
        self.epoch_cancel = self.cancel.child_token();

        let state = self
            .context
            .state
            .wait_for_epoch(epoch, &self.cancel)
            .await?;

        info!(epoch = epoch.0, "re-evaluating owned spools");

        let owned = match state.find_member(self.context.node_id()) {
            Some((index, _)) => state.member_spools(index),
            None => Vec::new(),
        };

        for spool_id in owned {
            self.spawn_worker(state.epoch, spool_id);
        }

        Ok(())
    }

    async fn ensure_spool_running(&mut self, spool_id: SpoolIndex) -> Result<(), NodeError> {
        if self.active.contains_key(&spool_id) {
            return Ok(());
        }

        let state = self.context.state();
        let owner = state.spool_owner(spool_id);
        if owner != Some(self.context.node_id()) {
            debug!(
                spool_id,
                owner = owner.map(|node_id| node_id.0),
                "skipping worker spawn for unowned spool"
            );
            return Ok(());
        }

        self.spawn_worker(state.epoch, spool_id);

        Ok(())
    }

    fn spawn_worker(&mut self, epoch: EpochNumber, spool_id: SpoolIndex) {
        if self.active.contains_key(&spool_id) {
            return;
        }

        let worker_cancel = self.epoch_cancel.child_token();

        let assignment = SpoolAssignment {
            epoch,
            spool_id,
            cancel: worker_cancel.clone(),
        };

        self.active.insert(spool_id, worker_cancel);

        let context = self.context.clone();
        let config = self.config.clone();
        let semaphore = Arc::clone(&self.semaphore);

        self.workers
            .spawn(async move { 
                run_spool_worker(context, config, assignment, semaphore).await 
            });
    }

    async fn cancel_epoch_workers(&mut self) -> Result<(), NodeError> {
        self.epoch_cancel.cancel();
        self.active.clear();

        while let Some(result) = self.workers.join_next().await {
            self.handle_worker_exit(result)?;
        }

        Ok(())
    }

    async fn shutdown_workers(&mut self) -> Result<(), NodeError> {
        self.epoch_cancel.cancel();
        self.active.clear();

        while let Some(result) = self.workers.join_next().await {
            self.handle_worker_exit(result)?;
        }

        Ok(())
    }

    fn handle_worker_exit(
        &mut self,
        result: Result<Result<SpoolWorkerExit, NodeError>, tokio::task::JoinError>,
    ) -> Result<(), NodeError> {
        match result {
            Ok(Ok(exit)) => {
                self.active.remove(&exit.spool_id);
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
