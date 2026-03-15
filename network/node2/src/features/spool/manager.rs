use std::collections::BTreeMap;
use std::sync::Arc;

use mpsc::Receiver;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};


pub struct SpoolManager {
    context: AppContext,
    config: SpoolManagerConfig,
    rx: Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
    semaphore: Arc<Semaphore>,
    workers: JoinSet<Result<SpoolWorkerExit, NodeError>>,
    active: BTreeMap<SpoolId, CancellationToken>,
    epoch_cancel: CancellationToken,
}

impl SpoolManager {
    pub fn new(
        context: AppContext,
        config: SpoolManagerConfig,
        rx: Receiver<Arc<ParsedBlock>>,
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
        debug!(height = block.height.0, "spool manager received block");

        for parsed in &block.extracted {
            match parsed.instruction {
                ProtocolInstruction::AdvanceEpoch { epoch, .. } => {
                    self.handle_advance_epoch(epoch).await?;
                }
                ProtocolInstruction::AdvancePool { spool_id } => {
                    self.ensure_spool_running(spool_id).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    async fn handle_advance_epoch(&mut self, epoch: EpochId) -> Result<(), NodeError> {
        self.cancel_epoch_workers().await?;
        self.epoch_cancel = self.cancel.child_token();

        let state = self
            .context
            .state
            .wait_for_epoch(epoch, &self.cancel)
            .await?;

        info!(epoch = epoch.0, "re-evaluating owned spools");

        // <todo>
        // let owned = owned_spools(&state, self.context.local_node);

        for spool_id in owned {
            self.spawn_worker(epoch, spool_id);
        }

        Ok(())
    }

    async fn ensure_spool_running(&mut self, spool_id: SpoolId) -> Result<(), NodeError> {
        if self.active.contains_key(&spool_id) {
            return Ok(());
        }

        let state = self.context.state.current();

        // <todo>

        self.spawn_worker(state.epoch, spool_id);

        Ok(())
    }

    fn spawn_worker(&mut self, epoch: EpochId, spool_id: SpoolId) {
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
