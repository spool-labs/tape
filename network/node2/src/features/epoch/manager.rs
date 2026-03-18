use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::config::EpochManagerConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::epoch::handlers::EpochHandlers;

pub struct EpochManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: EpochManagerConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> EpochManager<Db, Cluster, Blockchain> {

    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: EpochManagerConfig,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let handlers = EpochHandlers::new(
            self.context.clone(),
            self.config.clone(),
            self.cancel.clone(),
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::EpochManager })
                        };
                    };

                    self.handle_block(&handlers, block).await?;
                }
            }
        }
    }

    async fn handle_block(
        &self,
        handlers: &EpochHandlers<Db, Cluster, Blockchain>,
        block: Arc<ParsedBlock>,
    ) -> Result<(), NodeError> {
        debug!(slot = block.slot.0, "epoch manager received block");

        for instruction in &block.instructions {
            match instruction {
                ParsedInstruction::AdvanceEpoch { event } => {
                    handlers.handle_advance_epoch(event.new_epoch).await?;
                }
                ParsedInstruction::SyncEpoch { event } => {
                    handlers.handle_sync_epoch(event.epoch, event.phase).await?;
                }
                ParsedInstruction::AdvancePool { node, event } => {
                    handlers.handle_advance_pool(*node, event.epoch, event.phase).await?;
                }
                ParsedInstruction::JoinNetwork { event, .. } => {
                    handlers.handle_join_network(event.id).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
