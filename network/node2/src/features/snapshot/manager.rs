use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::config::SnapshotConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: SnapshotConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SnapshotManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: SnapshotConfig,
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
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "snapshot manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::SnapshotManager })
                        };
                    };

                    self.handle_block(block).await?;
                }
            }
        }
    }

    async fn handle_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        info!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            entries = block.instructions.len(),
            "snapshot state persisted"
        );

        Ok(())
    }
}
