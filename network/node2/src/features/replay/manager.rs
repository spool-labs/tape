use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::config::ReplayConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;

pub struct ReplayManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: ReplayConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ReplayManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: ReplayConfig,
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
            "replay manager started"
        );

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                received = self.rx.recv() => {
                    let Some(block) = received else {
                        return if self.cancel.is_cancelled() {
                            Ok(())
                        } else {
                            Err(NodeError::ChannelClosed { channel: ChannelName::ReplayManager })
                        };
                    };

                    self.persist_block(block).await;
                }
            }
        }
    }

    async fn persist_block(&self, block: Arc<ParsedBlock>) {
        debug!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            entries = block.instructions.len(),
            "replay state persisted"
        );
    }
}
