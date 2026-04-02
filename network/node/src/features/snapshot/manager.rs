use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::{Api, ProtocolState};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::snapshot::observe::{observe_block, observe_state};

pub struct SnapshotManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> SnapshotManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        snapshot_rx: mpsc::Receiver<Arc<ParsedBlock>>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            snapshot_rx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            "snapshot manager started"
        );

        let mut state_rx = self.context.subscribe_state();

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                changed = state_rx.changed() => {
                    if changed.is_err() {
                        return self.channel_closed_or_cancelled(ChannelName::SnapshotManager);
                    }

                    let state = state_rx.borrow().clone();
                    self.handle_state(state).await?;
                }
                received = self.snapshot_rx.recv() => {
                    let Some(block) = received else {
                        return self.channel_closed_or_cancelled(ChannelName::SnapshotManager);
                    };

                    self.handle_block(block).await?;
                }
            }
        }
    }

    async fn handle_state(&self, state: Arc<ProtocolState>) -> Result<(), NodeError> {
        observe_state(&self.context, state).await
    }

    async fn handle_block(&self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        observe_block(&self.context, block).await
    }

    fn channel_closed_or_cancelled(&self, channel: ChannelName) -> Result<(), NodeError> {
        if self.cancel.is_cancelled() {
            Ok(())
        } else {
            Err(NodeError::ChannelClosed { channel })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::sync::Arc;

    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::SnapshotManager;
    use crate::context::test_utils::test_context;
    use crate::features::block::ingestor::ParsedBlock;

    #[tokio::test]
    async fn returns_when_cancelled() {
        let context = test_context();
        let (_tx, rx) = mpsc::channel::<Arc<ParsedBlock>>(1);

        let cancel = CancellationToken::new();
        let manager = SnapshotManager::new(context, rx, cancel.clone());
        cancel.cancel();

        timeout(Duration::from_secs(1), manager.run())
            .await
            .expect("manager completed in time")
            .expect("manager returned ok");
    }
}
