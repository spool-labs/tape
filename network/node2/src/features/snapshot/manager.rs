use std::sync::Arc;

use mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use tracing::info;


pub struct SnapshotManager {
    context: AppContext,
    config: SnapshotConfig,
    rx: Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl SnapshotManager {
    pub fn new(
        context: AppContext,
        config: SnapshotConfig,
        rx: Receiver<Arc<ParsedBlock>>,
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
            height = block.height.0,
            entries = block.extracted.len(),
            "snapshot state persisted"
        );

        Ok(())
    }
}
