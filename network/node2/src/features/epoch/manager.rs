use std::sync::Arc;
use mpsc::Receiver;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;


pub struct EpochManager {
    context: AppContext,
    config: EpochManagerConfig,
    rx: Receiver<Arc<ParsedBlock>>,
    cancel: CancellationToken,
}

impl EpochManager {
    pub fn new(
        context: AppContext,
        config: EpochManagerConfig,
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
        handlers: &EpochHandlers,
        block: Arc<ParsedBlock>,
    ) -> Result<(), NodeError> {
        debug!(height = block.height.0, "epoch manager received block");

        for parsed in &block.extracted {
            match parsed.instruction {
                ProtocolInstruction::AdvanceEpoch { epoch, .. } => {
                    handlers.handle_advance_epoch(epoch).await?;
                }
                ProtocolInstruction::SyncEpoch { epoch, .. } => {
                    handlers.handle_sync_epoch(epoch).await?;
                }
                ProtocolInstruction::AdvancePool { spool_id, .. } => {
                    handlers.handle_advance_pool(spool_id).await?;
                }
                ProtocolInstruction::JoinNetwork { node_id, .. } => {
                    handlers.handle_join_network(node_id).await?;
                }
                _ => {}
            }
        }

        Ok(())
    }
}
