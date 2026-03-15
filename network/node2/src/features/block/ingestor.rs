use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

pub struct BlockIngestor {
    context: AppContext,
    config: BlockIngestorConfig,
    senders: DownstreamSenders,
    cancel: CancellationToken,
}

impl BlockIngestor {
    pub fn new(
        context: AppContext,
        config: BlockIngestorConfig,
        senders: DownstreamSenders,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            senders,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let mut next_height = self.config.start_height;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                result = self.fetch_parse_and_dispatch(next_height) => {
                    result?;
                    next_height = next_height.next();
                }
            }
        }
    }

    async fn fetch_parse_and_dispatch(&self, height: BlockHeight) -> Result<(), NodeError> {
        let context = self.context.clone();
        let bytes = retry_if(
            self.config.fetch_retry.clone(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move { context.rpc.get_block_binary(height).await }
            },
            NodeError::is_retryable,
        )
        .await?;

        let block = Arc::new(parse_block_bytes(&bytes)?);

        debug!(
            height = block.height.0,
            extracted = block.extracted.len(),
            "parsed block"
        );

        send_block(
            &self.senders.epoch,
            ChannelName::EpochManager,
            Arc::clone(&block),
        )
        .await?;

        send_block(
            &self.senders.spool,
            ChannelName::SpoolManager,
            Arc::clone(&block),
        )
        .await?;

        send_block(
            &self.senders.snapshot,
            ChannelName::SnapshotManager,
            Arc::clone(&block),
        )
        .await?;

        send_block(
            &self.senders.replay, 
            ChannelName::ReplayManager, 
            block
        ).await?;

        info!(height = height.0, "dispatched parsed block");
        Ok(())
    }
}
