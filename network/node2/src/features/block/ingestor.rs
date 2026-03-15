use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::SlotNumber;
use tape_protocol::Api;
use tape_retry::retry_if;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::core::channels::{DownstreamSenders, send_block};
use crate::core::config::BlockIngestorConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;

#[derive(Debug)]
pub struct ParsedBlock {
    pub slot: SlotNumber,
    pub instructions: Vec<ParsedInstruction>,
}

pub struct BlockIngestor<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: BlockIngestorConfig,
    senders: DownstreamSenders,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> 
    BlockIngestor<Db, Cluster, Blockchain> {

    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
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
        let mut next_slot = self.config.start_slot;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                result = self.fetch_parse_and_dispatch(next_slot) => {
                    result?;
                    next_slot = SlotNumber(next_slot.0.saturating_add(1));
                }
            }
        }
    }

    async fn fetch_parse_and_dispatch(&self, slot: SlotNumber) -> Result<(), NodeError> {
        let context = self.context.clone();

        let block = retry_if(
            self.config.fetch_retry.clone(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                async move { context.rpc.get_block(slot.0).await }
            },
            |error| error.is_retriable() && !error.is_skipped_slot(),
        )
        .await;

        let block = match block {
            Ok(block) => block,
            Err(error) if error.is_skipped_slot() => {
                debug!(slot = slot.0, "slot skipped");
                return Ok(());
            }
            Err(error) => return Err(NodeError::from(error)),
        };

        let instructions = tape_blocks::parse_and_merge(&block)?;
        let block = Arc::new(ParsedBlock { slot, instructions });

        debug!(
            slot = block.slot.0,
            extracted = block.instructions.len(),
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
            Arc::clone(&block)
        ).await?;

        info!(slot = slot.0, "dispatched parsed block");
        Ok(())
    }
}
