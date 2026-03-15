use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_core::types::EpochNumber;
use tape_protocol::Api;
use tape_store::ops::EventLogOps;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::channels::send_replay_batch;
use crate::core::config::ReplayConfig;
use crate::core::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::capture::capture_block;
use crate::features::replay::types::ReplayBatch;

pub struct ReplayManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: ReplayConfig,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    state_tx: mpsc::Sender<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ReplayManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: ReplayConfig,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        state_tx: mpsc::Sender<ReplayBatch>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            config,
            rx,
            state_tx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            config = ?self.config,
            "replay manager started"
        );

        let mut current_epoch = self.context.state().epoch;

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

                    current_epoch = self.persist_block(current_epoch, block).await?;
                }
            }
        }
    }

    async fn persist_block(
        &self,
        current_epoch: EpochNumber,
        block: Arc<ParsedBlock>,
    ) -> Result<EpochNumber, NodeError> {
        let captured = capture_block(current_epoch, &block)?;

        for entry in &captured.events {
            self.context
                .store
                .append_event(entry.epoch, block.slot, &entry.event)
                .map_err(store_error)?;
        }

        let next_epoch = captured.next_epoch;
        let batch = captured.into_batch(block.slot);
        let event_count = batch.events.len();

        send_replay_batch(&self.state_tx, batch).await?;

        debug!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            journaled = event_count,
            next_epoch = next_epoch.0,
            "replay journal persisted"
        );

        Ok(next_epoch)
    }
}

fn store_error(error: impl std::fmt::Display) -> NodeError {
    NodeError::Store(error.to_string())
}
