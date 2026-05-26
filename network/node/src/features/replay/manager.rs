use std::sync::Arc;

use rpc::Rpc;
use store::Store;
use tape_protocol::Api;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::core::channels::send_replay_batch;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::ingestor::ParsedBlock;
use crate::features::replay::engine::ReplayEngine;
use crate::features::replay::types::ReplayBatch;

pub struct ReplayManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    rx: mpsc::Receiver<Arc<ParsedBlock>>,
    store_tx: mpsc::Sender<ReplayBatch>,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> ReplayManager<Db, Cluster, Blockchain> {
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        rx: mpsc::Receiver<Arc<ParsedBlock>>,
        store_tx: mpsc::Sender<ReplayBatch>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            rx,
            store_tx,
            cancel,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        debug!(
            node_id = self.context.node_id().0,
            "replay manager started"
        );

        let mut replay = ReplayEngine::new(
            self.context.store.as_ref(),
            self.context.state().epoch(),
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

                    self.persist_block(&mut replay, block).await?;
                }
            }
        }
    }

    async fn persist_block(
        &self,
        replay: &mut ReplayEngine<'_, Db>,
        block: Arc<ParsedBlock>,
    ) -> Result<(), NodeError> {
        let (batch, event_count) = replay.capture_and_journal(&block)?;

        send_replay_batch(&self.store_tx, batch).await?;
        self.context.metrics.add_events(event_count as u64);

        debug!(
            node_id = self.context.node_id().0,
            slot = block.slot.0,
            journaled = event_count,
            next_epoch = replay.current_epoch().0,
            "replay journal persisted"
        );

        Ok(())
    }
}
