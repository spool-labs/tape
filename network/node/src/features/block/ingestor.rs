use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::{ParsedInstruction, parse_and_merge};
use tape_core::types::SlotNumber;
use tape_protocol::Api;
use tape_retry::{RetryConfig, retry_if};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::core::channels::{DownstreamSenders, send_block};
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;

#[derive(Debug)]
pub struct ParsedBlock {
    pub slot: SlotNumber,
    pub instructions: Vec<ParsedInstruction>,
}

const TIP_POLL_MS: u64 = 400;

enum IngestStep {
    Continue,
    Wait,
}

pub struct BlockIngestor<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    start_slot: SlotNumber,
    senders: DownstreamSenders,
    cancel: CancellationToken,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> 
    BlockIngestor<Db, Cluster, Blockchain> {

    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        start_slot: SlotNumber,
        senders: DownstreamSenders,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            start_slot,
            senders,
            cancel,
        }
    }

    pub async fn run(self) -> Result<(), NodeError> {
        let mut next_slot = self.start_slot;

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return Ok(()),
                result = self.fetch_parse_and_dispatch(next_slot) => {
                    match result? {
                        IngestStep::Continue => next_slot.increment(),
                        IngestStep::Wait => {}
                    }
                }
            }
        }
    }

    async fn fetch_parse_and_dispatch(&self, slot: SlotNumber) -> Result<IngestStep, NodeError> {
        let tip = match self.context.rpc.get_slot().await {
            Ok(tip) => tip,
            Err(error) => {
                error!(
                    slot = slot.0,
                    error = %error,
                    "block_ingestor: get_slot failed: {}",
                    error
                );
                return Err(NodeError::from(error));
            }
        };

        if slot.0 > tip {
            sleep(Duration::from_millis(TIP_POLL_MS)).await;
            return Ok(IngestStep::Wait);
        }

        let context = self.context.clone();

        let block = retry_if(
            RetryConfig::infinite(),
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
                return Ok(IngestStep::Continue);
            }
            Err(error) => {
                error!(
                    slot = slot.0,
                    error = %error,
                    "block_ingestor: get_block failed: {}",
                    error
                );
                return Err(NodeError::from(error));
            }
        };

        let instructions = match parse_and_merge(&block) {
            Ok(instructions) => instructions,
            Err(error) => {
                error!(
                    slot = slot.0,
                    error = %error,
                    "block_ingestor: parse_and_merge failed: {}",
                    error
                );
                return Err(NodeError::from(error));
            }
        };

        let block = Arc::new(ParsedBlock { slot, instructions });

        debug!(
            slot = block.slot.0,
            extracted = block.instructions.len(),
            "parsed block"
        );

        if let Err(error) = send_block(
            &self.senders.state,
            ChannelName::StateManager,
            Arc::clone(&block),
        )
        .await
        {
            error!(
                slot = slot.0,
                error = %error,
                "block_ingestor: send to StateManager failed: {}",
                error
            );
            return Err(error);
        }

        if let Err(error) = send_block(
            &self.senders.replay, 
            ChannelName::ReplayManager, 
            Arc::clone(&block)
        ).await
        {
            error!(
                slot = slot.0,
                error = %error,
                "block_ingestor: send to ReplayManager failed: {}",
                error
            );
            return Err(error);
        }

        if let Err(error) = send_block(
            &self.senders.snapshot,
            ChannelName::SnapshotManager,
            Arc::clone(&block),
        )
        .await
        {
            error!(
                slot = slot.0,
                error = %error,
                "block_ingestor: send to SnapshotManager failed: {}",
                error
            );
            return Err(error);
        }

        self.context.metrics.inc_blocks_processed();
        info!(slot = slot.0, "dispatched parsed block");
        Ok(IngestStep::Continue)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rpc::Rpc;
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::system::EpochPhase;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store::ops::EventLogOps;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::BlockIngestor;
    use crate::chain::submit_join_network;
    use crate::core::channels::{downstream_channels, store_channel};
    use crate::features::replay::manager::ReplayManager;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

    #[tokio::test]
    async fn forwards_batch() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let confirmed_tip = ctx.rpc.get_slot().await.expect("get confirmed tip");

        submit_join_network(&ctx)
            .await
            .expect("submit join network");
        harness
            .rpc()
            .warp_to_slot(confirmed_tip + 1)
            .expect("confirm produced block");
        let produced_slot = produced_slot(harness.rpc(), confirmed_tip)
            .await
            .expect("discover produced slot");

        let (senders, receivers) = downstream_channels();
        let (store_tx, mut store_rx) = store_channel();
        let replay = ReplayManager::new(
            ctx.clone(),
            receivers.replay,
            store_tx,
            CancellationToken::new(),
        );
        let replay_task = tokio::spawn(replay.run());

        let ingestor = BlockIngestor::new(
            ctx.clone(),
            produced_slot,
            senders,
            CancellationToken::new(),
        );

        ingestor
            .fetch_parse_and_dispatch(produced_slot)
            .await
            .expect("dispatch produced block");

        let batch = timeout(Duration::from_secs(1), store_rx.recv())
            .await
            .expect("receive replay batch in time")
            .expect("replay batch");

        replay_task.abort();
        let _ = replay_task.await;

        assert_eq!(batch.slot, produced_slot);
        assert!(matches!(
            batch.events.as_slice(),
            [ReplayableEvent::JoinNetwork { .. }]
        ));

        let entries = ctx.store.get_epoch_events(EPOCH).expect("get epoch events");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].slot, produced_slot);
        assert!(matches!(
            entries[0].events.as_slice(),
            [ReplayableEvent::JoinNetwork { .. }]
        ));
    }

    async fn produced_slot(rpc: &rpc_litesvm::LiteSvmRpc, confirmed_tip: u64) -> Option<SlotNumber> {
        for slot in [confirmed_tip, confirmed_tip + 1] {
            if rpc.get_block(slot).await.is_ok() {
                return Some(SlotNumber(slot));
            }
        }

        None
    }
}
