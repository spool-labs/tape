use std::sync::Arc;
use std::time::Duration;
use futures::StreamExt;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::core::channels::{DownstreamSenders, send_block};
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::fetch::{
    FETCH_PIPELINE_DEPTH, fetch_and_parse_block, fetch_blocks_ordered,
};
use crate::features::block::pending_blocks::{AppendOutcome, PendingBlocks};

const TIP_POLL_MS: u64 = 400;

#[derive(Debug, Default)]
pub struct ParsedBlock {
    pub slot: SlotNumber,
    pub parent_slot: SlotNumber,
    pub blockhash: Hash,
    pub previous_blockhash: Hash,
    pub block_time: Option<i64>,
    pub instructions: Vec<ParsedInstruction>,
    pub instruction_tx_ids: Vec<Txid>,
}

pub struct BlockIngestor<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    start_slot: SlotNumber,
    senders: DownstreamSenders,
    cancel: CancellationToken,
    queue: PendingBlocks,
    /// Most recently observed finalized slot. Refreshed on every iteration
    /// and consulted by the queue promotion check.
    finalized_tip: SlotNumber,
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
            queue: PendingBlocks::new(),
            finalized_tip: SlotNumber(0),
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut next_slot = self.start_slot;
        let cancel = self.cancel.clone();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                result = self.ingest_step(next_slot) => {
                    next_slot = result?;
                }
            }
        }
    }

    /// Run one ingest step and return the next slot to fetch.
    ///
    /// A serial fetch cannot outpace slot production on high-latency RPC
    /// links, which starves the promotion witness forever: the queue tail
    /// never passes the finalized tip. Far behind the confirmed tip, catch up
    /// by streaming the gap through a fetch pipeline and applying blocks in
    /// slot order; near the tip, fall back to the serial path.
    async fn ingest_step(&mut self, next_slot: SlotNumber) -> Result<SlotNumber, NodeError> {
        let tip = match self.context.rpc.get_slot().await {
            Ok(tip) => tip,
            Err(error) => {
                error!(
                    error = %error,
                    "block_ingestor: get_slot failed: {}",
                    error
                );
                return Err(NodeError::from(error));
            }
        };

        if tip.saturating_sub(next_slot.0) < FETCH_PIPELINE_DEPTH as u64 {
            return self.fetch_parse_and_dispatch(next_slot, tip).await;
        }

        let end_slot = SlotNumber(tip);
        let mut blocks = fetch_blocks_ordered(
            self.context.clone(),
            self.cancel.clone(),
            next_slot.0..=end_slot.0,
        );

        // Promote as the stream advances so consumers make progress across a
        // long gap instead of waiting for the full catch-up to drain.
        let mut since_promote = 0usize;
        while let Some((_, fetched)) = blocks.next().await {
            if let Some(block) = fetched? {
                self.enqueue(block);
            }
            since_promote += 1;
            if since_promote >= FETCH_PIPELINE_DEPTH {
                since_promote = 0;
                self.refresh_finalized_tip().await?;
                self.promote().await?;
            }
        }
        drop(blocks);

        self.refresh_finalized_tip().await?;
        self.promote().await?;
        Ok(end_slot.next())
    }

    /// Serial near-tip path: fetch one block at or below the confirmed tip,
    /// promote, and return the next slot to fetch.
    async fn fetch_parse_and_dispatch(
        &mut self,
        slot: SlotNumber,
        tip: u64,
    ) -> Result<SlotNumber, NodeError> {
        self.refresh_finalized_tip().await?;

        // Ingest readiness is measured against finalized_tip, because
        // promoted/durable consumers intentionally lag confirmed.
        let next = if slot.0 > tip {
            sleep(Duration::from_millis(TIP_POLL_MS)).await;
            slot
        } else {
            let fetched =
                fetch_and_parse_block(self.context.clone(), self.cancel.clone(), slot).await?;
            if let Some(block) = fetched {
                self.enqueue(block);
            }
            slot.next()
        };

        self.promote().await?;

        Ok(next)
    }

    async fn refresh_finalized_tip(&mut self) -> Result<(), NodeError> {
        let tip = match self.context.rpc.get_finalized_slot().await {
            Ok(tip) => tip,
            Err(error) => {
                error!(
                    error = %error,
                    "block_ingestor: get_finalized_slot failed: {}",
                    error
                );
                return Err(NodeError::from(error));
            }
        };
        self.finalized_tip = SlotNumber(tip);
        self.context.ingest.progress().record_tip(tip);
        Ok(())
    }

    /// Append `block` to the pending queue, applying its track events to
    /// pending in-memory state. On a confirmed reorg the rolled-back pending
    /// entries are removed; on a chain break beyond queue depth the queue is
    /// cleared and the new block becomes the start of a new chain.
    fn enqueue(&mut self, block: Arc<ParsedBlock>) {
        let slot = block.slot;
        let outcome = self.queue.append(Arc::clone(&block));
        match outcome {
            AppendOutcome::Appended => {
                self.context.pending.apply_block(&block);
            }
            AppendOutcome::Forked { dropped } => {
                for entry in &dropped {
                    self.context.pending.drop_slot(entry.slot);
                }
                warn!(
                    slot = slot.0,
                    dropped = dropped.len(),
                    "block_ingestor: confirmed reorg, rolled back forked entries"
                );
                self.context.pending.apply_block(&block);
            }
            AppendOutcome::ChainBroken => {
                let stale = self.queue.drain();
                for entry in &stale {
                    self.context.pending.drop_slot(entry.slot);
                }
                warn!(
                    slot = slot.0,
                    cleared = stale.len(),
                    "block_ingestor: chain break beyond queue depth, queue cleared"
                );
                // Queue is empty now; the next append skips the chain check.
                let _ = self.queue.append(Arc::clone(&block));
                self.context.pending.apply_block(&block);
            }
        }
    }

    /// Promote every queue head whose slot is at or below the finalized tip,
    /// provided the queue has also seen a later confirmed block. Each promoted
    /// block is fanned out to the existing consumers; pending track entries are
    /// dropped later by StoreManager after durable state has caught up.
    async fn promote(&mut self) -> Result<(), NodeError> {
        let progress = self.context.ingest.progress();

        while self.queue.front_promotable(self.finalized_tip) {
            let block = self
                .queue
                .pop_front()
                .expect("queue non-empty per front_promotable");
            self.fanout(&block).await?;
            self.context.metrics.inc_blocks_processed();
            progress.record_dispatched(block.slot.0);
            info!(slot = block.slot.0, "dispatched parsed block");
        }

        Ok(())
    }

    async fn fanout(&self, block: &Arc<ParsedBlock>) -> Result<(), NodeError> {
        let slot = block.slot;

        if let Err(error) = send_block(
            &self.senders.state,
            ChannelName::StateManager,
            Arc::clone(block),
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
            &self.senders.assignment,
            ChannelName::AssignmentManager,
            Arc::clone(block),
        )
        .await
        {
            error!(
                slot = slot.0,
                error = %error,
                "block_ingestor: send to AssignmentManager failed: {}",
                error
            );
            return Err(error);
        }

        if let Err(error) = send_block(
            &self.senders.replay,
            ChannelName::ReplayManager,
            Arc::clone(block),
        )
        .await
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
            Arc::clone(block),
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

        Ok(())
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
    use crate::chain::{submit_join_committee, submit_set_network_tls};
    use crate::core::channels::{downstream_channels, store_channel};
    use crate::features::replay::manager::ReplayManager;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

    #[tokio::test]
    async fn forwards_batch_after_later_confirmed_block() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let confirmed_tip = ctx.rpc.get_slot().await.expect("get confirmed tip");

        // Block 1: the JoinCommittee we want to verify gets fanned out.
        submit_join_committee(&ctx)
            .await
            .expect("submit join committee");
        harness
            .rpc()
            .warp_to_slot(confirmed_tip + 1)
            .expect("confirm join block");
        let join_slot = produced_slot(harness.rpc(), &[confirmed_tip, confirmed_tip + 1])
            .await
            .expect("discover join slot");

        // Block 2: a subsequent transaction at a later confirmed slot.
        // SetNetworkTls is cheap, idempotent, and may fail at the program
        // level; either way it produces a recorded block at the next slot.
        let _ = submit_set_network_tls(
            &ctx.rpc,
            ctx.signer(),
            ctx.node_address(),
            ctx.tls_pubkey(),
        )
        .await;
        harness
            .rpc()
            .warp_to_slot(join_slot.0 + 2)
            .expect("confirm later block");
        let later_slot = produced_slot(harness.rpc(), &[join_slot.0 + 1, join_slot.0 + 2])
            .await
            .expect("discover later confirmed slot");

        // Pin finalized at the join slot. The join block is at-or-below
        // finalized; the later confirmed block is strictly past it, so the
        // join block becomes promotable.
        harness
            .rpc()
            .set_finalized_tip(join_slot.0)
            .expect("set finalized tip");

        let (senders, receivers) = downstream_channels();
        let (store_tx, mut store_rx) = store_channel();
        let replay = ReplayManager::new(
            ctx.clone(),
            receivers.replay,
            store_tx,
            CancellationToken::new(),
        );
        let replay_task = tokio::spawn(replay.run());

        let mut ingestor = BlockIngestor::new(
            ctx.clone(),
            join_slot,
            senders,
            CancellationToken::new(),
        );

        // First fetch: queues the join block but cannot promote yet because
        // the ingestor has not seen a later confirmed block.
        let tip = ctx.rpc.get_slot().await.expect("get tip");
        ingestor
            .fetch_parse_and_dispatch(join_slot, tip)
            .await
            .expect("dispatch join slot");
        assert!(
            timeout(Duration::from_millis(100), store_rx.recv())
                .await
                .is_err(),
            "join block should not promote before a later confirmed block is queued"
        );

        // Second fetch: queues the later block, which lets the join block promote.
        ingestor
            .fetch_parse_and_dispatch(later_slot, tip)
            .await
            .expect("dispatch later confirmed slot");

        let batch = timeout(Duration::from_secs(1), store_rx.recv())
            .await
            .expect("receive replay batch in time")
            .expect("replay batch");

        replay_task.abort();
        let _ = replay_task.await;

        assert_eq!(batch.slot, join_slot);
        assert!(matches!(
            batch.records.as_slice(),
            [record] if matches!(&record.event, ReplayableEvent::JoinCommittee { .. })
        ));

        let entries = ctx.store.get_epoch_events(EPOCH).expect("get epoch events");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].slot, join_slot);
        assert!(matches!(
            entries[0].records.as_slice(),
            [record] if matches!(&record.event, ReplayableEvent::JoinCommittee { .. })
        ));
    }

    async fn produced_slot(rpc: &rpc_litesvm::LiteSvmRpc, candidates: &[u64]) -> Option<SlotNumber> {
        for &slot in candidates {
            if rpc.get_block(slot).await.is_ok() {
                return Some(SlotNumber(slot));
            }
        }
        None
    }
}
