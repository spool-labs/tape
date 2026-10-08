use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::StreamExt;
use tape_core::challenge::schedule::SLOT_MS;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use tape_rpc::skip::slots_to_fetch;
use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::core::channels::{DownstreamSenders, send_block, ChainEvent};
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::fetch::{
    FETCH_PIPELINE_DEPTH, fetch_and_parse_block, fetch_blocks_ordered,
};
use crate::features::bootstrap::LiveStart;
use crate::features::block::pending_blocks::{AppendOutcome, PendingBlocks};

/// Bounds on the poll wait, so a measured slot time cannot spin or stall it
const TIP_POLL_MIN_MS: u64 = 2;
const TIP_POLL_MAX_MS: u64 = 400;

/// Minimum interval between INFO-level dispatch summaries. Per-block
/// dispatch logging is debug-level; at catch-up rates it would flood.
const DISPATCH_LOG_INTERVAL: Duration = Duration::from_secs(10);

/// Slots listed per catch-up round trip; the loop re-enters for the rest.
const CATCH_UP_WINDOW_SLOTS: u64 = 4096;

/// An RPC failure that ends ingest, logged where it happened.
fn rpc_failed(call: &str, error: RpcError) -> NodeError {
    error!(error = %error, "block_ingestor: {call} failed: {}", error);
    NodeError::from(error)
}

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
    dispatch_log_at: Instant,
    dispatch_log_count: u64,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc>
    BlockIngestor<Db, Cluster, Blockchain> {

    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        start: LiveStart,
        senders: DownstreamSenders,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            start_slot: start.slot,
            senders,
            cancel,
            queue: PendingBlocks::new(start.parent),
            dispatch_log_at: Instant::now(),
            dispatch_log_count: 0,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let mut next_slot = self.start_slot;
        let cancel = self.cancel.clone();
        self.context
            .ingest
            .progress()
            .start_live(next_slot.0.saturating_sub(1));

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
    /// Far behind the confirmed tip, catch up by streaming the gap through
    /// a fetch pipeline and applying blocks in slot order; near the tip,
    /// fall back to the serial path.
    async fn ingest_step(&mut self, next_slot: SlotNumber) -> Result<SlotNumber, NodeError> {
        let next = self.ingest_once(next_slot).await?;
        self.record_edge(SlotNumber(next.0.saturating_sub(1)));
        Ok(next)
    }

    /// Mark ingest current as far as `through`, the slot it has read up to, once the queue drains
    fn record_edge(&self, through: SlotNumber) {
        if !self.queue.is_empty() {
            return;
        }
        let progress = self.context.ingest.progress();
        let edge = through.0.min(progress.last_known_tip());
        if edge > progress.last_dispatched_slot() {
            progress.record_dispatched(edge);
        }
    }

    async fn ingest_once(&mut self, next_slot: SlotNumber) -> Result<SlotNumber, NodeError> {
        let tip = self.refresh_tip().await?;

        if tip.saturating_sub(next_slot.0) < FETCH_PIPELINE_DEPTH as u64 {
            return self.fetch_parse_and_dispatch(next_slot, tip).await;
        }

        // Only produced slots: a skipped one answers "not available" until it roots
        let window_end = tip.min(next_slot.0 + CATCH_UP_WINDOW_SLOTS);
        let window = self
            .context
            .rpc
            .get_block_window(next_slot.0, window_end)
            .await
            .map_err(|error| rpc_failed("get_blocks", error))?;
        let Some(&last) = window.produced.last() else {
            // A member behind the window lists nothing for a range it lacks, so
            // the tip it reports for the same pass is as far as this answer reaches.
            let answered_tip = window.tip.unwrap_or(next_slot.0);
            // A window the chain lists nothing in holds nothing to ingest, so resume at its end
            let resume = SlotNumber(window_end.min(answered_tip).max(next_slot.0));
            // An idle chain is still a chain this node keeps up with, so the
            // edge moves before the wait.
            self.record_edge(SlotNumber(resume.0.saturating_sub(1)));
            sleep(self.tip_poll_delay()).await;
            return Ok(resume);
        };
        let end_slot = SlotNumber(last);

        let mut blocks = fetch_blocks_ordered(
            self.context.clone(),
            self.cancel.clone(),
            slots_to_fetch(next_slot.0, &window.produced),
        );

        // Promote as the stream advances so consumers make progress across a
        // long gap instead of waiting for the full catch-up to drain.
        let mut since_promote = 0usize;
        while let Some((_, fetched)) = blocks.next().await {
            if let Some(block) = fetched? {
                self.enqueue(block).await?;
            }
            since_promote += 1;
            if since_promote >= FETCH_PIPELINE_DEPTH {
                since_promote = 0;
                self.refresh_tip().await?;
                self.promote().await?;
            }
        }
        drop(blocks);

        self.promote().await?;
        Ok(end_slot.next())
    }

    /// Sample the tip and record it, so lag and readiness stay current through a long catch-up
    async fn refresh_tip(&self) -> Result<u64, NodeError> {
        let tip = self
            .context
            .rpc
            .get_slot()
            .await
            .map_err(|error| rpc_failed("get_slot", error))?;
        self.context.ingest.progress().record_tip(tip);
        Ok(tip)
    }

    /// Wait before re-asking for a tip, from the measured slot time
    fn tip_poll_delay(&self) -> Duration {
        let slot_ms = self
            .context
            .ingest
            .progress()
            .slot_ms()
            .unwrap_or(SLOT_MS);
        let polls = self.context.config.solana.tip_polls_per_slot.max(1);
        let wait = (slot_ms / polls).clamp(TIP_POLL_MIN_MS, TIP_POLL_MAX_MS);
        Duration::from_millis(wait)
    }

    /// Serial near-tip path: fetch one block at or below the confirmed tip,
    /// promote, and return the next slot to fetch.
    async fn fetch_parse_and_dispatch(
        &mut self,
        slot: SlotNumber,
        tip: u64,
    ) -> Result<SlotNumber, NodeError> {
        let next = if slot.0 > tip {
            sleep(self.tip_poll_delay()).await;
            slot
        } else {
            let fetched =
                fetch_and_parse_block(self.context.clone(), self.cancel.clone(), slot).await?;
            if let Some(block) = fetched {
                self.enqueue(block).await?;
            }
            slot.next()
        };

        self.promote().await?;

        Ok(next)
    }

    /// Append `block` to the pending queue, applying its track events to
    /// pending in-memory state. On a confirmed reorg the rolled-back pending
    /// entries are removed; a break below the promoted head stops the node.
    async fn enqueue(&mut self, block: Arc<ParsedBlock>) -> Result<(), NodeError> {
        let slot = block.slot;
        let outcome = self.queue.append(Arc::clone(&block));
        match outcome {
            AppendOutcome::Appended => {
                self.context.pending.apply_block(&block, self.context.params().profile);
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
                // Ahead of the replacement, so a round seeded by a losing
                // candidate is voided before its successor opens.
                self.rolled(dropped.iter().map(|entry| entry.blockhash).collect())
                    .await?;
                self.context.pending.apply_block(&block, self.context.params().profile);
            }
            AppendOutcome::Diverged => {
                error!(
                    slot = slot.0,
                    "block_ingestor: confirmed chain diverged below the promoted head"
                );
                return Err(NodeError::ChainDiverged { slot });
            }
            AppendOutcome::ChainBroken => {
                let stale = self.queue.drain();
                for entry in &stale {
                    self.context.pending.drop_slot(entry.slot);
                }
                self.rolled(stale.iter().map(|entry| entry.blockhash).collect())
                    .await?;
                warn!(
                    slot = slot.0,
                    cleared = stale.len(),
                    "block_ingestor: chain break before any promotion, queue cleared"
                );
                // Queue is empty now; the next append skips the chain check.
                let _ = self.queue.append(Arc::clone(&block));
                self.context.pending.apply_block(&block, self.context.params().profile);
            }
        }
        self.context
            .ingest
            .progress()
            .record_queue_len(self.queue.len() as u64);

        self.send_chain(ChainEvent::Produced(block)).await
    }

    /// Tell the challenge lane which candidates lost.
    async fn rolled(&self, hashes: Vec<Hash>) -> Result<(), NodeError> {
        if hashes.is_empty() {
            return Ok(());
        }
        self.send_chain(ChainEvent::Rolled(hashes)).await
    }

    /// One ordered lane, so a rollback always precedes what replaced it.
    async fn send_chain(&self, event: ChainEvent) -> Result<(), NodeError> {
        self.senders
            .challenge
            .send(event)
            .await
            .map_err(|_| NodeError::ChannelClosed {
                channel: ChannelName::ChallengeManager,
            })
    }

    /// Promote every queued block: each was fetched at confirmed commitment,
    /// so it is confirmed on arrival. Each promoted block is fanned out to the
    /// existing consumers; pending track entries are dropped later by
    /// StoreManager after durable state has caught up.
    async fn promote(&mut self) -> Result<(), NodeError> {
        let progress = self.context.ingest.progress();

        while let Some(block) = self.queue.pop_front() {
            self.fanout(&block).await?;
            self.context.metrics.inc_blocks_processed();
            progress.record_dispatched(block.slot.0);
            if let Some(time) = block.block_time {
                progress.record_chain_time(block.slot, time);
            }
            self.dispatch_log_count += 1;
            debug!(slot = block.slot.0, "dispatched parsed block");
        }

        progress.record_queue_len(self.queue.len() as u64);

        if self.dispatch_log_count > 0 && self.dispatch_log_at.elapsed() >= DISPATCH_LOG_INTERVAL {
            info!(
                blocks = self.dispatch_log_count,
                through_slot = progress.last_dispatched_slot(),
                queued = self.queue.len(),
                lag = progress
                    .last_known_tip()
                    .saturating_sub(progress.last_dispatched_slot()),
                "dispatched parsed blocks"
            );
            self.dispatch_log_count = 0;
            self.dispatch_log_at = Instant::now();
        }

        Ok(())
    }

    async fn fanout(&self, block: &Arc<ParsedBlock>) -> Result<(), NodeError> {
        let slot = block.slot;

        // The challenge lane already saw this block when it was produced. What
        // it needs now is that the block promoted, which is what makes a round's
        // evidence standing.
        self.send_chain(ChainEvent::Confirmed(block.blockhash)).await?;

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
            &self.senders.eviction,
            ChannelName::EvictionManager,
            Arc::clone(block),
        )
        .await
        {
            error!(
                slot = slot.0,
                error = %error,
                "block_ingestor: send to EvictionManager failed: {}",
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

    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::system::EpochPhase;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store::ops::EventLogOps;
    use tokio::time::timeout;
    use tokio_util::sync::CancellationToken;

    use super::{BlockIngestor, CATCH_UP_WINDOW_SLOTS};
    use crate::chain::{submit_join_committee, submit_set_network_tls};
    use crate::core::channels::{downstream_channels, store_channel};
    use crate::features::block::fetch::FETCH_PIPELINE_DEPTH;
    use crate::features::bootstrap::LiveStart;
    use crate::features::replay::manager::ReplayManager;
    use crate::harness::{NodeHarness, TestContext};

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

    async fn harness() -> NodeHarness {
        NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .build()
            .await
            .expect("build harness")
    }

    /// Record a JoinCommittee block for `ctx` and return its slot
    async fn join_block(harness: &NodeHarness, ctx: &TestContext) -> SlotNumber {
        let confirmed_tip = ctx.rpc.get_slot().await.expect("get confirmed tip");
        submit_join_committee(ctx)
            .await
            .expect("submit join committee");
        harness
            .rpc()
            .warp_to_slot(confirmed_tip + 1)
            .expect("confirm join block");
        harness
            .produced_slot(&[confirmed_tip, confirmed_tip + 1])
            .await
            .expect("discover join slot")
    }

    /// Record one more block after `slot` and return its slot. SetNetworkTls
    /// is cheap and may fail at the program level; either way it records a block.
    async fn later_block(harness: &NodeHarness, ctx: &TestContext, slot: SlotNumber) -> SlotNumber {
        let _ = submit_set_network_tls(
            &ctx.rpc,
            ctx.signer(),
            ctx.node_address(),
            ctx.tls_pubkey(),
        )
        .await;
        harness
            .rpc()
            .warp_to_slot(slot.0 + 2)
            .expect("confirm later block");
        harness
            .produced_slot(&[slot.0 + 1, slot.0 + 2])
            .await
            .expect("discover later slot")
    }

    // a window the chain lists no block in moves ingest past it
    #[tokio::test]
    async fn empty_window_moves_ingest_on() {
        let harness = harness().await;
        let ctx = harness.ctx_for(NODE);
        let start = SlotNumber(ctx.rpc.get_slot().await.expect("get tip") + 1);

        // Far enough past the window that every slot in it is one the chain produced nothing at.
        harness
            .rpc()
            .warp_to_slot(start.0 + CATCH_UP_WINDOW_SLOTS * 2)
            .expect("warp past the window");

        let (senders, _receivers) = downstream_channels();
        let mut ingestor = BlockIngestor::new(
            ctx.clone(),
            LiveStart { slot: start, parent: None },
            senders,
            CancellationToken::new(),
        );

        assert_eq!(
            ingestor.ingest_step(start).await.expect("one ingest step"),
            SlotNumber(start.0 + CATCH_UP_WINDOW_SLOTS)
        );
        // The node read the whole window, so its edge sits at the end of it,
        // however few blocks the chain produced in it.
        let progress = ctx.ingest.progress();
        assert_eq!(
            progress.last_dispatched_slot(),
            start.0 + CATCH_UP_WINDOW_SLOTS - 1
        );
    }

    // the field stall: a block at the fetch tip must promote with nothing behind it
    #[tokio::test]
    async fn lone_tip_block() {
        let harness = harness().await;
        let ctx = harness.ctx_for(NODE);
        let join_slot = join_block(&harness, &ctx).await;

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
            LiveStart { slot: join_slot, parent: None },
            senders,
            CancellationToken::new(),
        );

        ingestor
            .ingest_step(join_slot)
            .await
            .expect("dispatch join slot");

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
    }

    // a gap wider than the fetch pipeline streams through the catch-up window and promotes in order
    #[tokio::test]
    async fn catch_up_gap() {
        let harness = harness().await;
        let ctx = harness.ctx_for(NODE);
        let join_slot = join_block(&harness, &ctx).await;
        let later_slot = later_block(&harness, &ctx, join_slot).await;
        harness
            .rpc()
            .warp_to_slot(later_slot.0 + FETCH_PIPELINE_DEPTH as u64)
            .expect("move the tip past the pipeline depth");

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
            LiveStart { slot: join_slot, parent: None },
            senders,
            CancellationToken::new(),
        );

        let next = ingestor
            .ingest_step(join_slot)
            .await
            .expect("catch up through the window");

        let batch = timeout(Duration::from_secs(1), store_rx.recv())
            .await
            .expect("receive replay batch in time")
            .expect("replay batch");

        replay_task.abort();
        let _ = replay_task.await;

        assert!(next > later_slot);
        let progress = ctx.ingest.progress();
        assert_eq!(SlotNumber(progress.last_dispatched_slot()).next(), next);
        assert_eq!(progress.queue_len(), 0);
        assert_eq!(batch.slot, join_slot);
        assert!(matches!(
            batch.records.as_slice(),
            [record] if matches!(&record.event, ReplayableEvent::JoinCommittee { .. })
        ));
    }
}
