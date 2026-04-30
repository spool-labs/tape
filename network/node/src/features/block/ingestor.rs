use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use rpc::Rpc;
use store::Store;
use tape_blocks::{ParsedInstruction, parse_and_merge};
use tape_core::types::SlotNumber;
use tape_crypto::Hash;
use tape_protocol::Api;
use tape_retry::{RetryConfig, retry_if};

use crate::core::channels::{DownstreamSenders, send_block};
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::types::ChannelName;
use crate::features::block::pending_blocks::{AppendOutcome, PendingBlocks};

const TIP_POLL_MS: u64 = 400;

#[derive(Debug, Default)]
pub struct ParsedBlock {
    pub slot: SlotNumber,
    pub parent_slot: SlotNumber,
    pub blockhash: Hash,
    pub previous_blockhash: Hash,
    pub instructions: Vec<ParsedInstruction>,
}

fn parse_chain_hash(slot: SlotNumber, label: &str, encoded: &str) -> Result<Hash, NodeError> {
    Hash::from_str(encoded).map_err(|err| {
        error!(
            slot = slot.0,
            label,
            encoded,
            error = %err,
            "block_ingestor: chain hash parse failed"
        );
        NodeError::BlockMalformed {
            slot: slot.0,
            reason: format!("{label}: {err}"),
        }
    })
}

enum IngestStep {
    Continue,
    Wait,
}

enum FetchOutcome {
    Block(Arc<ParsedBlock>),
    PastTip,
    Skipped,
}

pub struct BlockIngestor<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    start_slot: SlotNumber,
    senders: DownstreamSenders,
    cancel: CancellationToken,
    queue: PendingBlocks,
    /// Most recently observed finalized slot. Refreshed on every iteration
    /// and consulted by the witness-rule promotion check.
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
                result = self.fetch_parse_and_dispatch(next_slot) => {
                    match result? {
                        IngestStep::Continue => next_slot.increment(),
                        IngestStep::Wait => {}
                    }
                }
            }
        }
    }

    async fn fetch_parse_and_dispatch(&mut self, slot: SlotNumber) -> Result<IngestStep, NodeError> {
        self.refresh_finalized_tip().await?;

        let outcome = self.fetch_block(slot).await?;
        let step = if matches!(outcome, FetchOutcome::PastTip) {
            IngestStep::Wait
        } else {
            IngestStep::Continue
        };

        if let FetchOutcome::Block(block) = outcome {
            self.enqueue(block);
        }

        self.promote().await?;

        Ok(step)
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

    async fn fetch_block(&self, slot: SlotNumber) -> Result<FetchOutcome, NodeError> {
        let progress = self.context.ingest.progress();
        progress.record_attempt();

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

        // This is the confirmed fetch tip, used only to avoid asking for
        // future blocks. Ingest readiness is measured against finalized_tip,
        // because promoted/durable consumers intentionally lag confirmed.
        if slot.0 > tip {
            sleep(Duration::from_millis(TIP_POLL_MS)).await;
            return Ok(FetchOutcome::PastTip);
        }

        let context = self.context.clone();
        let attempt_progress = progress.clone();

        let block = retry_if(
            RetryConfig::infinite(),
            Some(&self.cancel),
            move || {
                let context = context.clone();
                let attempt_progress = attempt_progress.clone();
                async move {
                    attempt_progress.record_attempt();
                    context.rpc.get_block(slot.0).await
                }
            },
            |error| error.is_retriable() && !error.is_skipped_slot(),
        )
        .await;

        let block = match block {
            Ok(block) => block,
            Err(error) if error.is_skipped_slot() => {
                debug!(slot = slot.0, "slot skipped");
                return Ok(FetchOutcome::Skipped);
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

        let parent_slot = SlotNumber(block.parent_slot);
        let blockhash = parse_chain_hash(slot, "blockhash", &block.blockhash)?;
        let previous_blockhash =
            parse_chain_hash(slot, "previous_blockhash", &block.previous_blockhash)?;

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

        let parsed = Arc::new(ParsedBlock {
            slot,
            parent_slot,
            blockhash,
            previous_blockhash,
            instructions,
        });

        debug!(
            slot = parsed.slot.0,
            extracted = parsed.instructions.len(),
            "parsed block"
        );

        Ok(FetchOutcome::Block(parsed))
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
    /// provided the queue still has a witness block past that tip. Each
    /// promoted block is fanned out to the existing consumers; pending track
    /// entries are dropped later by StoreManager after durable state has
    /// caught up.
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
    use crate::chain::{submit_join_network, submit_set_network_tls};
    use crate::core::channels::{downstream_channels, store_channel};
    use crate::features::replay::manager::ReplayManager;
    use crate::harness::NodeHarness;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 24;

    #[tokio::test]
    async fn forwards_batch_after_witness_seen() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Active)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let confirmed_tip = ctx.rpc.get_slot().await.expect("get confirmed tip");

        // Block 1: the JoinNetwork we want to verify gets fanned out.
        submit_join_network(&ctx)
            .await
            .expect("submit join network");
        harness
            .rpc()
            .warp_to_slot(confirmed_tip + 1)
            .expect("confirm join block");
        let join_slot = produced_slot(harness.rpc(), &[confirmed_tip, confirmed_tip + 1])
            .await
            .expect("discover join slot");

        // Block 2: a subsequent transaction at a later slot that exists only
        // to act as the finality witness for the join block. SetNetworkTls is
        // cheap, idempotent, and may fail at the program level — either way
        // it produces a recorded block at the next slot.
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
            .expect("confirm witness block");
        let witness_slot = produced_slot(harness.rpc(), &[join_slot.0 + 1, join_slot.0 + 2])
            .await
            .expect("discover witness slot");

        // Pin finalized at the join slot. The join block is at-or-below
        // finalized; the witness block is strictly past it, so the join
        // block becomes promotable.
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

        // First fetch: queues the join block but cannot promote yet (no
        // witness past finalized).
        ingestor
            .fetch_parse_and_dispatch(join_slot)
            .await
            .expect("dispatch join slot");
        assert!(
            timeout(Duration::from_millis(100), store_rx.recv())
                .await
                .is_err(),
            "join block should not be promoted without a witness past finalized"
        );

        // Second fetch: queues the witness, which lets the join block promote.
        ingestor
            .fetch_parse_and_dispatch(witness_slot)
            .await
            .expect("dispatch witness slot");

        let batch = timeout(Duration::from_secs(1), store_rx.recv())
            .await
            .expect("receive replay batch in time")
            .expect("replay batch");

        replay_task.abort();
        let _ = replay_task.await;

        assert_eq!(batch.slot, join_slot);
        assert!(matches!(
            batch.events.as_slice(),
            [ReplayableEvent::JoinNetwork { .. }]
        ));

        let entries = ctx.store.get_epoch_events(EPOCH).expect("get epoch events");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].slot, join_slot);
        assert!(matches!(
            entries[0].events.as_slice(),
            [ReplayableEvent::JoinNetwork { .. }]
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
