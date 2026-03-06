//! Block ingestor — sequential Solana block fetching and parsing.
//!
//! The `BlockIngestor` polls the Solana RPC for new blocks, parses them via
//! `tape_blocks`, and sends `ParsedInstruction` batches to the FSM over a
//! bounded channel. It resumes from the last processed slot stored in
//! `MetaOps::get_sync_cursor()`.

use std::sync::Arc;
use std::time::Duration;

use rpc::Rpc;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::SlotNumber;
use tape_store::ops::MetaOps;
use tape_store::types::NodeStatus;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use tape_retry::{Backoff, RetryConfig};
use crate::core::NodeContext;

const BOOTSTRAP_POLL_SECS: u64 = 2;
const TIP_POLL_MS: u64 = 400;
const BACKOFF_MIN_MS: u64 = 100;
const BACKOFF_MAX_SECS: u64 = 30;

/// A batch of parsed instructions from a single block.
pub struct IngestedBlock {
    /// The slot this block was fetched from.
    pub slot: SlotNumber,
    /// Parsed and merged instructions from the block.
    pub instructions: Vec<ParsedInstruction>,
}

/// Fetches Solana blocks sequentially and emits parsed instruction batches.
pub struct BlockIngestor;

impl BlockIngestor {
    /// Run the ingestor loop, fetching blocks and sending them to the FSM.
    ///
    /// Resumes from the sync cursor stored in the database. Polls for new blocks
    /// with a 400ms interval when caught up to the chain tip. Uses exponential
    /// backoff for RPC errors.
    pub async fn run<S: Store, R: Rpc>(
        context: Arc<NodeContext<S, R>>,
        sender: mpsc::Sender<IngestedBlock>,
        cancel: CancellationToken,
    ) -> Result<(), anyhow::Error> {

        let mut next_slot = match wait_bootstrap(&context, &cancel).await? {
            Some(slot) => slot,
            None => return Ok(()),
        };

        tracing::trace!(next_slot = next_slot.0, "ingestor run started");

        let mut backoff = Backoff::new(RetryConfig {
            base_delay: Duration::from_millis(BACKOFF_MIN_MS),
            max_delay: Duration::from_secs(BACKOFF_MAX_SECS),
            max_retries: None,
        });

        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }

            match ingest_slot(&context, &sender, &cancel, next_slot, &mut backoff).await? {
                IngestStep::Continue(slot) => next_slot = slot,
                IngestStep::Wait => continue,
                IngestStep::Stop => return Ok(()),
            }
        }
    }
}

enum IngestStep {
    Continue(SlotNumber),
    Wait,
    Stop,
}

async fn wait_bootstrap<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    cancel: &CancellationToken,
) -> Result<Option<SlotNumber>, anyhow::Error> {

    loop {

        let cursor = context
            .store
            .get_sync_cursor()?;

        let cs = context.chain_state.load();
        let epoch = if !cs.epoch.is_zero() { Some(cs.epoch) } else { None };
        let status = cs.node_status.clone();

        let _cursor_slot = cursor.map(|slot| slot.0);

        if let Some(slot) = cursor {
            return Ok(Some(SlotNumber(slot.0 + 1)));
        }

        let needs_bootstrap = 
            matches!(status, NodeStatus::Active) && 
            matches!(epoch, Some(e) if e.0 >= 2);

        if !needs_bootstrap {
            tracing::trace!(
                cursor = cursor.map(|slot| slot.0),
                status = ?status,
                epoch = epoch.map(|e| e.0),
                "ingestor bootstrap completed"
            );

            // TODO: why are we returning slot number 0? feels broken...
            return Ok(Some(SlotNumber(0)));
        }

        tracing::trace!(
            status = ?status,
            epoch = epoch.map(|e| e.0),
            "waiting for snapshot bootstrap to complete"
        );

        if !sleep_or_active(Duration::from_secs(BOOTSTRAP_POLL_SECS), cancel).await {
            return Ok(None);
        }
    }
}

async fn ingest_slot<S: Store, R: Rpc>(
    context: &Arc<NodeContext<S, R>>,
    sender: &mpsc::Sender<IngestedBlock>,
    cancel: &CancellationToken,
    next_slot: SlotNumber,
    backoff: &mut Backoff,
) -> Result<IngestStep, anyhow::Error> {

    let tip = match context.rpc.get_slot().await {
        Ok(tip) => SlotNumber(tip),
        Err(e) => {
            tracing::warn!("Failed to get chain tip: {e}");
            if let Some(delay) = backoff.next_delay() {
                if !sleep_or_active(delay, cancel).await {
                    return Ok(IngestStep::Stop);
                }
            }
            return Ok(IngestStep::Wait);
        }
    };

    tracing::trace!(
        next_slot = next_slot.0,
        tip_slot = tip.0,
        "ingestor checking next slot"
    );

    if next_slot.0 > tip.0 {
        tracing::trace!(
            next_slot = next_slot.0,
            tip_slot = tip.0,
            "ingestor waiting for next produced slot"
        );

        if !sleep_or_active(Duration::from_millis(TIP_POLL_MS), cancel).await {
            return Ok(IngestStep::Stop);
        }

        return Ok(IngestStep::Wait);
    }

    let block = match context.rpc.get_block(next_slot.0).await {
        Ok(block) => {
            backoff.reset();
            block
        }
        Err(e) if e.is_skipped_slot() => {
            tracing::trace!(slot = next_slot.0, "block skipped");
            return Ok(IngestStep::Continue(SlotNumber(next_slot.0 + 1)));
        }
        Err(e) => {
            tracing::warn!(slot = next_slot.0, "Failed to fetch block: {e}");

            if let Some(delay) = backoff.next_delay() {
                if !sleep_or_active(delay, cancel).await {
                    return Ok(IngestStep::Stop);
                }
            }

            return Ok(IngestStep::Wait);
        }
    };

    let block_height = block.
        block_height
        .unwrap_or_default();

    tracing::trace!(
        slot = next_slot.0,
        block_height,
        blockhash = %block.blockhash,
        "fetched block from rpc"
    );

    if let Some(signatures) = &block.signatures {
        tracing::trace!(
            slot = next_slot.0,
            signatures = signatures.len(),
            "block signatures observed"
        );
        for signature in signatures {
            tracing::trace!(slot = next_slot.0, tx_signature = %signature, "ingestor saw transaction signature");
        }
    }

    let instructions = match tape_blocks::parse_and_merge(&block) {
        Ok(instructions) => instructions,
        Err(e) => {
            tracing::warn!(slot = next_slot.0, "Failed to parse block: {e}");
            return Ok(IngestStep::Continue(SlotNumber(next_slot.0 + 1)));
        }
    };

    tracing::trace!(
        slot = next_slot.0,
        merged_instructions = instructions.len(),
        "merged ingested block instructions"
    );

    let ingested = IngestedBlock {
        slot: next_slot,
        instructions,
    };

    tracing::trace!(slot = ingested.slot.0, "sending ingested block to fsm");

    if sender.send(ingested).await.is_err() {
        tracing::trace!(slot = next_slot.0, "fsm receiver closed while ingesting block");

        return Ok(IngestStep::Stop);
    }

    Ok(IngestStep::Continue(SlotNumber(next_slot.0 + 1)))
}

async fn sleep_or_active(delay: Duration, cancel: &CancellationToken) -> bool {
    tokio::select! {
        _ = sleep(delay) => true,
        _ = cancel.cancelled() => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use tape_core::types::EpochNumber;
    use tape_core::system::EpochPhase;
    use tokio::time::sleep;

    use crate::state::ChainState;
    use crate::core::test_utils::test_context;

    #[tokio::test]
    async fn waits_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5 with no cursor → needs bootstrap
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(5),
            phase: EpochPhase::Active,
            nonce: tape_crypto::Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            node_status: NodeStatus::Active,
            spools: HashSet::new(),
        });

        let (tx, _rx) = mpsc::channel(4);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Let the ingestor enter the wait loop
        sleep(Duration::from_millis(100)).await;

        // Simulate bootstrap completing
        ctx.store.set_sync_cursor(SlotNumber(1000)).unwrap();

        // Wait for the ingestor to notice the cursor (poll interval is 2s)
        sleep(Duration::from_secs(3)).await;

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn starts_no_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Standby with no cursor → no bootstrap needed, start from 0
        ctx.chain_state.store(ChainState {
            epoch: EpochNumber(0),
            phase: EpochPhase::Unknown,
            nonce: tape_crypto::Hash::default(),
            committee: Vec::new(),
            committee_prev: Vec::new(),
            node_status: NodeStatus::Standby,
            spools: HashSet::new(),
        });

        let (tx, _rx) = mpsc::channel(4);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Give ingestor time to start polling
        sleep(Duration::from_millis(500)).await;

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn resumes_cursor() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Cursor at slot 100 → should start from 101
        ctx.store.set_sync_cursor(SlotNumber(100)).unwrap();

        let (tx, _rx) = mpsc::channel(4);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Give ingestor time to start polling
        sleep(Duration::from_millis(500)).await;

        cancel.cancel();
        handle.await.unwrap();
    }
}
