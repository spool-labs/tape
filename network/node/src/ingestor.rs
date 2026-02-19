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

use crate::core::{Backoff, BackoffConfig, NodeContext};

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
        let mut backoff = Backoff::new(BackoffConfig {
            min_delay: Duration::from_millis(BACKOFF_MIN_MS),
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
        let cursor = context.store.get_sync_cursor()?;
        let status = context
            .store
            .get_node_status()
            .ok()
            .flatten()
            .unwrap_or(NodeStatus::Standby);
        let epoch = context.store.get_chain_epoch().ok().flatten();

        if let Some(slot) = cursor {
            return Ok(Some(SlotNumber(slot.0 + 1)));
        }

        let needs_bootstrap = matches!(status, NodeStatus::Active) && matches!(epoch, Some(e) if e.0 >= 2);
        if !needs_bootstrap {
            return Ok(Some(SlotNumber(0)));
        }

        tracing::info!("waiting for snapshot bootstrap to complete");
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

    if next_slot.0 > tip.0 {
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

    let parsed = match tape_blocks::parse(&block) {
        Ok(parsed) => parsed,
        Err(e) => {
            tracing::warn!(slot = next_slot.0, "Failed to parse block: {e}");
            return Ok(IngestStep::Continue(SlotNumber(next_slot.0 + 1)));
        }
    };
    let instructions = match tape_blocks::merge(parsed.raw_instructions, parsed.events) {
        Ok(instructions) => instructions,
        Err(e) => {
            tracing::warn!(slot = next_slot.0, "Failed to merge block: {e}");
            return Ok(IngestStep::Continue(SlotNumber(next_slot.0 + 1)));
        }
    };

    let ingested = IngestedBlock {
        slot: next_slot,
        instructions,
    };
    if sender.send(ingested).await.is_err() {
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

    use tape_core::types::EpochNumber;
    use tokio::time::sleep;

    use crate::test_util::test_context;

    #[tokio::test]
    async fn waits_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5 with no cursor → needs bootstrap
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_chain_epoch(EpochNumber(5)).unwrap();

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
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

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
