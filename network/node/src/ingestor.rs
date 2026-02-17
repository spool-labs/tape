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
use tokio_util::sync::CancellationToken;

use crate::core::{Backoff, BackoffConfig, NodeContext};

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
        // Wait for bootstrap to complete before ingesting.
        // If the node is Active at epoch >= 2 with no sync cursor, snapshot
        // bootstrap is needed — poll until the cursor appears.
        let mut next_slot;
        loop {
            let cursor = context.store.get_sync_cursor()?;
            let status = context
                .store
                .get_node_status()
                .ok()
                .flatten()
                .unwrap_or(NodeStatus::Standby);
            let epoch = context.store.get_current_epoch().ok().flatten();

            if let Some(slot) = cursor {
                next_slot = SlotNumber(slot.0 + 1);
                break;
            }

            let needs_bootstrap =
                matches!(status, NodeStatus::Active) && matches!(epoch, Some(e) if e.0 >= 2);

            if !needs_bootstrap {
                next_slot = SlotNumber(0);
                break;
            }

            tracing::info!("waiting for snapshot bootstrap to complete");
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                _ = cancel.cancelled() => return Ok(()),
            }
        }

        let mut backoff = Backoff::new(BackoffConfig {
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_retries: None,
        });

        loop {
            if cancel.is_cancelled() {
                return Ok(());
            }

            // Poll chain tip
            let tip = match context.rpc.get_slot().await {
                Ok(tip) => SlotNumber(tip),
                Err(e) => {
                    tracing::warn!("Failed to get chain tip: {e}");
                    if let Some(delay) = backoff.next_delay() {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancel.cancelled() => return Ok(()),
                        }
                    }
                    continue;
                }
            };

            // Wait for new blocks if caught up
            if next_slot.0 > tip.0 {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(400)) => {}
                    _ = cancel.cancelled() => return Ok(()),
                }
                continue;
            }

            // Fetch block
            let block = match context.rpc.get_block(next_slot.0).await {
                Ok(block) => {
                    backoff.reset();
                    block
                }
                Err(e) if e.to_string().contains("skipped") => {
                    // Skipped slot
                    next_slot = SlotNumber(next_slot.0 + 1);
                    continue;
                }
                Err(e) => {
                    tracing::warn!(slot = next_slot.0, "Failed to fetch block: {e}");
                    if let Some(delay) = backoff.next_delay() {
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancel.cancelled() => return Ok(()),
                        }
                    }
                    continue;
                }
            };

            // Parse and merge
            let parsed = tape_blocks::parse(&block)?;
            let instructions = tape_blocks::merge(parsed.raw_instructions, parsed.events)?;

            let ingested = IngestedBlock {
                slot: next_slot,
                instructions,
            };

            // Send to FSM — bounded channel provides backpressure.
            // If the receiver is dropped, exit cleanly.
            if sender.send(ingested).await.is_err() {
                return Ok(());
            }

            next_slot = SlotNumber(next_slot.0 + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tape_core::types::EpochNumber;

    use crate::test_util::test_context;

    #[tokio::test]
    async fn waits_for_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5 with no cursor → needs bootstrap
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(5)).unwrap();

        let (tx, _rx) = mpsc::channel(4);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Let the ingestor enter the wait loop
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate bootstrap completing
        ctx.store.set_sync_cursor(SlotNumber(1000)).unwrap();

        // Wait for the ingestor to notice the cursor (poll interval is 2s)
        tokio::time::sleep(Duration::from_secs(3)).await;

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn starts_immediately_no_bootstrap() {
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
        tokio::time::sleep(Duration::from_millis(500)).await;

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn resumes_from_cursor() {
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
        tokio::time::sleep(Duration::from_millis(500)).await;

        cancel.cancel();
        handle.await.unwrap();
    }
}
