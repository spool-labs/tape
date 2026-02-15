//! Block ingestor — sequential Solana block fetching and parsing.
//!
//! The `BlockIngestor` polls the Solana RPC for new blocks, parses them via
//! `tape_blocks`, and sends `ParsedInstruction` batches to the FSM over a
//! bounded channel. It resumes from the last processed slot stored in
//! `MetaOps::get_sync_cursor()`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use solana_transaction_status::UiConfirmedBlock;
use store::Store;
use tape_blocks::ParsedInstruction;
use tape_core::types::SlotNumber;
use tape_store::ops::MetaOps;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::core::{Backoff, BackoffConfig, NodeContext};

/// Abstracts the Solana RPC for block fetching.
///
/// Tests use a `MockBlockSource`; production will wrap `RpcClient`.
#[async_trait]
pub trait BlockSource: Send + Sync {
    /// Get the current chain tip slot.
    async fn get_slot(&self) -> Result<SlotNumber, anyhow::Error>;

    /// Get a block by slot. Returns `None` for skipped slots.
    async fn get_block(&self, slot: SlotNumber) -> Result<Option<UiConfirmedBlock>, anyhow::Error>;
}

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
    pub async fn run<S: Store>(
        context: Arc<NodeContext<S>>,
        source: Arc<dyn BlockSource>,
        sender: mpsc::Sender<IngestedBlock>,
        cancel: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        let mut next_slot = match context.store.get_sync_cursor()? {
            Some(slot) => SlotNumber(slot.0 + 1),
            None => SlotNumber(0),
        };

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
            let tip = match source.get_slot().await {
                Ok(tip) => tip,
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
            let block = match source.get_block(next_slot).await {
                Ok(Some(block)) => {
                    backoff.reset();
                    block
                }
                Ok(None) => {
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
