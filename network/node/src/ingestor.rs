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
use tape_store::types::NodeStatus;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use tape_core::bls::BlsPrivateKey;
    use tape_core::types::EpochNumber;
    use tape_store::{MemoryStore, TapeStore};

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, NodeContext, TlsConfig};

    fn test_config() -> NodeConfig {
        NodeConfig {
            version: 1,
            name: "test-node".to_string(),
            tls_keypair: PathBuf::from("/dev/null"),
            bls_keypair: PathBuf::from("/dev/null"),
            node_keypair: String::new(),
            bind_address: "127.0.0.1:0".parse().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 0,
            tls: TlsConfig::default(),
            storage_path: "/tmp".to_string(),
            poll_interval_ms: None,
            sync_concurrency: None,
            sync_batch_size: None,
            commission: None,
            recovery: RecoveryConfig::default(),
            node_api: NodeApiConfig::default(),
        }
    }

    fn test_context() -> Arc<NodeContext<MemoryStore>> {
        let config = test_config();
        let keypair = solana_sdk::signature::Keypair::new();
        let bls_keypair = BlsPrivateKey::from_random();
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(config, keypair, bls_keypair, store)
    }

    struct MockBlockSource {
        get_slot_calls: AtomicU64,
    }

    impl MockBlockSource {
        fn new() -> Self {
            Self {
                get_slot_calls: AtomicU64::new(0),
            }
        }

        fn call_count(&self) -> u64 {
            self.get_slot_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl BlockSource for MockBlockSource {
        async fn get_slot(&self) -> Result<SlotNumber, anyhow::Error> {
            self.get_slot_calls.fetch_add(1, Ordering::SeqCst);
            Ok(SlotNumber(0))
        }

        async fn get_block(
            &self,
            _slot: SlotNumber,
        ) -> Result<Option<UiConfirmedBlock>, anyhow::Error> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn waits_for_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Active at epoch 5 with no cursor → needs bootstrap
        ctx.store.set_node_status(NodeStatus::Active).unwrap();
        ctx.store.set_current_epoch(EpochNumber(5)).unwrap();

        let source = Arc::new(MockBlockSource::new());
        let (tx, _rx) = mpsc::channel(4);

        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let src = source.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, src, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Let the ingestor enter the wait loop
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Ingestor should NOT be fetching blocks (no get_slot calls)
        assert_eq!(source.call_count(), 0);

        // Simulate bootstrap completing
        ctx.store.set_sync_cursor(SlotNumber(1000)).unwrap();

        // Wait for the ingestor to notice the cursor (poll interval is 2s)
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Now the ingestor should have started fetching
        assert!(source.call_count() > 0);

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn starts_immediately_no_bootstrap() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Standby with no cursor → no bootstrap needed, start from 0
        ctx.store.set_node_status(NodeStatus::Standby).unwrap();

        let source = Arc::new(MockBlockSource::new());
        let (tx, _rx) = mpsc::channel(4);

        let src = source.clone();
        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, src, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Give ingestor time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should have started fetching immediately
        assert!(source.call_count() > 0);

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn resumes_from_cursor() {
        let ctx = test_context();
        let cancel = CancellationToken::new();

        // Cursor at slot 100 → should start from 101
        ctx.store.set_sync_cursor(SlotNumber(100)).unwrap();

        let source = Arc::new(MockBlockSource::new());
        let (tx, _rx) = mpsc::channel(4);

        let src = source.clone();
        let ingestor_ctx = ctx.clone();
        let ingestor_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            BlockIngestor::run(ingestor_ctx, src, tx, ingestor_cancel)
                .await
                .unwrap();
        });

        // Give ingestor time to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should have started fetching (get_slot called to check tip)
        assert!(source.call_count() > 0);

        cancel.cancel();
        handle.await.unwrap();
    }
}
