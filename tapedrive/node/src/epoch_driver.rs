//! Epoch driver for monitoring and handling epoch transitions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, error};

use crate::shard_sync::{ShardSyncHandler, SyncError};
use crate::sync_types::{EpochNumber, SpoolIndex};

/// Default polling interval for epoch changes.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Error type for epoch driver operations.
#[derive(Debug, thiserror::Error)]
pub enum EpochError {
    #[error("sync error: {0}")]
    Sync(#[from] SyncError),

    #[error("failed to fetch epoch: {0}")]
    FetchEpoch(String),

    #[error("failed to compute spool changes: {0}")]
    SpoolChanges(String),

    #[error("failed to submit sync done: {0}")]
    SubmitSyncDone(String),
}

/// Epoch driver for monitoring epoch changes and triggering shard sync.
pub struct EpochDriver {
    /// Current epoch number.
    current_epoch: AtomicU64,
    /// Shard sync handler.
    shard_sync: Arc<ShardSyncHandler>,
    /// Polling interval.
    poll_interval: Duration,
    /// Node identity (for determining owned spools).
    node_id: String,
}

impl EpochDriver {
    /// Create a new epoch driver.
    ///
    /// # Arguments
    /// * `initial_epoch` - The starting epoch number
    /// * `node_id` - This node's identifier
    pub fn new(initial_epoch: EpochNumber, node_id: String) -> Self {
        Self {
            current_epoch: AtomicU64::new(initial_epoch),
            shard_sync: Arc::new(ShardSyncHandler::new()),
            poll_interval: DEFAULT_POLL_INTERVAL,
            node_id,
        }
    }

    /// Set the shard sync handler.
    pub fn with_shard_sync(mut self, handler: Arc<ShardSyncHandler>) -> Self {
        self.shard_sync = handler;
        self
    }

    /// Set the polling interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Get the current epoch.
    pub fn current_epoch(&self) -> EpochNumber {
        self.current_epoch.load(Ordering::SeqCst)
    }

    /// Get the node ID.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Run the epoch driver until shutdown.
    ///
    /// # Arguments
    /// * `shutdown` - Cancellation token for graceful shutdown
    /// * `fetch_epoch` - Async function to fetch current on-chain epoch
    /// * `compute_changes` - Function to compute spool changes between epochs
    /// * `on_sync_done` - Callback when sync is complete
    pub async fn run<FetchEpoch, ComputeChanges, OnSyncDone>(
        &self,
        shutdown: CancellationToken,
        fetch_epoch: FetchEpoch,
        compute_changes: ComputeChanges,
        on_sync_done: OnSyncDone,
    ) where
        FetchEpoch: Fn() -> futures::future::BoxFuture<'static, Result<EpochNumber, EpochError>>
            + Send
            + Sync,
        ComputeChanges: Fn(
                EpochNumber,
                EpochNumber,
            )
                -> Result<(Vec<SpoolIndex>, Vec<(SpoolIndex, String)>), EpochError>
            + Send
            + Sync,
        OnSyncDone: Fn(EpochNumber) -> futures::future::BoxFuture<'static, Result<(), EpochError>>
            + Send
            + Sync,
    {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    info!("Epoch driver shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.poll_epoch_change(&fetch_epoch, &compute_changes, &on_sync_done).await {
                        error!("Epoch poll error: {}", e);
                    }
                }
            }
        }
    }

    /// Poll for epoch changes and handle them.
    async fn poll_epoch_change<FetchEpoch, ComputeChanges, OnSyncDone>(
        &self,
        fetch_epoch: &FetchEpoch,
        compute_changes: &ComputeChanges,
        on_sync_done: &OnSyncDone,
    ) -> Result<(), EpochError>
    where
        FetchEpoch: Fn() -> futures::future::BoxFuture<'static, Result<EpochNumber, EpochError>>
            + Send
            + Sync,
        ComputeChanges: Fn(
                EpochNumber,
                EpochNumber,
            )
                -> Result<(Vec<SpoolIndex>, Vec<(SpoolIndex, String)>), EpochError>
            + Send
            + Sync,
        OnSyncDone: Fn(EpochNumber) -> futures::future::BoxFuture<'static, Result<(), EpochError>>
            + Send
            + Sync,
    {
        let on_chain_epoch = fetch_epoch().await?;
        let local_epoch = self.current_epoch.load(Ordering::SeqCst);

        if on_chain_epoch > local_epoch {
            info!(
                from_epoch = local_epoch,
                to_epoch = on_chain_epoch,
                "Epoch change detected"
            );

            self.handle_epoch_change(
                local_epoch,
                on_chain_epoch,
                compute_changes,
                on_sync_done,
            )
            .await?;
        }

        Ok(())
    }

    /// Handle an epoch transition.
    async fn handle_epoch_change<ComputeChanges, OnSyncDone>(
        &self,
        from_epoch: EpochNumber,
        to_epoch: EpochNumber,
        compute_changes: &ComputeChanges,
        on_sync_done: &OnSyncDone,
    ) -> Result<(), EpochError>
    where
        ComputeChanges: Fn(
                EpochNumber,
                EpochNumber,
            )
                -> Result<(Vec<SpoolIndex>, Vec<(SpoolIndex, String)>), EpochError>
            + Send
            + Sync,
        OnSyncDone: Fn(EpochNumber) -> futures::future::BoxFuture<'static, Result<(), EpochError>>
            + Send
            + Sync,
    {
        // 1. Compute spool changes
        let (released_spools, new_spools) = compute_changes(from_epoch, to_epoch)?;

        info!(
            released = released_spools.len(),
            new = new_spools.len(),
            "Computed spool changes"
        );

        // 2. Sync new spools
        if !new_spools.is_empty() {
            info!("Syncing {} new spools", new_spools.len());

            let total_slices = self
                .shard_sync
                .sync_spools(
                    new_spools,
                    from_epoch,
                    Arc::new(|_track, _idx, _data| {
                        // In production, this would store the slice
                        Ok(())
                    }),
                )
                .await?;

            info!(slices = total_slices, "Spool sync complete");
        }

        // 3. Signal sync done
        on_sync_done(to_epoch).await?;

        // 4. Update local epoch
        self.current_epoch.store(to_epoch, Ordering::SeqCst);

        info!(epoch = to_epoch, "Epoch transition complete");

        Ok(())
    }

    /// Manually trigger an epoch update (for testing).
    pub fn set_epoch(&self, epoch: EpochNumber) {
        self.current_epoch.store(epoch, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_creation() {
        let driver = EpochDriver::new(5, "node_1".to_string());
        assert_eq!(driver.current_epoch(), 5);
    }

    #[test]
    fn test_set_epoch() {
        let driver = EpochDriver::new(0, "node_1".to_string());
        driver.set_epoch(10);
        assert_eq!(driver.current_epoch(), 10);
    }

    #[test]
    fn test_custom_poll_interval() {
        let driver = EpochDriver::new(0, "node_1".to_string())
            .with_poll_interval(Duration::from_secs(30));

        assert_eq!(driver.poll_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_node_id() {
        let driver = EpochDriver::new(0, "my_node".to_string());
        assert_eq!(driver.node_id(), "my_node");
    }
}
