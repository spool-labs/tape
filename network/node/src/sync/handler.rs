//! Spool synchronization handler for epoch transitions.

use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::info;

use tape_node_client::{NodeClientBuilder, NodeError};

use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;

use super::types::{SyncSlice, SyncSpoolRequest, SyncSpoolResponse};

/// Default batch size for sync requests.
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Default max concurrent sync operations.
const DEFAULT_MAX_CONCURRENT_SYNCS: usize = 4;

/// Error type for spool sync operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("node communication error: {0}")]
    NodeError(#[from] NodeError),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("no previous owner found for spool {0}")]
    NoPreviousOwner(SpoolIndex),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("signing error: {0}")]
    Signing(String),
}

/// Handler for spool synchronization during epoch transitions.
pub struct SpoolSyncHandler {
    /// Semaphore to limit concurrent sync operations.
    permits: Arc<Semaphore>,
    /// Batch size for sync requests.
    batch_size: usize,
}

impl Default for SpoolSyncHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl SpoolSyncHandler {
    /// Create a new spool sync handler.
    pub fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(DEFAULT_MAX_CONCURRENT_SYNCS)),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the maximum concurrent sync operations.
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.permits = Arc::new(Semaphore::new(max));
        self
    }

    /// Set the batch size for sync requests.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Sync a single spool from a previous owner node.
    ///
    /// # Arguments
    /// * `spool` - The spool index to sync
    /// * `from_epoch` - The epoch we're syncing from
    /// * `prev_owner_address` - Address of the previous owner node
    /// * `on_slice` - Callback for each received slice (includes merkle proofs)
    pub async fn sync_spool<F>(
        &self,
        spool: SpoolIndex,
        from_epoch: EpochNumber,
        prev_owner_address: &str,
        mut on_slice: F,
    ) -> Result<usize, SyncError>
    where
        F: FnMut(SyncSlice) -> Result<(), SyncError>,
    {
        let _permit = self.permits.acquire().await.map_err(|_| {
            SyncError::Storage("semaphore closed".to_string())
        })?;

        let client = NodeClientBuilder::new()
            .build(prev_owner_address)?;

        let mut starting_track = String::new();
        let mut total_slices = 0;

        loop {
            let request = SyncSpoolRequest::new_v1(
                spool,
                starting_track.clone(),
                self.batch_size,
                from_epoch,
            );

            // Serialize request for sending
            let request_bytes = serde_json::to_vec(&request)
                .map_err(|e| SyncError::Serialization(e.to_string()))?;

            // Send sync request
            let response_bytes = client.sync_spool(request_bytes).await?;

            // Deserialize response
            let response: SyncSpoolResponse = serde_json::from_slice(&response_bytes)
                .map_err(|e| SyncError::Serialization(e.to_string()))?;

            if response.is_empty() {
                break;
            }

            let slices = response.slices();
            for slice in slices {
                on_slice(slice.clone())?;
                total_slices += 1;
            }

            // Update pagination cursor
            if let Some(last_slice) = slices.last() {
                starting_track = last_slice.track_id.clone();
            } else {
                break;
            }
        }

        info!(
            spool = spool,
            slices = total_slices,
            "Completed spool sync"
        );

        Ok(total_slices)
    }

    /// Sync multiple spools in parallel.
    ///
    /// # Arguments
    /// * `spools` - List of (spool_index, previous_owner_address) pairs
    /// * `from_epoch` - The epoch we're syncing from
    /// * `on_slice` - Callback for each received slice (must be thread-safe)
    pub async fn sync_spools<F>(
        &self,
        spools: Vec<(SpoolIndex, String)>,
        from_epoch: EpochNumber,
        on_slice: Arc<F>,
    ) -> Result<usize, SyncError>
    where
        F: Fn(SyncSlice) -> Result<(), SyncError> + Send + Sync + 'static,
    {
        use futures::stream::{self, StreamExt};

        let results: Vec<Result<usize, SyncError>> = stream::iter(spools)
            .map(|(spool, address): (SpoolIndex, String)| {
                let handler = self.clone();
                let on_slice = Arc::clone(&on_slice);

                async move {
                    handler
                        .sync_spool(spool, from_epoch, address.as_str(), |slice| {
                            on_slice(slice)
                        })
                        .await
                }
            })
            .buffer_unordered(DEFAULT_MAX_CONCURRENT_SYNCS)
            .collect()
            .await;

        let mut total = 0;
        for result in results {
            total += result?;
        }

        Ok(total)
    }
}

impl Clone for SpoolSyncHandler {
    fn clone(&self) -> Self {
        Self {
            permits: Arc::clone(&self.permits),
            batch_size: self.batch_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_creation() {
        let handler = SpoolSyncHandler::new();
        assert_eq!(handler.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_handler_custom_settings() {
        let handler = SpoolSyncHandler::new()
            .with_max_concurrent(8)
            .with_batch_size(500);

        assert_eq!(handler.batch_size, 500);
    }
}
