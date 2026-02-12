//! Spool synchronization handler for epoch transitions.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use tape_core::types::network::NetworkAddress;
use tape_node_client::{NodeClientBuilder, NodeError};

use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_store::types::Pubkey;

use super::types::{SyncSlice, SyncSpoolRequest};

use crate::core::{Backoff, BackoffConfig};

/// Default batch size for sync requests.
pub const DEFAULT_BATCH_SIZE: u32 = 1000;

/// Default max concurrent sync operations.
pub const DEFAULT_MAX_CONCURRENT_SYNCS: usize = 10;

/// Default timeout before switching from spool sync to direct recovery.
pub const DEFAULT_SPOOL_SYNC_TIMEOUT: Duration = Duration::from_secs(12 * 3600);

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

    #[error("spool sync timed out for spool {0}, falling back to recovery")]
    TimedOut(SpoolIndex),
}

/// Handler for spool synchronization during epoch transitions.
pub struct SpoolSyncHandler {
    /// Semaphore to limit concurrent sync operations.
    permits: Arc<Semaphore>,
    /// Batch size for sync requests.
    batch_size: u32,
    /// Accept invalid TLS certificates (for local testing with self-signed certs).
    accept_invalid_certs: bool,
    /// Timeout before switching from spool transfer to direct recovery.
    recovery_timeout: Duration,
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
            accept_invalid_certs: false,
            recovery_timeout: DEFAULT_SPOOL_SYNC_TIMEOUT,
        }
    }

    /// Set the maximum concurrent sync operations.
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.permits = Arc::new(Semaphore::new(max));
        self
    }

    /// Set the batch size for sync requests.
    pub fn with_batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Accept invalid TLS certificates (for local testing with self-signed certs).
    ///
    /// WARNING: Only use this for local development/testing. Never enable in production.
    pub fn with_insecure(mut self, insecure: bool) -> Self {
        self.accept_invalid_certs = insecure;
        self
    }

    /// Sync a single spool from a previous owner node.
    ///
    /// # Arguments
    /// * `spool` - The spool index to sync
    /// * `from_epoch` - The epoch we're syncing from
    /// * `prev_owner_address` - Network address of the previous owner node
    /// * `on_slice` - Callback for each received slice
    /// * `resume_cursor` - Starting track pubkey to resume from (Pubkey::default() = from beginning)
    /// * `on_batch` - Optional callback after each batch with last track pubkey for cursor persistence
    /// * `cursor_out` - Updated in-place after each batch for cursor preservation across retries
    pub async fn sync_spool<F, B>(
        &self,
        spool: SpoolIndex,
        from_epoch: EpochNumber,
        prev_owner_address: NetworkAddress,
        mut on_slice: F,
        resume_cursor: Pubkey,
        on_batch: &mut Option<B>,
        cursor_out: &mut Pubkey,
    ) -> Result<usize, SyncError>
    where
        F: FnMut(SyncSlice) -> Result<(), SyncError>,
        B: FnMut(&Pubkey) -> Result<(), SyncError>,
    {
        let _permit = self.permits.acquire().await.map_err(|_| {
            SyncError::Storage("semaphore closed".to_string())
        })?;

        let addr = prev_owner_address
            .to_socket_addr()
            .map_err(|e| SyncError::Storage(format!("invalid network address: {e}")))?;
        let client = NodeClientBuilder::new()
            .accept_invalid_certs(self.accept_invalid_certs)
            .build(&addr.to_string())?;

        let mut starting_track = resume_cursor;
        let mut total_slices = 0;

        loop {
            let request = SyncSpoolRequest {
                spool_index: spool,
                starting_track,
                batch_size: self.batch_size,
                epoch: from_epoch,
            };

            let request_bytes = wincode::serialize(&request)
                .map_err(|e| SyncError::Serialization(e.to_string()))?;

            let response_bytes = client.sync_spool(request_bytes).await?;

            let slices: Vec<SyncSlice> = wincode::deserialize(&response_bytes)
                .map_err(|e| SyncError::Serialization(e.to_string()))?;

            if slices.is_empty() {
                break;
            }

            for slice in &slices {
                on_slice(slice.clone())?;
                total_slices += 1;
            }

            if let Some(last_slice) = slices.last() {
                starting_track = last_slice.track_address;
                *cursor_out = starting_track;
                if let Some(ref mut cb) = on_batch {
                    cb(&starting_track)?;
                }
            } else {
                break;
            }
        }

        info!(
            spool = spool,
            slices = total_slices,
            "spool sync complete"
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
        spools: Vec<(SpoolIndex, NetworkAddress)>,
        from_epoch: EpochNumber,
        on_slice: Arc<F>,
    ) -> Result<usize, SyncError>
    where
        F: Fn(SyncSlice) -> Result<(), SyncError> + Send + Sync + 'static,
    {
        use futures::stream::{self, StreamExt};

        let results: Vec<Result<usize, SyncError>> = stream::iter(spools)
            .map(|(spool, address): (SpoolIndex, NetworkAddress)| {
                let handler = self.clone();
                let on_slice = Arc::clone(&on_slice);

                async move {
                    let mut no_batch: Option<fn(&Pubkey) -> Result<(), SyncError>> = None;
                    let mut cursor_out = Pubkey::default();
                    handler
                        .sync_spool(
                            spool,
                            from_epoch,
                            address,
                            |slice| on_slice(slice),
                            Pubkey::default(),
                            &mut no_batch,
                            &mut cursor_out,
                        )
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

    /// Sync a spool with exponential backoff retry.
    ///
    /// Retries on transient failures with exponential backoff (60s → 10min, max 10 attempts).
    /// Returns `Err(SyncError::TimedOut)` if the total timeout or max retries are exceeded,
    /// signaling the caller should fall back to direct recovery.
    pub async fn sync_spool_with_retry<F, B>(
        &self,
        spool: SpoolIndex,
        from_epoch: EpochNumber,
        prev_owner_address: NetworkAddress,
        mut on_slice: F,
        resume_cursor: Option<Pubkey>,
        mut on_batch: Option<B>,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> Result<usize, SyncError>
    where
        F: FnMut(SyncSlice) -> Result<(), SyncError>,
        B: FnMut(&Pubkey) -> Result<(), SyncError>,
    {
        let deadline = tokio::time::Instant::now() + self.recovery_timeout;
        let mut backoff = Backoff::new(BackoffConfig::spool_sync());
        let mut cursor = resume_cursor.unwrap_or_default();

        loop {
            match self
                .sync_spool(
                    spool,
                    from_epoch,
                    prev_owner_address,
                    &mut on_slice,
                    cursor,
                    &mut on_batch,
                    &mut cursor,
                )
                .await
            {
                Ok(count) => return Ok(count),
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        warn!(
                            spool,
                            error = %e,
                            "spool sync timed out, falling back to recovery"
                        );
                        return Err(SyncError::TimedOut(spool));
                    }

                    let delay = match backoff.next_delay() {
                        Some(d) => d,
                        None => {
                            warn!(
                                spool,
                                attempts = backoff.attempt(),
                                error = %e,
                                "spool sync exhausted retries, falling back to recovery"
                            );
                            return Err(SyncError::TimedOut(spool));
                        }
                    };

                    warn!(
                        spool,
                        attempt = backoff.attempt(),
                        backoff_secs = delay.as_secs(),
                        error = %e,
                        "spool sync failed, retrying"
                    );

                    tokio::select! {
                        _ = cancel.cancelled() => return Err(SyncError::Storage("cancelled".into())),
                        _ = tokio::time::sleep(delay) => {}
                    }
                }
            }
        }
    }
}

impl Clone for SpoolSyncHandler {
    fn clone(&self) -> Self {
        Self {
            permits: Arc::clone(&self.permits),
            batch_size: self.batch_size,
            accept_invalid_certs: self.accept_invalid_certs,
            recovery_timeout: self.recovery_timeout,
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
