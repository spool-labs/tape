//! TrackSyncHandler — manages concurrent per-track recovery tasks.
//!
//! Provides semaphore-based concurrency limiting for track sync operations.
//! Each track sync acquires a track permit before proceeding, and each
//! individual slice download within that track acquires a slice permit.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn, Instrument};

use tape_store::types::Pubkey;

/// Default maximum concurrent track sync tasks.
pub const DEFAULT_MAX_TRACK_SYNCS: usize = 100;

/// Default maximum concurrent slice downloads across all track syncs.
pub const DEFAULT_MAX_SLICE_SYNCS: usize = 2000;

/// Manages lifecycle and concurrency of per-track recovery operations.
pub struct TrackSyncHandler {
    /// Limits concurrent track-level sync tasks.
    track_semaphore: Arc<Semaphore>,
    /// Limits concurrent slice-level downloads (shared across all tracks).
    slice_semaphore: Arc<Semaphore>,
    /// Active track sync tasks for cancellation and monitoring.
    in_progress: RwLock<HashMap<Pubkey, (JoinHandle<()>, CancellationToken)>>,
}

impl TrackSyncHandler {
    /// Create a new TrackSyncHandler with default concurrency limits.
    pub fn new() -> Self {
        Self {
            track_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_TRACK_SYNCS)),
            slice_semaphore: Arc::new(Semaphore::new(DEFAULT_MAX_SLICE_SYNCS)),
            in_progress: RwLock::new(HashMap::new()),
        }
    }

    /// Create with custom concurrency limits.
    pub fn with_limits(max_track_syncs: usize, max_slice_syncs: usize) -> Self {
        Self {
            track_semaphore: Arc::new(Semaphore::new(max_track_syncs)),
            slice_semaphore: Arc::new(Semaphore::new(max_slice_syncs)),
            in_progress: RwLock::new(HashMap::new()),
        }
    }

    /// Get the shared slice semaphore (for use by TrackSynchronizer).
    pub fn slice_semaphore(&self) -> Arc<Semaphore> {
        Arc::clone(&self.slice_semaphore)
    }

    /// Start a track sync task. The provided future will be spawned with a
    /// track-level concurrency permit.
    ///
    /// If a sync for this track is already in progress, the existing one is
    /// cancelled first. Completed entries are evicted opportunistically.
    pub async fn start_sync<F>(
        &self,
        track_address: Pubkey,
        task: F,
    )
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        // Opportunistically evict completed entries
        {
            let mut map = self.in_progress.write().await;
            map.retain(|_, (h, _)| !h.is_finished());
        }

        // Cancel any existing sync for this track
        self.cancel_sync(&track_address).await;

        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        let permit = Arc::clone(&self.track_semaphore);

        let span = tracing::Span::current();
        let handle = tokio::spawn(async move {
            let _permit = match permit.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    warn!("track semaphore closed");
                    return;
                }
            };

            tokio::select! {
                _ = cancel_clone.cancelled() => {
                    debug!("track sync cancelled");
                }
                _ = task => {}
            }
        }.instrument(span));

        self.in_progress
            .write()
            .await
            .insert(track_address, (handle, cancel));
    }

    /// Cancel an in-progress track sync.
    pub async fn cancel_sync(&self, track_address: &Pubkey) {
        if let Some((handle, cancel)) = self.in_progress.write().await.remove(track_address) {
            cancel.cancel();
            let _ = handle.await;
        }
    }

    /// Cancel all in-progress track syncs.
    pub async fn cancel_all(&self) {
        let tasks: Vec<(Pubkey, (JoinHandle<()>, CancellationToken))> =
            self.in_progress.write().await.drain().collect();

        for (_, (handle, cancel)) in tasks {
            cancel.cancel();
            let _ = handle.await;
        }
    }

    /// Number of currently in-progress syncs.
    pub async fn active_count(&self) -> usize {
        self.in_progress.read().await.len()
    }

    /// Wait for all in-progress syncs to complete.
    pub async fn wait_all(&self, cancel: &CancellationToken) {
        loop {
            // Check if empty
            {
                let map = self.in_progress.read().await;
                if map.is_empty() {
                    break;
                }
            }

            // Yield to let tasks make progress
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }

            // Clean up completed entries
            let mut map = self.in_progress.write().await;
            map.retain(|_, (h, _)| !h.is_finished());
            if map.is_empty() {
                break;
            }
        }
    }
}

impl Default for TrackSyncHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_start_and_complete() {
        let handler = TrackSyncHandler::new();
        let track = Pubkey::new([1u8; 32]);

        handler
            .start_sync(track, async {
                // Simulate work
            })
            .await;

        // Give the task time to complete
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let cancel = CancellationToken::new();
        handler.wait_all(&cancel).await;
        assert_eq!(handler.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_cancel_sync() {
        let handler = TrackSyncHandler::new();
        let track = Pubkey::new([2u8; 32]);

        handler
            .start_sync(track, async {
                tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            })
            .await;

        handler.cancel_sync(&track).await;
        assert_eq!(handler.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_cancel_all() {
        let handler = TrackSyncHandler::new();

        for i in 0..5u8 {
            let track = Pubkey::new([i; 32]);
            handler
                .start_sync(track, async {
                    tokio::time::sleep(std::time::Duration::from_secs(100)).await;
                })
                .await;
        }

        assert_eq!(handler.active_count().await, 5);
        handler.cancel_all().await;
        assert_eq!(handler.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_duplicate_replaces_existing() {
        let handler = TrackSyncHandler::new();
        let track = Pubkey::new([3u8; 32]);

        handler
            .start_sync(track, async {
                tokio::time::sleep(std::time::Duration::from_secs(100)).await;
            })
            .await;

        // Starting another sync for the same track cancels the first
        handler
            .start_sync(track, async {
                // quick task
            })
            .await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let cancel = CancellationToken::new();
        handler.wait_all(&cancel).await;
        assert_eq!(handler.active_count().await, 0);
    }
}
