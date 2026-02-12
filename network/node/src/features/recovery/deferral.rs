//! Live upload deferral.
//!
//! When a track is actively being recovered, live upload requests for
//! the same track should be deferred briefly to avoid conflicting writes.
//! After `max_total_defer`, uploads proceed regardless.
//!
//! Backed by `CleanupMap` for automatic eviction of stale entries.

use std::time::Duration;

use tape_store::types::Pubkey;
use tokio_util::sync::CancellationToken;

use crate::core::CleanupMap;

/// Default maximum deferral time per track (production).
pub const DEFAULT_MAX_TOTAL_DEFER: Duration = Duration::from_secs(120);

/// Manages deferral of live uploads during recovery.
pub struct LiveUploadDeferral {
    deferrals: CleanupMap<Pubkey, CancellationToken>,
}

impl LiveUploadDeferral {
    pub fn new(max_total_defer: Duration) -> Self {
        Self {
            deferrals: CleanupMap::new(max_total_defer),
        }
    }

    /// Register that a track is being recovered. Returns a cancellation token
    /// that should be cancelled when recovery completes.
    pub async fn begin_recovery(&self, track: Pubkey) -> CancellationToken {
        let cancel = CancellationToken::new();
        self.deferrals.insert(track, cancel.clone()).await;
        cancel
    }

    /// Mark recovery as complete for a track.
    pub async fn end_recovery(&self, track: &Pubkey) {
        if let Some(cancel) = self.deferrals.remove(track).await {
            cancel.cancel();
        }
    }

    /// Wait for a recovery window if the track is being recovered.
    ///
    /// Returns immediately if:
    /// - Track is not being recovered
    /// - Max deferral time has elapsed (CleanupMap TTL expired)
    ///
    /// Otherwise waits until recovery completes or timeout.
    pub async fn wait_for_recovery_window(&self, track: &Pubkey) {
        let cancel = self.deferrals.get(track, |c| c.clone()).await;
        let Some(cancel) = cancel else { return };

        let ttl = self.deferrals.ttl();
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = tokio::time::sleep(ttl) => {}
        }
    }

    /// Number of tracks currently deferred.
    pub async fn active_count(&self) -> usize {
        self.deferrals.len().await
    }

    /// Cancel token for the background cleanup task.
    pub fn cancel_token(&self) -> CancellationToken {
        self.deferrals.cancel_token()
    }

    /// Run the background cleanup loop. Must be spawned as a task.
    pub async fn run_cleanup(&self) {
        self.deferrals.run_cleanup().await;
    }
}

impl Default for LiveUploadDeferral {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_TOTAL_DEFER)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_deferral_for_unknown_track() {
        let d = LiveUploadDeferral::default();
        let track = Pubkey([1u8; 32]);
        // Should return immediately
        d.wait_for_recovery_window(&track).await;
    }

    #[tokio::test]
    async fn deferral_ends_on_recovery_complete() {
        let d = LiveUploadDeferral::new(Duration::from_secs(60));
        let track = Pubkey([2u8; 32]);

        let _cancel = d.begin_recovery(track).await;
        assert_eq!(d.active_count().await, 1);

        d.end_recovery(&track).await;
        assert_eq!(d.active_count().await, 0);
    }

    #[tokio::test]
    async fn deferral_returns_after_timeout() {
        let d = LiveUploadDeferral::new(Duration::from_millis(50));
        let track = Pubkey([3u8; 32]);

        let _cancel = d.begin_recovery(track).await;

        // Wait slightly longer than timeout
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Should return immediately since CleanupMap TTL expired (get returns None)
        d.wait_for_recovery_window(&track).await;
    }
}
