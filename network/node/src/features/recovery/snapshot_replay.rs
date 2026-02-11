//! Snapshot replay stub.
//!
//! When a node is far behind (lag >= replay_threshold), it should download
//! an epoch snapshot and replay from there instead of processing all
//! intermediate blocks. This is not yet implemented — the node falls through
//! to standard block processing.

use tracing::error;

/// Threshold (in epochs) above which snapshot replay should be attempted.
pub const DEFAULT_REPLAY_THRESHOLD: u64 = 10;

/// Stub implementation of snapshot-based epoch replay.
pub struct SnapshotReplay {
    /// How far behind the node is (in epochs).
    pub lag: u64,
    /// Lag threshold to trigger snapshot replay.
    pub replay_threshold: u64,
}

impl SnapshotReplay {
    pub fn new(lag: u64) -> Self {
        Self {
            lag,
            replay_threshold: DEFAULT_REPLAY_THRESHOLD,
        }
    }

    /// Attempt snapshot replay. Currently a stub that logs a warning
    /// and falls through to block processing.
    pub async fn run(&self) -> Result<(), SnapshotReplayError> {
        if self.lag <= self.replay_threshold {
            return Ok(());
        }

        error!(
            lag = self.lag,
            threshold = self.replay_threshold,
            "Node is behind but epoch snapshots are not yet available. \
             Falling through to block processing."
        );
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotReplayError {
    #[error("snapshot not available for epoch")]
    NotAvailable,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn below_threshold_is_noop() {
        let replay = SnapshotReplay::new(3);
        assert!(replay.run().await.is_ok());
    }

    #[tokio::test]
    async fn above_threshold_falls_through() {
        let replay = SnapshotReplay::new(50);
        assert!(replay.run().await.is_ok());
    }
}
