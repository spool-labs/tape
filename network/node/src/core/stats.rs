//! Runtime statistics — atomic counters for observability.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters incremented by pipeline components and read by the stats handler.
pub struct RuntimeStats {
    pub blocks_processed: AtomicU64,
    pub epoch_transitions: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub bytes_downloaded: AtomicU64,
}

impl Default for RuntimeStats {
    fn default() -> Self {
        Self {
            blocks_processed: AtomicU64::new(0),
            epoch_transitions: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
        }
    }
}

impl RuntimeStats {
    pub fn inc_blocks(&self) {
        self.blocks_processed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_epochs(&self) {
        self.epoch_transitions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_uploaded(&self, n: u64) {
        self.bytes_uploaded.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_downloaded(&self, n: u64) {
        self.bytes_downloaded.fetch_add(n, Ordering::Relaxed);
    }
}
