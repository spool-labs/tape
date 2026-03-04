//! Runtime statistics — atomic counters for observability.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters incremented by pipeline components and read by the stats handler.
pub struct RuntimeStats {
    pub events: AtomicU64,
    pub blocks_processed: AtomicU64,
    pub epoch_transitions: AtomicU64,
    pub bytes_uploaded: AtomicU64,
    pub bytes_downloaded: AtomicU64,
    pub repair_bytes_received: AtomicU64,
    pub recovery_bytes_received: AtomicU64,
    pub sync_bytes_received: AtomicU64,
}

impl Default for RuntimeStats {
    fn default() -> Self {
        Self {
            events: AtomicU64::new(0),
            blocks_processed: AtomicU64::new(0),
            epoch_transitions: AtomicU64::new(0),
            bytes_uploaded: AtomicU64::new(0),
            bytes_downloaded: AtomicU64::new(0),
            repair_bytes_received: AtomicU64::new(0),
            recovery_bytes_received: AtomicU64::new(0),
            sync_bytes_received: AtomicU64::new(0),
        }
    }
}

impl RuntimeStats {
    pub fn inc_events(&self) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }

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

    pub fn add_repair_received(&self, n: u64) {
        self.repair_bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_recovery_received(&self, n: u64) {
        self.recovery_bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_sync_received(&self, n: u64) {
        self.sync_bytes_received.fetch_add(n, Ordering::Relaxed);
    }
}
