use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeMetricsSnapshot {
    pub requests_total: u64,
    pub events_total: u64,
    pub blocks_processed_total: u64,
    pub epoch_transitions_total: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub sync_bytes_fetched: u64,
    pub sync_bytes_persisted: u64,
    pub repair_bytes_fetched: u64,
    pub repair_bytes_persisted: u64,
    pub repair_escalations: u64,
    pub recover_bytes_fetched: u64,
    pub recover_bytes_persisted: u64,
}

#[derive(Debug, Default)]
pub struct NodeMetrics {
    requests_total: AtomicU64,
    events_total: AtomicU64,
    blocks_processed_total: AtomicU64,
    epoch_transitions_total: AtomicU64,
    bytes_uploaded: AtomicU64,
    bytes_downloaded: AtomicU64,
    sync_bytes_fetched: AtomicU64,
    sync_bytes_persisted: AtomicU64,
    repair_bytes_fetched: AtomicU64,
    repair_bytes_persisted: AtomicU64,
    repair_escalations: AtomicU64,
    recover_bytes_fetched: AtomicU64,
    recover_bytes_persisted: AtomicU64,
}

impl NodeMetrics {
    pub fn snapshot(&self) -> NodeMetricsSnapshot {
        NodeMetricsSnapshot {
            requests_total: self.requests_total.load(Ordering::Relaxed),
            events_total: self.events_total.load(Ordering::Relaxed),
            blocks_processed_total: self.blocks_processed_total.load(Ordering::Relaxed),
            epoch_transitions_total: self.epoch_transitions_total.load(Ordering::Relaxed),
            bytes_uploaded: self.bytes_uploaded.load(Ordering::Relaxed),
            bytes_downloaded: self.bytes_downloaded.load(Ordering::Relaxed),
            sync_bytes_fetched: self.sync_bytes_fetched.load(Ordering::Relaxed),
            sync_bytes_persisted: self.sync_bytes_persisted.load(Ordering::Relaxed),
            repair_bytes_fetched: self.repair_bytes_fetched.load(Ordering::Relaxed),
            repair_bytes_persisted: self.repair_bytes_persisted.load(Ordering::Relaxed),
            repair_escalations: self.repair_escalations.load(Ordering::Relaxed),
            recover_bytes_fetched: self.recover_bytes_fetched.load(Ordering::Relaxed),
            recover_bytes_persisted: self.recover_bytes_persisted.load(Ordering::Relaxed),
        }
    }

    pub fn inc_requests_total(&self) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_events(&self, n: u64) {
        self.events_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_blocks_processed(&self) {
        self.blocks_processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_epoch_transitions(&self) {
        self.epoch_transitions_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_uploaded(&self, n: u64) {
        self.bytes_uploaded.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_downloaded(&self, n: u64) {
        self.bytes_downloaded.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_sync_fetched(&self, n: u64) {
        self.sync_bytes_fetched.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_sync_persisted(&self, n: u64) {
        self.sync_bytes_persisted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_repair_fetched(&self, n: u64) {
        self.repair_bytes_fetched.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_repair_persisted(&self, n: u64) {
        self.repair_bytes_persisted.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_repair_escalations(&self) {
        self.repair_escalations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add_recover_fetched(&self, n: u64) {
        self.recover_bytes_fetched.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_recover_persisted(&self, n: u64) {
        self.recover_bytes_persisted.fetch_add(n, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::{NodeMetrics, NodeMetricsSnapshot};

    #[test]
    fn snapshots() {
        let metrics = NodeMetrics::default();
        metrics.inc_requests_total();
        metrics.add_events(3);
        metrics.inc_blocks_processed();
        metrics.inc_epoch_transitions();
        metrics.add_uploaded(5);
        metrics.add_downloaded(7);
        metrics.add_sync_fetched(11);
        metrics.add_sync_persisted(13);
        metrics.add_repair_fetched(17);
        metrics.add_repair_persisted(19);
        metrics.inc_repair_escalations();
        metrics.add_recover_fetched(23);
        metrics.add_recover_persisted(29);

        assert_eq!(
            metrics.snapshot(),
            NodeMetricsSnapshot {
                requests_total: 1,
                events_total: 3,
                blocks_processed_total: 1,
                epoch_transitions_total: 1,
                bytes_uploaded: 5,
                bytes_downloaded: 7,
                sync_bytes_fetched: 11,
                sync_bytes_persisted: 13,
                repair_bytes_fetched: 17,
                repair_bytes_persisted: 19,
                repair_escalations: 1,
                recover_bytes_fetched: 23,
                recover_bytes_persisted: 29,
            }
        );
    }
}
