//! Node metrics facade over the global metric set.

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
    pub repair_to_recovery: u64,
    pub recover_bytes_fetched: u64,
    pub recover_bytes_persisted: u64,
}

#[derive(Debug, Default)]
pub struct NodeMetrics;

#[cfg(feature = "metrics")]
fn spool(op: &str, stage: &str) -> u64 {
    tape_metrics::metrics()
        .spool_bytes_total
        .with_label_values(&[op, stage])
        .get()
}

impl NodeMetrics {
    pub fn snapshot(&self) -> NodeMetricsSnapshot {
        #[cfg(feature = "metrics")]
        {
            let m = tape_metrics::metrics();
            NodeMetricsSnapshot {
                requests_total: m.requests_total.get(),
                events_total: m.replay_events_total.get(),
                blocks_processed_total: m.blocks_processed_total.get(),
                epoch_transitions_total: m.epoch_transitions_total.get(),
                bytes_uploaded: m.bytes_uploaded.get(),
                bytes_downloaded: m.bytes_downloaded.get(),
                sync_bytes_fetched: spool("sync", "fetched"),
                sync_bytes_persisted: spool("sync", "persisted"),
                repair_bytes_fetched: spool("repair", "fetched"),
                repair_bytes_persisted: spool("repair", "persisted"),
                repair_to_recovery: m.repair_attempts_total.with_label_values(&["to_recovery"]).get(),
                recover_bytes_fetched: spool("recover", "fetched"),
                recover_bytes_persisted: spool("recover", "persisted"),
            }
        }
        #[cfg(not(feature = "metrics"))]
        NodeMetricsSnapshot::default()
    }

    pub fn inc_requests_total(&self) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().requests_total.inc();
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn add_events(&self, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().replay_events_total.inc_by(n);
    }

    pub fn inc_blocks_processed(&self) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().blocks_processed_total.inc();
    }

    pub fn inc_epoch_transitions(&self) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().epoch_transitions_total.inc();
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn add_uploaded(&self, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().bytes_uploaded.inc_by(n);
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn add_downloaded(&self, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().bytes_downloaded.inc_by(n);
    }

    pub fn add_sync_fetched(&self, n: u64) {
        self.add_spool("sync", "fetched", n);
    }

    pub fn add_sync_persisted(&self, n: u64) {
        self.add_spool("sync", "persisted", n);
    }

    pub fn add_repair_fetched(&self, n: u64) {
        self.add_spool("repair", "fetched", n);
    }

    pub fn add_repair_persisted(&self, n: u64) {
        self.add_spool("repair", "persisted", n);
    }

    pub fn add_recover_fetched(&self, n: u64) {
        self.add_spool("recover", "fetched", n);
    }

    pub fn add_recover_persisted(&self, n: u64) {
        self.add_spool("recover", "persisted", n);
    }

    pub fn inc_repair_to_recovery(&self) {
        self.inc_repair_attempts("to_recovery");
    }

    pub fn inc_repair_repaired(&self) {
        self.inc_repair_attempts("repaired");
    }

    pub fn inc_repair_put_back(&self) {
        self.inc_repair_attempts("put_back");
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn inc_repair_missing(&self, cause: &str) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics()
            .repair_missing_total
            .with_label_values(&[cause])
            .inc();
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn add_repair_missing_helper_stripes(&self, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().repair_missing_helper_stripes_total.inc_by(n);
    }

    pub fn inc_spool_missing_encoding(&self) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().spool_missing_encoding_total.inc();
    }

    pub fn inc_recover_recovered(&self) {
        self.inc_recover_tries("recovered");
    }

    pub fn inc_recover_put_back(&self) {
        self.inc_recover_tries("put_back");
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    pub fn add_recover_rejected(&self, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics().recover_rejected_slices_total.inc_by(n);
    }

    pub fn add_sync_stored(&self, n: u64) {
        self.add_sync_tracks("stored", n);
    }

    pub fn add_sync_taken_back(&self, n: u64) {
        self.add_sync_tracks("taken_back", n);
    }

    pub fn add_sync_window(&self, n: u64) {
        self.add_sync_tracks("window", n);
    }

    pub fn add_sync_missing(&self, n: u64) {
        self.add_sync_tracks("missing", n);
    }

    pub fn add_sync_invalid(&self, n: u64) {
        self.add_sync_tracks("invalid", n);
    }

    pub fn add_sync_failed(&self, n: u64) {
        self.add_sync_tracks("failed", n);
    }

    pub fn add_sync_in_flight(&self, n: u64) {
        self.add_sync_tracks("in_flight", n);
    }

    pub fn add_sync_inline_stored(&self, n: u64) {
        self.add_sync_tracks("inline_stored", n);
    }

    pub fn add_sync_inline_missing(&self, n: u64) {
        self.add_sync_tracks("inline_missing", n);
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn add_spool(&self, op: &str, stage: &str, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics()
            .spool_bytes_total
            .with_label_values(&[op, stage])
            .inc_by(n);
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn inc_repair_attempts(&self, outcome: &str) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics()
            .repair_attempts_total
            .with_label_values(&[outcome])
            .inc();
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn inc_recover_tries(&self, outcome: &str) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics()
            .recover_tries_total
            .with_label_values(&[outcome])
            .inc();
    }

    #[cfg_attr(not(feature = "metrics"), allow(unused_variables))]
    fn add_sync_tracks(&self, outcome: &str, n: u64) {
        #[cfg(feature = "metrics")]
        tape_metrics::metrics()
            .sync_tracks_total
            .with_label_values(&[outcome])
            .inc_by(n);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::NodeMetrics;

    // The metric set is a process-global singleton, so this asserts the facade
    // maps each call onto the right series via before/after deltas rather than
    // absolute values.
    #[test]
    fn facade_maps_to_snapshot_fields() {
        let m = NodeMetrics;
        let before = m.snapshot();

        m.inc_requests_total();
        m.add_events(3);
        m.inc_blocks_processed();
        m.inc_epoch_transitions();
        m.add_uploaded(5);
        m.add_downloaded(7);
        m.add_sync_fetched(11);
        m.add_sync_persisted(13);
        m.add_repair_fetched(17);
        m.add_repair_persisted(19);
        m.inc_repair_to_recovery();
        m.add_recover_fetched(23);
        m.add_recover_persisted(29);

        let after = m.snapshot();
        assert_eq!(after.requests_total - before.requests_total, 1);
        assert_eq!(after.events_total - before.events_total, 3);
        assert_eq!(after.blocks_processed_total - before.blocks_processed_total, 1);
        assert_eq!(after.epoch_transitions_total - before.epoch_transitions_total, 1);
        assert_eq!(after.bytes_uploaded - before.bytes_uploaded, 5);
        assert_eq!(after.bytes_downloaded - before.bytes_downloaded, 7);
        assert_eq!(after.sync_bytes_fetched - before.sync_bytes_fetched, 11);
        assert_eq!(after.sync_bytes_persisted - before.sync_bytes_persisted, 13);
        assert_eq!(after.repair_bytes_fetched - before.repair_bytes_fetched, 17);
        assert_eq!(after.repair_bytes_persisted - before.repair_bytes_persisted, 19);
        assert_eq!(after.repair_to_recovery - before.repair_to_recovery, 1);
        assert_eq!(after.recover_bytes_fetched - before.recover_bytes_fetched, 23);
        assert_eq!(after.recover_bytes_persisted - before.recover_bytes_persisted, 29);
    }

    // each repair and recovery outcome lands on its own series
    #[test]
    fn repair_and_recover_counters() {
        let m = NodeMetrics;
        let set = tape_metrics::metrics();
        let attempts = |outcome: &str| set.repair_attempts_total.with_label_values(&[outcome]).get();
        let missing = |cause: &str| set.repair_missing_total.with_label_values(&[cause]).get();
        let tries = |outcome: &str| set.recover_tries_total.with_label_values(&[outcome]).get();
        let before = (
            attempts("repaired"),
            attempts("put_back"),
            missing("deadline"),
            set.repair_missing_helper_stripes_total.get(),
            tries("recovered"),
            tries("put_back"),
            set.recover_rejected_slices_total.get(),
        );

        m.inc_repair_repaired();
        m.inc_repair_put_back();
        m.inc_repair_missing("deadline");
        m.add_repair_missing_helper_stripes(3);
        m.inc_recover_recovered();
        m.inc_recover_put_back();
        m.add_recover_rejected(2);

        assert_eq!(attempts("repaired") - before.0, 1);
        assert_eq!(attempts("put_back") - before.1, 1);
        assert_eq!(missing("deadline") - before.2, 1);
        assert_eq!(set.repair_missing_helper_stripes_total.get() - before.3, 3);
        assert_eq!(tries("recovered") - before.4, 1);
        assert_eq!(tries("put_back") - before.5, 1);
        assert_eq!(set.recover_rejected_slices_total.get() - before.6, 2);
    }
}
