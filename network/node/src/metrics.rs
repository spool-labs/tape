//! Metrics for storage node operations.

use tape_metrics::prometheus::{
    self, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts, Registry,
};

/// Latency buckets for node operations (in seconds).
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

/// Metrics for storage node operations.
pub struct NodeMetrics {
    // Request metrics
    pub request_duration: HistogramVec,
    pub requests_total: IntCounterVec,

    // Slice storage metrics
    pub slices_stored_total: IntGauge,
    pub slices_retrieved_total: IntGauge,
    pub bytes_stored_total: IntGauge,
    pub bytes_retrieved_total: IntGauge,

    // Epoch metrics
    pub current_epoch: IntGauge,
    pub owned_spools: IntGauge,
    pub epoch_transitions_total: IntCounter,

    // Block processing metrics
    pub last_processed_slot: IntGauge,
    pub blocks_processed_total: IntCounter,

    // Spool sync metrics
    pub spools_synced_total: IntCounter,

    // Recovery metrics
    pub slices_recovered_total: IntCounter,
    pub recovery_queue_len: IntGauge,

    // GC metrics
    pub gc_runs_total: IntCounter,

    // Storage metrics
    pub storage_bytes_used: IntGauge,
    pub tracks_stored: IntGauge,
}

impl NodeMetrics {
    /// Create new metrics with the default global registry.
    pub fn new() -> Self {
        Self::with_registry(prometheus::default_registry())
    }

    /// Create new metrics registered with the given registry.
    pub fn with_registry(registry: &Registry) -> Self {
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "tape_node_request_duration_seconds",
                "Duration of node API requests in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["endpoint", "status"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(request_duration.clone())).ok();

        let requests_total = IntCounterVec::new(
            Opts::new("tape_node_requests_total", "Total number of API requests"),
            &["endpoint", "status"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(requests_total.clone())).ok();

        let slices_stored_total = IntGauge::new(
            "tape_node_slices_stored_total",
            "Total number of slices stored",
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(slices_stored_total.clone())).ok();

        let slices_retrieved_total = IntGauge::new(
            "tape_node_slices_retrieved_total",
            "Total number of slices retrieved",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(slices_retrieved_total.clone()))
            .ok();

        let bytes_stored_total =
            IntGauge::new("tape_node_bytes_stored_total", "Total bytes stored")
                .expect("metric creation should not fail");
        registry.register(Box::new(bytes_stored_total.clone())).ok();

        let bytes_retrieved_total =
            IntGauge::new("tape_node_bytes_retrieved_total", "Total bytes retrieved")
                .expect("metric creation should not fail");
        registry
            .register(Box::new(bytes_retrieved_total.clone()))
            .ok();

        let current_epoch =
            IntGauge::new("tape_node_current_epoch", "Current epoch number")
                .expect("metric creation should not fail");
        registry.register(Box::new(current_epoch.clone())).ok();

        let owned_spools = IntGauge::new(
            "tape_node_owned_spools",
            "Number of spools owned by this node",
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(owned_spools.clone())).ok();

        let storage_bytes_used = IntGauge::new(
            "tape_node_storage_bytes_used",
            "Storage bytes currently used",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(storage_bytes_used.clone()))
            .ok();

        let tracks_stored =
            IntGauge::new("tape_node_tracks_stored", "Number of tracks stored")
                .expect("metric creation should not fail");
        registry.register(Box::new(tracks_stored.clone())).ok();

        let epoch_transitions_total = IntCounter::new(
            "tape_node_epoch_transitions_total",
            "Total number of epoch transitions processed",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(epoch_transitions_total.clone()))
            .ok();

        let last_processed_slot = IntGauge::new(
            "tape_node_last_processed_slot",
            "Last Solana slot processed by the node",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(last_processed_slot.clone()))
            .ok();

        let blocks_processed_total = IntCounter::new(
            "tape_node_blocks_processed_total",
            "Total number of blocks processed",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(blocks_processed_total.clone()))
            .ok();

        let spools_synced_total = IntCounter::new(
            "tape_node_spools_synced_total",
            "Total number of spools synced during epoch transitions",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(spools_synced_total.clone()))
            .ok();

        let slices_recovered_total = IntCounter::new(
            "tape_node_slices_recovered_total",
            "Total number of slices recovered via erasure coding",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(slices_recovered_total.clone()))
            .ok();

        let recovery_queue_len = IntGauge::new(
            "tape_node_recovery_queue_len",
            "Number of slices pending recovery",
        )
        .expect("metric creation should not fail");
        registry
            .register(Box::new(recovery_queue_len.clone()))
            .ok();

        let gc_runs_total = IntCounter::new(
            "tape_node_gc_runs_total",
            "Total number of garbage collection runs",
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(gc_runs_total.clone())).ok();

        Self {
            request_duration,
            requests_total,
            slices_stored_total,
            slices_retrieved_total,
            bytes_stored_total,
            bytes_retrieved_total,
            current_epoch,
            owned_spools,
            epoch_transitions_total,
            last_processed_slot,
            blocks_processed_total,
            spools_synced_total,
            slices_recovered_total,
            recovery_queue_len,
            gc_runs_total,
            storage_bytes_used,
            tracks_stored,
        }
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeMetrics {
    /// Record a completed request.
    pub fn record_request(&self, endpoint: &str, status: &str, duration: f64) {
        self.request_duration
            .with_label_values(&[endpoint, status])
            .observe(duration);
        self.requests_total
            .with_label_values(&[endpoint, status])
            .inc();
    }
}
