//! Metrics for node client operations.

use tape_metrics::{
    prometheus::{HistogramOpts, Opts},
    HistogramVec, IntCounterVec, Registry,
};

/// Latency buckets for client operations (in seconds).
const LATENCY_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Metrics for node client operations.
pub struct NodeClientMetrics {
    /// Request duration histogram.
    pub request_duration: HistogramVec,
    /// Total requests counter.
    pub requests_total: IntCounterVec,
    /// Bytes sent counter.
    pub bytes_sent: IntCounterVec,
    /// Bytes received counter.
    pub bytes_received: IntCounterVec,
}

impl NodeClientMetrics {
    /// Create new metrics registered with the given registry.
    pub fn new(registry: &Registry) -> Self {
        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "tape_node_client_request_duration_seconds",
                "Duration of node client requests in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["operation", "status"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(request_duration.clone())).ok();

        let requests_total = IntCounterVec::new(
            Opts::new(
                "tape_node_client_requests_total",
                "Total number of node client requests",
            ),
            &["operation", "status"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(requests_total.clone())).ok();

        let bytes_sent = IntCounterVec::new(
            Opts::new(
                "tape_node_client_bytes_sent_total",
                "Total bytes sent to storage nodes",
            ),
            &["operation"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(bytes_sent.clone())).ok();

        let bytes_received = IntCounterVec::new(
            Opts::new(
                "tape_node_client_bytes_received_total",
                "Total bytes received from storage nodes",
            ),
            &["operation"],
        )
        .expect("metric creation should not fail");
        registry.register(Box::new(bytes_received.clone())).ok();

        Self {
            request_duration,
            requests_total,
            bytes_sent,
            bytes_received,
        }
    }

    /// Record a completed request.
    pub fn record_request(&self, operation: &str, status: &str, duration: f64) {
        self.request_duration
            .with_label_values(&[operation, status])
            .observe(duration);
        self.requests_total
            .with_label_values(&[operation, status])
            .inc();
    }

    /// Record bytes sent.
    pub fn record_bytes_sent(&self, operation: &str, bytes: u64) {
        self.bytes_sent
            .with_label_values(&[operation])
            .inc_by(bytes);
    }

    /// Record bytes received.
    pub fn record_bytes_received(&self, operation: &str, bytes: u64) {
        self.bytes_received
            .with_label_values(&[operation])
            .inc_by(bytes);
    }
}
