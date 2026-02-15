//! Prometheus metrics for node client operations.

use prometheus::{HistogramVec, IntCounterVec, Registry};

/// Latency buckets for client operations (in seconds).
const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Metrics for node client operations.
pub struct NodeClientMetrics {
    pub request_duration: HistogramVec,
    pub requests_total: IntCounterVec,
    pub bytes_sent: IntCounterVec,
    pub bytes_received: IntCounterVec,
}

impl NodeClientMetrics {
    /// Register metrics with the given Prometheus registry.
    pub fn new(registry: &Registry) -> Self {
        let request_duration = HistogramVec::new(
            prometheus::histogram_opts!(
                "node_client_request_duration_seconds",
                "Duration of node client HTTP requests",
                LATENCY_BUCKETS.to_vec()
            ),
            &["operation", "status"],
        )
        .unwrap();
        registry.register(Box::new(request_duration.clone())).unwrap();

        let requests_total = IntCounterVec::new(
            prometheus::opts!(
                "node_client_requests_total",
                "Total number of node client HTTP requests"
            ),
            &["operation", "status"],
        )
        .unwrap();
        registry.register(Box::new(requests_total.clone())).unwrap();

        let bytes_sent = IntCounterVec::new(
            prometheus::opts!(
                "node_client_bytes_sent_total",
                "Total bytes sent by node client"
            ),
            &["operation"],
        )
        .unwrap();
        registry.register(Box::new(bytes_sent.clone())).unwrap();

        let bytes_received = IntCounterVec::new(
            prometheus::opts!(
                "node_client_bytes_received_total",
                "Total bytes received by node client"
            ),
            &["operation"],
        )
        .unwrap();
        registry.register(Box::new(bytes_received.clone())).unwrap();

        Self {
            request_duration,
            requests_total,
            bytes_sent,
            bytes_received,
        }
    }

    /// Record a completed request with its operation name, status, and duration.
    pub fn record_request(&self, operation: &str, status: &str, duration: f64) {
        self.request_duration
            .with_label_values(&[operation, status])
            .observe(duration);
        self.requests_total
            .with_label_values(&[operation, status])
            .inc();
    }

    /// Record bytes sent for an operation.
    pub fn record_bytes_sent(&self, operation: &str, bytes: u64) {
        self.bytes_sent
            .with_label_values(&[operation])
            .inc_by(bytes);
    }

    /// Record bytes received for an operation.
    pub fn record_bytes_received(&self, operation: &str, bytes: u64) {
        self.bytes_received
            .with_label_values(&[operation])
            .inc_by(bytes);
    }
}
