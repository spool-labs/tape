//! Metrics for Solana RPC operations.
//!
//! This module defines Prometheus metrics for tracking RPC operations,
//! including request durations, error rates, retries, and failovers.

use tape_metrics::{HistogramVec, IntCounterVec, IntGauge, Registry};

/// Latency buckets for RPC operations (in seconds).
///
/// These buckets are designed to capture the full range of RPC latencies,
/// from fast local calls (5ms) to slow operations (60s+).
const LATENCY_BUCKETS: &[f64] = &[
    0.005, // 5ms
    0.01,  // 10ms
    0.025, // 25ms
    0.05,  // 50ms
    0.1,   // 100ms
    0.25,  // 250ms
    0.5,   // 500ms
    1.0,   // 1s
    2.5,   // 2.5s
    5.0,   // 5s
    10.0,  // 10s
    30.0,  // 30s
    60.0,  // 60s
];

/// Metrics for RPC operations.
///
/// This struct holds all Prometheus metrics for tracking RPC behavior,
/// including latency, success/failure rates, retries, and failovers.
///
/// # Example
///
/// ```ignore
/// use tape_metrics::MetricsRegistry;
/// use rpc_solana::metrics::RpcMetrics;
///
/// let registry = MetricsRegistry::init();
/// let metrics = RpcMetrics::new(registry.prometheus_registry());
///
/// // Record a successful request
/// metrics.record_request("getAccountInfo", "success", 0.025);
///
/// // Record a retry
/// metrics.record_retry("getAccountInfo", "timeout");
/// ```
pub struct RpcMetrics {
    /// Histogram of RPC request durations by method and status.
    ///
    /// Labels:
    /// - `method`: The RPC method name (e.g., "getAccountInfo")
    /// - `status`: "success" or "error"
    request_duration: HistogramVec,

    /// Counter of total RPC requests by method and status.
    ///
    /// Labels:
    /// - `method`: The RPC method name
    /// - `status`: "success" or "error"
    requests_total: IntCounterVec,

    /// Counter of RPC retries by method and reason.
    ///
    /// Labels:
    /// - `method`: The RPC method name
    /// - `reason`: The retry reason (e.g., "timeout", "connection_error")
    retries_total: IntCounterVec,

    /// Counter of endpoint failovers by source endpoint and reason.
    ///
    /// Labels:
    /// - `from_endpoint`: The endpoint we're failing over from
    /// - `reason`: The failover reason (e.g., "timeout", "connection_error")
    failovers_total: IntCounterVec,

    /// Counter of RPC errors by method and error type.
    ///
    /// Labels:
    /// - `method`: The RPC method name
    /// - `error_type`: The error category (e.g., "timeout", "connection", "server")
    errors_total: IntCounterVec,

    /// Gauge of the current endpoint index.
    ///
    /// This helps track which endpoint is currently in use.
    current_endpoint: IntGauge,

    /// Gauge of total configured endpoints.
    ///
    /// This is set once at initialization and helps calculate endpoint availability.
    endpoints_configured: IntGauge,
}

impl RpcMetrics {
    /// Create a new RpcMetrics instance and register all metrics with the given registry.
    ///
    /// # Panics
    ///
    /// Panics if any metric registration fails. This should only happen if there's
    /// a naming conflict with existing metrics.
    ///
    /// Note: If metrics are already registered (e.g., in tests), this will retrieve
    /// the existing metrics from the registry rather than fail.
    pub fn new(registry: &Registry) -> Self {
        use tape_metrics::prometheus::{HistogramOpts, Opts};

        let request_duration = HistogramVec::new(
            HistogramOpts::new(
                "tape_rpc_request_duration_seconds",
                "Duration of RPC requests in seconds",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["method", "status"],
        )
        .unwrap();
        registry.register(Box::new(request_duration.clone())).ok();

        let requests_total = IntCounterVec::new(
            Opts::new("tape_rpc_requests_total", "Total number of RPC requests"),
            &["method", "status"],
        )
        .unwrap();
        registry.register(Box::new(requests_total.clone())).ok();

        let retries_total = IntCounterVec::new(
            Opts::new("tape_rpc_retries_total", "Total number of RPC retries"),
            &["method", "reason"],
        )
        .unwrap();
        registry.register(Box::new(retries_total.clone())).ok();

        let failovers_total = IntCounterVec::new(
            Opts::new(
                "tape_rpc_failovers_total",
                "Total number of endpoint failovers",
            ),
            &["from_endpoint", "reason"],
        )
        .unwrap();
        registry.register(Box::new(failovers_total.clone())).ok();

        let errors_total = IntCounterVec::new(
            Opts::new("tape_rpc_errors_total", "Total number of RPC errors"),
            &["method", "error_type"],
        )
        .unwrap();
        registry.register(Box::new(errors_total.clone())).ok();

        let current_endpoint = IntGauge::new(
            "tape_rpc_current_endpoint",
            "Index of the current RPC endpoint",
        )
        .unwrap();
        registry.register(Box::new(current_endpoint.clone())).ok();

        let endpoints_configured = IntGauge::new(
            "tape_rpc_endpoints_configured",
            "Total number of configured RPC endpoints",
        )
        .unwrap();
        registry.register(Box::new(endpoints_configured.clone())).ok();

        Self {
            request_duration,
            requests_total,
            retries_total,
            failovers_total,
            errors_total,
            current_endpoint,
            endpoints_configured,
        }
    }

    /// Record a completed RPC request.
    pub fn record_request(&self, method: &str, status: &str, duration: f64) {
        self.request_duration
            .with_label_values(&[method, status])
            .observe(duration);
        self.requests_total
            .with_label_values(&[method, status])
            .inc();
    }

    /// Record an RPC error.
    pub fn record_error(&self, method: &str, error_type: &str) {
        self.errors_total
            .with_label_values(&[method, error_type])
            .inc();
    }

    /// Record an RPC retry.
    pub fn record_retry(&self, method: &str, reason: &str) {
        self.retries_total
            .with_label_values(&[method, reason])
            .inc();
    }

    /// Record an endpoint failover.
    pub fn record_failover(&self, from_endpoint: &str, reason: &str) {
        self.failovers_total
            .with_label_values(&[from_endpoint, reason])
            .inc();
    }

    /// Set the current endpoint index.
    pub fn set_current_endpoint(&self, index: usize) {
        self.current_endpoint.set(index as i64);
    }

    /// Set the total number of configured endpoints.
    pub fn set_endpoints_configured(&self, count: usize) {
        self.endpoints_configured.set(count as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_metrics::MetricsRegistry;

    #[test]
    fn test_metrics_creation() {
        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };

        let metrics = RpcMetrics::new(registry.prometheus_registry());

        metrics.record_request("getSlot", "success", 0.025);
        metrics.record_error("getAccountInfo", "timeout");
        metrics.record_retry("sendTransaction", "rate_limit");
        metrics.record_failover("https://api.mainnet-beta.solana.com", "timeout");
        metrics.set_current_endpoint(0);
        metrics.set_endpoints_configured(3);
    }
}
