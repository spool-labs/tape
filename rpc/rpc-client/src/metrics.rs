//! Prometheus metrics for rpc-client operations.
//!
//! This module provides metrics tracking for client operations including:
//! - Operation duration and success/failure rates
//! - Account fetching statistics
//! - Transaction submission and confirmation metrics
//!
//! All metrics are feature-gated behind the `metrics` feature flag and have
//! zero overhead when disabled.

use std::sync::Arc;
use tape_metrics::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry, HistogramVec,
    IntCounterVec, MetricsRegistry, OperationTimer, Registry,
};

/// Latency buckets optimized for RPC operations.
///
/// Covers ranges from 10ms to 120s to capture various scenarios:
/// - Fast cached responses: 10-50ms
/// - Normal RPC calls: 50-500ms
/// - Slow operations: 500ms-5s
/// - Timeouts and retries: 5s-120s
const LATENCY_BUCKETS: &[f64] = &[
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Metrics collector for RpcClient operations.
///
/// This struct holds all Prometheus metrics for the rpc-client crate.
/// It is designed to be shared across threads via `Arc`.
///
/// # Example
///
/// ```ignore
/// use rpc_client::metrics::ClientMetrics;
/// use tape_metrics::MetricsRegistry;
///
/// let registry = MetricsRegistry::init();
/// let metrics = ClientMetrics::new(registry.prometheus_registry());
///
/// // Record a successful operation
/// let timer = metrics.start_operation();
/// // ... perform operation ...
/// metrics.record_operation("get_system", "success", timer);
/// ```
pub struct ClientMetrics {
    /// Histogram tracking operation duration in seconds.
    ///
    /// Labels: operation (e.g., "get_system", "send_tx"), status (success/error)
    pub operation_duration: HistogramVec,

    /// Counter tracking total operations.
    ///
    /// Labels: operation, status
    pub operations_total: IntCounterVec,

    /// Counter tracking account fetches.
    ///
    /// Labels: account_type (e.g., "system", "node", "tape"), status
    pub accounts_fetched_total: IntCounterVec,

    /// Counter tracking transaction submissions.
    ///
    /// Labels: status (success/error)
    pub transactions_total: IntCounterVec,

    /// Histogram tracking transaction confirmation duration.
    ///
    /// Labels: status (confirmed/timeout/error)
    pub transaction_confirmation_duration: HistogramVec,
}

impl ClientMetrics {
    /// Create a new ClientMetrics instance and register all metrics.
    ///
    /// # Arguments
    /// * `registry` - The Prometheus registry to register metrics with
    ///
    /// # Panics
    /// Panics if metrics cannot be registered (e.g., duplicate registration)
    pub fn new(registry: &Registry) -> Self {
        let operation_duration = register_histogram_vec_with_registry!(
            "tape_client_operation_duration_seconds",
            "Duration of tape-client operations in seconds",
            &["operation", "status"],
            LATENCY_BUCKETS.to_vec(),
            registry
        )
        .expect("Failed to register tape_client_operation_duration_seconds");

        let operations_total = register_int_counter_vec_with_registry!(
            "tape_client_operations_total",
            "Total number of tape-client operations",
            &["operation", "status"],
            registry
        )
        .expect("Failed to register tape_client_operations_total");

        let accounts_fetched_total = register_int_counter_vec_with_registry!(
            "tape_client_accounts_fetched_total",
            "Total number of accounts fetched by type",
            &["account_type", "status"],
            registry
        )
        .expect("Failed to register tape_client_accounts_fetched_total");

        let transactions_total = register_int_counter_vec_with_registry!(
            "tape_client_transactions_total",
            "Total number of transactions submitted",
            &["status"],
            registry
        )
        .expect("Failed to register tape_client_transactions_total");

        let transaction_confirmation_duration = register_histogram_vec_with_registry!(
            "tape_client_transaction_confirmation_duration_seconds",
            "Duration of transaction confirmation in seconds",
            &["status"],
            LATENCY_BUCKETS.to_vec(),
            registry
        )
        .expect("Failed to register tape_client_transaction_confirmation_duration_seconds");

        Self {
            operation_duration,
            operations_total,
            accounts_fetched_total,
            transactions_total,
            transaction_confirmation_duration,
        }
    }

    /// Create a new ClientMetrics instance using the global registry.
    ///
    /// This is a convenience method that initializes the global metrics
    /// registry if needed and creates metrics with it.
    pub fn new_with_global_registry() -> Arc<Self> {
        let registry = MetricsRegistry::init();
        Arc::new(Self::new(registry.prometheus_registry()))
    }

    /// Start timing an operation.
    ///
    /// Returns a timer that can be passed to `record_operation()`.
    #[inline]
    pub fn start_operation(&self) -> OperationTimer {
        OperationTimer::new()
    }

    /// Record a completed operation with its duration and status.
    ///
    /// # Arguments
    /// * `operation` - Operation name (e.g., "get_system", "send_tx")
    /// * `status` - Operation status ("success" or "error")
    /// * `timer` - Timer started at the beginning of the operation
    pub fn record_operation(&self, operation: &str, status: &str, timer: &OperationTimer) {
        let duration = timer.elapsed_secs();
        self.operation_duration
            .with_label_values(&[operation, status])
            .observe(duration);
        self.operations_total
            .with_label_values(&[operation, status])
            .inc();
    }

    /// Record an account fetch operation.
    ///
    /// # Arguments
    /// * `account_type` - Type of account fetched (e.g., "system", "node", "tape")
    /// * `status` - Fetch status ("success" or "error")
    /// * `timer` - Timer started at the beginning of the fetch
    pub fn record_account_fetch(
        &self,
        account_type: &str,
        status: &str,
        timer: &OperationTimer,
    ) {
        let duration = timer.elapsed_secs();
        self.accounts_fetched_total
            .with_label_values(&[account_type, status])
            .inc();
        let operation_name = format!("get_{}", account_type);
        self.operation_duration
            .with_label_values(&[operation_name.as_str(), status])
            .observe(duration);
    }

    /// Record a successful transaction submission.
    pub fn record_transaction_success(&self) {
        self.transactions_total
            .with_label_values(&["success"])
            .inc();
    }

    /// Record a failed transaction submission.
    pub fn record_transaction_error(&self) {
        self.transactions_total.with_label_values(&["error"]).inc();
    }

    /// Record transaction confirmation duration.
    ///
    /// # Arguments
    /// * `status` - Confirmation status ("confirmed", "timeout", or "error")
    /// * `timer` - Timer started when transaction was submitted
    pub fn record_transaction_confirmation(&self, status: &str, timer: &OperationTimer) {
        let duration = timer.elapsed_secs();
        self.transaction_confirmation_duration
            .with_label_values(&[status])
            .observe(duration);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let registry = Registry::new();
        let metrics = ClientMetrics::new(&registry);

        // Verify metrics are registered by checking they can be used
        let timer = metrics.start_operation();
        metrics.record_operation("test", "success", &timer);

        // Verify the registry has collected metrics
        let metric_families = registry.gather();
        assert!(!metric_families.is_empty());
    }

    #[test]
    fn test_record_operations() {
        let registry = Registry::new();
        let metrics = ClientMetrics::new(&registry);

        // Record various operations
        let timer = metrics.start_operation();
        metrics.record_operation("get_system", "success", &timer);

        let timer = metrics.start_operation();
        metrics.record_account_fetch("node", "success", &timer);

        metrics.record_transaction_success();
        metrics.record_transaction_error();

        let timer = metrics.start_operation();
        metrics.record_transaction_confirmation("confirmed", &timer);

        // Verify metrics were recorded
        let metric_families = registry.gather();
        assert!(!metric_families.is_empty());
    }

    #[test]
    fn test_timer() {
        let timer = OperationTimer::new();
        let elapsed = timer.elapsed_secs();

        // Should be a very small but positive value
        assert!(elapsed >= 0.0);
        assert!(elapsed < 1.0);
    }
}
