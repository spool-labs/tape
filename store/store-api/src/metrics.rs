//! Prometheus metrics for store operations
//!
//! This module provides comprehensive metrics tracking for Store operations including:
//! - Operation latencies (histograms)
//! - Throughput counters (operations/bytes)
//! - Size distributions (key/value/batch sizes)
//! - Error tracking
//!
//! Metrics are optional and enabled via the "metrics" feature flag.

use std::sync::Arc;
use tape_metrics::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    HistogramVec, IntCounterVec, MetricsRegistry, Registry,
};

// Re-export OperationTimer from tape-metrics for convenience
pub use tape_metrics::OperationTimer;

/// Latency buckets optimized for local storage operations
/// Covers range from 10µs to 10s to capture everything from memory to slow disk ops
const LATENCY_SEC_BUCKETS: &[f64] = &[
    0.00001, 0.00005, // 10µs, 50µs - memory operations
    0.0001, 0.0003, 0.0005, // 100µs, 300µs, 500µs - fast disk
    0.001, 0.003, 0.005, 0.01, // 1-10ms - typical disk ops
    0.05, 0.1, 0.5, 1.0, // 50ms-1s - slower ops
    5.0, 10.0, // 5s, 10s - batch operations
];

/// Size buckets for bytes (exponential from 1 byte to ~16MB)
fn size_buckets() -> Vec<f64> {
    tape_metrics::prometheus::exponential_buckets(1.0, 4.0, 12).unwrap()
}

/// Global store metrics instance
static STORE_METRICS: std::sync::OnceLock<Arc<StoreMetrics>> = std::sync::OnceLock::new();

/// Initialize the global store metrics
///
/// This should be called once at application startup if you want to use metrics
/// with the store implementations.
pub fn init_metrics() {
    STORE_METRICS.get_or_init(|| {
        let registry = MetricsRegistry::init();
        Arc::new(StoreMetrics::new(registry.prometheus_registry()))
    });
}

/// Get the global store metrics instance
///
/// Returns None if init_metrics() has not been called yet.
pub fn get_metrics() -> Option<&'static Arc<StoreMetrics>> {
    STORE_METRICS.get()
}

/// Core store operation metrics
///
/// Tracks latencies, throughput, sizes, and errors for all Store operations.
#[derive(Clone)]
pub struct StoreMetrics {
    // Latency histograms
    pub get_duration: HistogramVec,
    pub put_duration: HistogramVec,
    pub delete_duration: HistogramVec,
    pub contains_duration: HistogramVec,
    pub batch_duration: HistogramVec,
    pub iter_duration: HistogramVec,

    // Throughput counters
    pub operations_total: IntCounterVec,
    pub bytes_read_total: IntCounterVec,
    pub bytes_written_total: IntCounterVec,

    // Size histograms
    pub key_bytes: HistogramVec,
    pub value_bytes: HistogramVec,
    pub batch_items: HistogramVec,

    // Error tracking
    pub errors_total: IntCounterVec,
}

impl StoreMetrics {
    /// Create new StoreMetrics and register with the provided registry
    pub fn new(registry: &Registry) -> Self {
        StoreMetrics {
            get_duration: register_histogram_vec_with_registry!(
                "tape_store_get_duration_seconds",
                "Duration of get operations in seconds",
                &["cf_name", "found"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_get_duration_seconds"),

            put_duration: register_histogram_vec_with_registry!(
                "tape_store_put_duration_seconds",
                "Duration of put operations in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_put_duration_seconds"),

            delete_duration: register_histogram_vec_with_registry!(
                "tape_store_delete_duration_seconds",
                "Duration of delete operations in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_delete_duration_seconds"),

            contains_duration: register_histogram_vec_with_registry!(
                "tape_store_contains_duration_seconds",
                "Duration of contains operations in seconds",
                &["cf_name", "found"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_contains_duration_seconds"),

            batch_duration: register_histogram_vec_with_registry!(
                "tape_store_batch_duration_seconds",
                "Duration of write_batch operations in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_batch_duration_seconds"),

            iter_duration: register_histogram_vec_with_registry!(
                "tape_store_iter_duration_seconds",
                "Duration to create iterators in seconds",
                &["cf_name", "iter_type"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry
            )
            .expect("Failed to register tape_store_iter_duration_seconds"),

            operations_total: register_int_counter_vec_with_registry!(
                "tape_store_operations_total",
                "Total number of store operations",
                &["cf_name", "operation", "status"],
                registry
            )
            .expect("Failed to register tape_store_operations_total"),

            bytes_read_total: register_int_counter_vec_with_registry!(
                "tape_store_bytes_read_total",
                "Total bytes read from store",
                &["cf_name"],
                registry
            )
            .expect("Failed to register tape_store_bytes_read_total"),

            bytes_written_total: register_int_counter_vec_with_registry!(
                "tape_store_bytes_written_total",
                "Total bytes written to store",
                &["cf_name"],
                registry
            )
            .expect("Failed to register tape_store_bytes_written_total"),

            key_bytes: register_histogram_vec_with_registry!(
                "tape_store_key_bytes",
                "Size distribution of keys in bytes",
                &["cf_name", "operation"],
                size_buckets(),
                registry
            )
            .expect("Failed to register tape_store_key_bytes"),

            value_bytes: register_histogram_vec_with_registry!(
                "tape_store_value_bytes",
                "Size distribution of values in bytes",
                &["cf_name", "operation"],
                size_buckets(),
                registry
            )
            .expect("Failed to register tape_store_value_bytes"),

            batch_items: register_histogram_vec_with_registry!(
                "tape_store_batch_items",
                "Number of items in write batches",
                &["cf_name"],
                tape_metrics::prometheus::exponential_buckets(1.0, 2.0, 16).unwrap(),
                registry
            )
            .expect("Failed to register tape_store_batch_items"),

            errors_total: register_int_counter_vec_with_registry!(
                "tape_store_errors_total",
                "Total number of store errors",
                &["cf_name", "operation", "error_type"],
                registry
            )
            .expect("Failed to register tape_store_errors_total"),
        }
    }

    /// Create metrics using the global registry.
    pub fn new_with_global_registry() -> Arc<Self> {
        let registry = MetricsRegistry::init();
        Arc::new(Self::new(registry.prometheus_registry()))
    }

    /// Start timing an operation.
    #[inline]
    pub fn start_operation(&self) -> OperationTimer {
        OperationTimer::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_registry_init() {
        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = StoreMetrics::new(registry.prometheus_registry());

        // Observe a metric to verify the registry works
        metrics
            .operations_total
            .with_label_values(&["test_cf", "get", "success"])
            .inc();
        assert!(!registry.prometheus_registry().gather().is_empty());
    }

    #[test]
    fn test_operation_timer() {
        let timer = OperationTimer::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.elapsed_secs();
        assert!(elapsed >= 0.01); // At least 10ms
        assert!(elapsed < 1.0); // Less than 1 second
    }

    #[test]
    fn test_store_metrics_creation() {
        let registry = Registry::new();
        let metrics = StoreMetrics::new(&registry);

        // Observe at least one metric from each type to make them appear in gather()
        metrics.get_duration.with_label_values(&["test", "true"]).observe(0.001);
        metrics.put_duration.with_label_values(&["test"]).observe(0.001);
        metrics.delete_duration.with_label_values(&["test"]).observe(0.001);
        metrics.contains_duration.with_label_values(&["test", "true"]).observe(0.001);
        metrics.batch_duration.with_label_values(&["test"]).observe(0.001);
        metrics.iter_duration.with_label_values(&["test", "full"]).observe(0.001);
        metrics.operations_total.with_label_values(&["test", "get", "success"]).inc();
        metrics.bytes_read_total.with_label_values(&["test"]).inc();
        metrics.bytes_written_total.with_label_values(&["test"]).inc();
        metrics.key_bytes.with_label_values(&["test", "get"]).observe(10.0);
        metrics.value_bytes.with_label_values(&["test", "get"]).observe(100.0);
        metrics.batch_items.with_label_values(&["test"]).observe(5.0);
        metrics.errors_total.with_label_values(&["test", "get", "database"]).inc();

        let metric_families = registry.gather();
        let names: Vec<String> = metric_families
            .iter()
            .map(|mf| mf.get_name().to_string())
            .collect();

        // Verify all expected metrics are registered with tape_store_ prefix
        assert!(names.contains(&"tape_store_get_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_put_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_delete_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_contains_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_batch_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_iter_duration_seconds".to_string()));
        assert!(names.contains(&"tape_store_operations_total".to_string()));
        assert!(names.contains(&"tape_store_bytes_read_total".to_string()));
        assert!(names.contains(&"tape_store_bytes_written_total".to_string()));
        assert!(names.contains(&"tape_store_key_bytes".to_string()));
        assert!(names.contains(&"tape_store_value_bytes".to_string()));
        assert!(names.contains(&"tape_store_batch_items".to_string()));
        assert!(names.contains(&"tape_store_errors_total".to_string()));
    }
}
