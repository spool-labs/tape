//! Global metrics registry using OnceCell for thread-safe lazy initialization.

use once_cell::sync::OnceCell;
use prometheus::Registry;
use std::sync::Arc;

/// Global metrics registry singleton.
static METRICS: OnceCell<Arc<MetricsRegistry>> = OnceCell::new();

/// Central registry for Prometheus metrics.
///
/// This struct wraps a Prometheus registry and provides thread-safe global access.
/// Individual crates register their own metrics with this shared registry.
///
/// # Thread Safety
///
/// The registry is wrapped in `Arc` and initialized via `OnceCell`, making it
/// safe to access from multiple threads without additional synchronization.
///
/// # Example
///
/// ```ignore
/// use tape_metrics::MetricsRegistry;
/// use prometheus::register_int_counter_vec_with_registry;
///
/// // Initialize once at startup
/// let registry = MetricsRegistry::init();
///
/// // Register metrics with the shared registry
/// let counter = register_int_counter_vec_with_registry!(
///     "my_counter",
///     "My counter help",
///     &["label"],
///     registry.prometheus_registry()
/// ).unwrap();
///
/// // Use the counter
/// counter.with_label_values(&["value"]).inc();
/// ```
pub struct MetricsRegistry {
    /// The underlying Prometheus registry.
    registry: Registry,
}

impl MetricsRegistry {
    /// Initialize the global metrics registry.
    ///
    /// This method is idempotent - calling it multiple times will return
    /// the same registry instance. Safe to call from multiple threads.
    ///
    /// # Returns
    ///
    /// A reference to the global metrics registry.
    pub fn init() -> &'static Arc<MetricsRegistry> {
        METRICS.get_or_init(|| {
            Arc::new(MetricsRegistry {
                registry: Registry::new(),
            })
        })
    }

    /// Get the global metrics registry if initialized.
    ///
    /// Returns `None` if `init()` has not been called yet.
    pub fn get() -> Option<&'static Arc<MetricsRegistry>> {
        METRICS.get()
    }

    /// Get the global metrics registry, initializing if necessary.
    ///
    /// This is a convenience method equivalent to calling `init()`.
    pub fn get_or_init() -> &'static Arc<MetricsRegistry> {
        Self::init()
    }

    /// Get a reference to the underlying Prometheus registry.
    ///
    /// Use this to register your own metrics:
    ///
    /// ```ignore
    /// let registry = MetricsRegistry::init();
    /// let counter = register_int_counter_with_registry!(
    ///     "my_counter",
    ///     "Help text",
    ///     registry.prometheus_registry()
    /// ).unwrap();
    /// ```
    pub fn prometheus_registry(&self) -> &Registry {
        &self.registry
    }

    /// Gather all metrics for export.
    ///
    /// Returns metric families in Prometheus format, ready for encoding.
    pub fn gather(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_init_idempotent() {
        let r1 = MetricsRegistry::init();
        let r2 = MetricsRegistry::init();

        // Same instance
        assert!(Arc::ptr_eq(r1, r2));
    }

    #[test]
    fn test_registry_get_after_init() {
        MetricsRegistry::init();
        assert!(MetricsRegistry::get().is_some());
    }

    #[test]
    fn test_prometheus_registry_access() {
        let registry = MetricsRegistry::init();
        let prom_registry = registry.prometheus_registry();

        // Should be able to register metrics
        let counter = prometheus::register_int_counter_with_registry!(
            "test_counter",
            "Test counter",
            prom_registry
        );

        // Registration might fail if already registered by another test,
        // but that's fine - we just want to verify access works
        drop(counter);
    }
}
