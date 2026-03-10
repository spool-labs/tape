//! Global metrics registry wrapper.

use prometheus::Registry;
use std::sync::OnceLock;
use std::time::Instant;

static GLOBAL_REGISTRY: OnceLock<MetricsRegistry> = OnceLock::new();

/// Wraps the prometheus `Registry` with initialization tracking.
#[derive(Clone)]
pub struct MetricsRegistry {
    registry: Registry,
}

impl MetricsRegistry {
    /// Initialize the global metrics registry (uses the prometheus default registry).
    ///
    /// Returns the registry. Subsequent calls return the same instance.
    pub fn init() -> &'static Self {
        GLOBAL_REGISTRY.get_or_init(|| Self {
            registry: prometheus::default_registry().clone(),
        })
    }

    /// Get the global registry if already initialized.
    pub fn get() -> Option<&'static Self> {
        GLOBAL_REGISTRY.get()
    }

    /// Access the underlying prometheus registry.
    pub fn prometheus_registry(&self) -> &Registry {
        &self.registry
    }
}

/// Lightweight operation timer for measuring elapsed time.
pub struct OperationTimer {
    start: Instant,
}

impl OperationTimer {
    /// Start a new timer.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time in seconds.
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

impl Default for OperationTimer {
    fn default() -> Self {
        Self::new()
    }
}
