//! Metrics infrastructure for Tapedrive.

#[cfg(feature = "metrics")]
pub use prometheus;

#[cfg(feature = "metrics")]
pub use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry, HistogramVec,
    IntCounterVec, IntGauge, Registry,
};

#[cfg(feature = "metrics")]
mod registry;

#[cfg(feature = "metrics")]
pub use registry::{MetricsRegistry, OperationTimer};

#[cfg(feature = "metrics")]
mod set;

#[cfg(feature = "metrics")]
pub use set::{init_app_metrics, metrics, Metrics};

/// Encode the global registry in Prometheus text exposition format.
#[cfg(feature = "metrics")]
pub fn render() -> Vec<u8> {
    use prometheus::{Encoder, TextEncoder};
    let mut buffer = Vec::new();
    let _ = TextEncoder::new().encode(&prometheus::gather(), &mut buffer);
    buffer
}

/// Register process-level resource metrics; procfs-backed, so Linux-only.
#[cfg(all(feature = "metrics", target_os = "linux"))]
pub fn register_process_collector(registry: &Registry) {
    let collector = prometheus::process_collector::ProcessCollector::for_self();
    let _ = registry.register(Box::new(collector));
}

#[cfg(all(feature = "metrics", not(target_os = "linux")))]
pub fn register_process_collector(_registry: &Registry) {}
