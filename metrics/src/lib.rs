//! # tape-metrics
//!
//! Shared Prometheus metrics infrastructure for Tape crates.
//!
//! This crate provides the foundational metrics infrastructure that other
//! Tape crates can use to define their own domain-specific metrics.
//!
//! ## Features
//!
//! - `metrics` - Core metrics collection (prometheus + once_cell)
//! - `http` - HTTP endpoint for metrics exposition (adds axum)
//!
//! ## Usage
//!
//! Each crate defines its own metrics and registers them with the shared registry:
//!
//! ```ignore
//! use tape_metrics::{MetricsRegistry, OperationTimer};
//! use prometheus::{Registry, IntCounterVec, register_int_counter_vec_with_registry};
//!
//! // Define domain-specific metrics
//! pub struct MyMetrics {
//!     pub operations_total: IntCounterVec,
//! }
//!
//! impl MyMetrics {
//!     pub fn new(registry: &Registry) -> Self {
//!         let operations_total = register_int_counter_vec_with_registry!(
//!             "my_operations_total",
//!             "Total operations",
//!             &["operation", "status"],
//!             registry
//!         ).unwrap();
//!
//!         Self { operations_total }
//!     }
//! }
//!
//! // Use the shared registry
//! fn main() {
//!     let registry = MetricsRegistry::init();
//!     let my_metrics = MyMetrics::new(registry.prometheus_registry());
//!
//!     // Record metrics
//!     my_metrics.operations_total
//!         .with_label_values(&["fetch", "success"])
//!         .inc();
//! }
//! ```
//!
//! ## HTTP Endpoint
//!
//! ```ignore
//! use tape_metrics::metrics_router;
//!
//! // Add /metrics endpoint to your axum app
//! let app = Router::new()
//!     .merge(metrics_router());
//! ```

#[cfg(feature = "metrics")]
mod registry;
#[cfg(feature = "metrics")]
mod timer;

#[cfg(feature = "http")]
mod http;

// Re-exports
#[cfg(feature = "metrics")]
pub use registry::MetricsRegistry;
#[cfg(feature = "metrics")]
pub use timer::OperationTimer;

#[cfg(feature = "http")]
pub use http::{health_handler, metrics_handler, metrics_router, observability_router};

// Re-export prometheus types for downstream crates to define their own metrics
#[cfg(feature = "metrics")]
pub use prometheus::{
    self, register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry, Encoder, HistogramVec, IntCounterVec, IntGauge, Registry,
    TextEncoder,
};

/// Encode all metrics to Prometheus text format.
///
/// Returns None if metrics feature is disabled or registry not initialized.
#[cfg(feature = "metrics")]
pub fn encode_metrics() -> Option<String> {
    let registry = MetricsRegistry::get()?;
    let encoder = TextEncoder::new();
    let metric_families = registry.prometheus_registry().gather();

    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).ok()?;
    String::from_utf8(buffer).ok()
}

#[cfg(not(feature = "metrics"))]
pub fn encode_metrics() -> Option<String> {
    None
}
