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
