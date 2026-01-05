//! Client library for communicating with tapedrive nodes.
//!
//! This crate provides `TapeNodeClient` for making requests to tape nodes,
//! including slice operations and shard synchronization.

pub mod api;
pub mod builder;
pub mod client;
pub mod error;

#[cfg(feature = "metrics")]
pub mod metrics;

pub use builder::TapeNodeClientBuilder;
pub use client::TapeNodeClient;
pub use error::NodeError;

#[cfg(feature = "metrics")]
pub use metrics::NodeClientMetrics;
