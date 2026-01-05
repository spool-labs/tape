//! Client library for communicating with tapedrive nodes.
//!
//! This crate provides `NodeClient` for making requests to tape nodes,
//! including slice operations and shard synchronization.

pub mod api;
pub mod builder;
pub mod client;
pub mod error;

#[cfg(feature = "metrics")]
pub mod metrics;

pub use builder::NodeClientBuilder;
pub use client::NodeClient;
pub use error::NodeError;

#[cfg(feature = "metrics")]
pub use metrics::NodeClientMetrics;
