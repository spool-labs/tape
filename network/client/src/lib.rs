//! Client library for communicating with tapedrive nodes.
//!
//! This crate provides `NodeClient` for making requests to tape nodes,
//! including slice operations and shard synchronization.

pub mod api;
pub mod builder;
pub mod client;
pub mod error;
pub mod retry;

#[cfg(feature = "metrics")]
pub mod metrics;

pub use builder::NodeClientBuilder;
pub use client::NodeClient;
pub use error::NodeError;
pub use retry::{RetryConfig, with_retry};

// Re-export SignResponse from tape-node-api for convenience
pub use tape_node_api::SignResponse;

#[cfg(feature = "metrics")]
pub use metrics::NodeClientMetrics;
