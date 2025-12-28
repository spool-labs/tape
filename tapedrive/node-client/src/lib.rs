//! Client library for communicating with tapedrive storage nodes.
//!
//! This crate provides `StorageNodeClient` for making requests to storage nodes,
//! including slice operations and shard synchronization.

pub mod api;
pub mod builder;
pub mod client;
pub mod error;

#[cfg(feature = "metrics")]
pub mod metrics;

pub use builder::StorageNodeClientBuilder;
pub use client::StorageNodeClient;
pub use error::NodeError;

#[cfg(feature = "metrics")]
pub use metrics::NodeClientMetrics;
