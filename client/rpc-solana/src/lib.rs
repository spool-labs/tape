//! # rpc-solana
//!
//! Production Solana RPC client with retry and failover capabilities.
//!
//! This crate provides `SolanaRpc`, a production-ready implementation of the
//! `tape_rpc::Rpc` trait with:
//!
//! - Automatic retry with exponential backoff
//! - Endpoint failover across multiple RPC endpoints
//! - Configurable timeouts and retry policies
//! - Optional Prometheus metrics
//!
//! ## Pattern
//!
//! This follows the same pattern as `store-rocks/` in `tapedrive/archive/`:
//! ```text
//! tape-rpc (trait)  →  rpc-solana (production) | rpc-test (testing)
//!                           ↓
//!                     tape-client<R: Rpc>
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use rpc_solana::{SolanaRpc, RpcConfig, RetryConfig};
//! use tape_rpc::Rpc;
//!
//! // Create with default settings
//! let rpc = SolanaRpc::new(RpcConfig::default())?;
//!
//! // Or with custom configuration
//! let config = RpcConfig {
//!     endpoints: vec![
//!         "https://api.mainnet-beta.solana.com".to_string(),
//!         "https://solana-api.projectserum.com".to_string(),
//!     ],
//!     commitment: CommitmentLevel::Finalized,
//!     retry: RetryConfig {
//!         max_retries: 3,
//!         ..Default::default()
//!     },
//!     ..Default::default()
//! };
//! let rpc = SolanaRpc::new(config)?;
//!
//! // Use via the Rpc trait
//! let slot = rpc.get_slot().await?;
//! ```

mod client;
mod config;
mod failover;
mod retry;

#[cfg(feature = "metrics")]
pub mod metrics;

// Primary export
pub use client::SolanaRpc;

// Configuration exports
pub use config::{RetryConfig, RpcConfig};

// Internal exports (useful for advanced users)
pub use failover::EndpointFailover;
pub use retry::ExponentialBackoff;

// Re-export tape-rpc types for convenience
pub use tape_rpc::{CommitmentLevel, Rpc, RpcError};

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::SolanaRpc;
    pub use crate::config::{RetryConfig, RpcConfig};
    pub use tape_rpc::{CommitmentLevel, Rpc, RpcError};
}
