//! # rpc-solana
//!
//! Production Solana RPC client with retry and failover capabilities.
//!
//! This crate provides `SolanaRpc`, a production-ready implementation of the
//! `rpc::Rpc` trait with:
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
//! tape-rpc (trait)  →  rpc-solana (production) | test backends
//!                           ↓
//!                     tape-client<R: Rpc>
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use rpc::Rpc;
//! use rpc_solana::{SolanaRpc, RpcConfig, RpcRetryConfig};
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
mod selector;

#[cfg(feature = "metrics")]
pub mod metrics;

// Primary export
pub use client::{redact_url_query, SolanaRpc};

// Configuration exports
pub use config::{RpcRetryConfig, RpcConfig};
pub use selector::EndpointStrategy;
