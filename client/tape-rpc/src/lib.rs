//! # tape-rpc
//!
//! Core RPC trait and error types for Solana RPC operations.
//!
//! This crate defines the `Rpc` trait which abstracts over different RPC implementations:
//! - `rpc-solana` - Production client with retry/failover
//! - `rpc-test` - Test validator client for integration tests
//!
//! ## Pattern
//!
//! This follows the same pattern as `store/` in `tapedrive/archive/`:
//! ```text
//! tape-rpc (trait)  →  rpc-solana | rpc-test
//!                           ↓
//!                     tape-client<R: Rpc>
//! ```
//!
//! ## Example
//!
//! ```ignore
//! use tape_rpc::{Rpc, RpcError};
//!
//! async fn fetch_slot<R: Rpc>(rpc: &R) -> Result<u64, RpcError> {
//!     rpc.get_slot().await
//! }
//! ```

mod client;
mod config;
mod error;
mod failover;
mod retry;
mod rpc;

#[cfg(feature = "metrics")]
pub mod metrics;

// Core trait export
pub use rpc::Rpc;

// Public exports (for backwards compatibility + rpc-solana will use these)
pub use client::TapeRpcClient;
pub use config::{RetryConfig, RpcConfig};
pub use error::RpcError;
pub use failover::EndpointFailover;
pub use retry::ExponentialBackoff;

// Re-export commonly used types from solana-sdk for convenience
pub use solana_sdk::commitment_config::CommitmentLevel;
pub use solana_sdk::pubkey::Pubkey;
pub use solana_sdk::signature::Signature;

// Re-export async_trait for implementors
pub use async_trait::async_trait;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::rpc::Rpc;
    pub use crate::client::TapeRpcClient;
    pub use crate::config::{RetryConfig, RpcConfig};
    pub use crate::error::RpcError;
    pub use solana_sdk::commitment_config::CommitmentLevel;
}
