//! # tape-rpc
//!
//! A robust Solana RPC client with automatic retry, exponential backoff, and endpoint failover.
//!
//! ## Features
//!
//! - **Automatic retry** with configurable exponential backoff
//! - **Endpoint failover** for high availability
//! - **Error classification** for smart retry decisions
//! - **Type-safe configuration** with serde support
//! - **Async/await** powered by tokio
//!
//! ## Example
//!
//! ```no_run
//! use tape_rpc::{RpcConfig, TapeRpcClient};
//! use solana_sdk::pubkey::Pubkey;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = RpcConfig {
//!         endpoints: vec![
//!             "https://api.mainnet-beta.solana.com".to_string(),
//!             "https://backup-rpc.com".to_string(),
//!         ],
//!         ..Default::default()
//!     };
//!
//!     let client = TapeRpcClient::new(config)?;
//!     let slot = client.get_slot().await?;
//!     println!("Current slot: {}", slot);
//!
//!     Ok(())
//! }
//! ```

mod client;
mod config;
mod error;
mod failover;
mod retry;

#[cfg(feature = "metrics")]
pub mod metrics;

// Public exports
pub use client::TapeRpcClient;
pub use config::{RetryConfig, RpcConfig};
pub use error::RpcError;
pub use failover::EndpointFailover;
pub use retry::ExponentialBackoff;

// Re-export commonly used types from solana-sdk for convenience
pub use solana_sdk::commitment_config::CommitmentLevel;
pub use solana_sdk::pubkey::Pubkey;
pub use solana_sdk::signature::Signature;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::TapeRpcClient;
    pub use crate::config::{RetryConfig, RpcConfig};
    pub use crate::error::RpcError;
    pub use solana_sdk::commitment_config::CommitmentLevel;
}
