//! # rpc-client
//!
//! RPC client library for querying Tapedrive on-chain program state.
//!
//! This crate provides a convenient interface for:
//! - Fetching Tapedrive account state (System, Epoch, Nodes, Tapes, etc.)
//! - Submitting transactions to Tapedrive programs
//! - Automatic retry and failover via the underlying rpc-solana layer
//!
//! ## Generic RPC Pattern
//!
//! `RpcClient<R: Rpc>` is generic over the RPC implementation, enabling:
//!
//! - **Production**: `RpcClient<SolanaRpc>` with retry/failover
//! - **Testing/Simulation**: `RpcClient<R>` with a custom `Rpc` backend
//!
//! ## Example
//!
//! ```no_run
//! use rpc_client::{RpcClient, RpcConfig};
//! use solana_sdk::signature::{Keypair, Signer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure the client (uses SolanaRpc internally)
//!     let config = RpcConfig {
//!         endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
//!         ..Default::default()
//!     };
//!
//!     let client = RpcClient::new(config)?;
//!
//!     // Fetch singleton accounts
//!     let system = client.get_system().await?;
//!     let epoch = client.get_epoch().await?;
//!
//!     // Fetch parameterized accounts
//!     let authority = Keypair::new().pubkey();
//!     let node = client.get_node(&authority).await?;
//!
//!     println!("Node: {:?}", node);
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Testing with a Custom Backend
//!
//! ```ignore
//! use rpc_client::RpcClient;
//! use rpc_litesvm::LiteSvmRpc;
//!
//! #[tokio::test]
//! async fn test_with_custom_rpc() {
//!     let rpc = LiteSvmRpc::new();
//!     let client = RpcClient::from_rpc(rpc);
//!
//!     // Same API as production!
//!     let slot = client.get_slot().await.unwrap();
//! }
//! ```
//!
//! ## Submitting Transactions
//!
//! ```no_run
//! use rpc_client::RpcClient;
//! use solana_sdk::signature::{Keypair, Signer};
//! # use tape_api::instruction::*; // Assuming tape-api provides instruction builders
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RpcClient::new(Default::default())?;
//! let payer = Keypair::new();
//!
//! // Build instruction using tape-api (example)
//! # let ix = solana_sdk::system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 0);
//! // let ix = build_register_node_ix(&payer.pubkey(), "my-node");
//!
//! // Submit with automatic retry and confirmation
//! let signature = client.send_instructions(&payer, vec![ix]).await?;
//! println!("Transaction confirmed: {}", signature);
//! # Ok(())
//! # }
//! ```

mod accounts;
mod client;
mod transactions;

#[cfg(feature = "metrics")]
pub mod metrics;

// Public exports
pub use client::RpcClient;

// Re-export tape-rpc trait types
pub use rpc::{CommitmentLevel, Pubkey, Rpc, RpcError, Signature};

// Re-export rpc-solana production types
pub use rpc_solana::{RetryConfig, RpcConfig, SolanaRpc};

// Re-export tape-api types for convenience
// Users can access account types and PDA functions
pub use tape_api;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::RpcClient;
    pub use rpc_solana::{RetryConfig, RpcConfig, SolanaRpc};
    pub use tape_api::prelude::*;
    pub use rpc::{CommitmentLevel, Rpc, RpcError};
}
