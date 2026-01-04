//! # tape-client
//!
//! High-level client library for interacting with Tape v2 Solana programs.
//!
//! This crate provides a convenient interface for:
//! - Fetching Tape account state (System, Epoch, Nodes, Tapes, etc.)
//! - Submitting transactions to Tape programs
//! - Automatic retry and failover via the underlying tape-rpc layer
//!
//! ## Generic RPC Pattern
//!
//! `TapeClient<R: Rpc>` is generic over the RPC implementation, following the same
//! pattern as `TapeStore<S: Store>` in the archive crates. This enables:
//!
//! - **Production**: `TapeClient<TapeRpcClient>` with retry/failover
//! - **Testing**: `TapeClient<TestRpc>` with local test validator
//!
//! ## Example
//!
//! ```no_run
//! use tape_client::{TapeClient, RpcConfig};
//! use solana_sdk::signature::{Keypair, Signer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure the client (uses TapeRpcClient internally)
//!     let config = RpcConfig {
//!         endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
//!         ..Default::default()
//!     };
//!
//!     let client = TapeClient::new(config)?;
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
//! ## Testing with TestRpc
//!
//! ```ignore
//! use tape_client::TapeClient;
//! use rpc_test::TestRpc;
//! use solana_test_validator::TestValidatorGenesis;
//!
//! #[tokio::test]
//! async fn test_with_validator() {
//!     let (validator, payer) = TestValidatorGenesis::default()
//!         .start_async()
//!         .await;
//!
//!     // Create client with test RPC
//!     let client = TapeClient::from_rpc(TestRpc::new(&validator));
//!
//!     // Same API as production!
//!     let slot = client.get_slot().await.unwrap();
//! }
//! ```
//!
//! ## Submitting Transactions
//!
//! ```no_run
//! use tape_client::TapeClient;
//! use solana_sdk::signature::{Keypair, Signer};
//! # use tape_api::instruction::*; // Assuming tape-api provides instruction builders
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = TapeClient::new(Default::default())?;
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
pub use client::TapeClient;

// Re-export tape-rpc types for convenience
pub use tape_rpc::{CommitmentLevel, Pubkey, RetryConfig, Rpc, RpcConfig, RpcError, Signature, TapeRpcClient};

// Re-export tape-api types for convenience
// Users can access account types and PDA functions
pub use tape_api;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::TapeClient;
    pub use tape_api::prelude::*;
    pub use tape_rpc::{CommitmentLevel, Rpc, RpcConfig, RpcError, TapeRpcClient};
}
