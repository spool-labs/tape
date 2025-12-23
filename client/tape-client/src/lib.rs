//! # tape-client
//!
//! High-level client library for interacting with Tape v2 Solana programs.
//!
//! This crate provides a convenient interface for:
//! - Fetching Tape account state (System, Epoch, Nodes, Tapes, etc.)
//! - Submitting transactions to Tape programs
//! - Automatic retry and failover via the underlying tape-rpc layer
//!
//! ## Features
//!
//! - **Type-safe account fetching** with automatic deserialization
//! - **PDA derivation** handled automatically
//! - **Transaction building** with blockhash management
//! - **Error handling** with retry and failover
//! - **Async/await** powered by tokio
//!
//! ## Example
//!
//! ```no_run
//! use tape_client::{TapeClient, RpcConfig};
//! use solana_sdk::signature::{Keypair, Signer};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure the client
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
pub use tape_rpc::{CommitmentLevel, Pubkey, RetryConfig, RpcConfig, RpcError, Signature};

// Re-export tape-api types for convenience
// Users can access account types and PDA functions
pub use tape_api;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::client::TapeClient;
    pub use tape_api::prelude::*;
    pub use tape_rpc::{CommitmentLevel, RpcConfig, RpcError};
}
