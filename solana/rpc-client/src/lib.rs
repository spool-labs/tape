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
//! use rpc_client::RpcClient;
//! use rpc_solana::RpcConfig;
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
pub mod compute;
mod snapshot;
mod transactions;

use rpc::RpcError;
use tape_api::errors::{ProgramError, TapeError};

/// Try to decode a typed `TapeError` from an RPC transaction error.
pub fn parse_tape_error(err: &RpcError) -> Option<TapeError> {
    let msg = match err {
        RpcError::Transaction(msg) => msg,
        RpcError::Request(msg) if looks_like_program_error(msg) => msg,
        _ => return None,
    };

    match ProgramError::from_error_string(msg) {
        Some(ProgramError::Tape(e)) => Some(e),
        _ => None,
    }
}

fn looks_like_program_error(msg: &str) -> bool {
    msg.contains("custom program error")
        || msg.contains("Error processing Instruction")
        || msg.contains("InstructionError")
}

#[cfg(feature = "metrics")]
pub mod metrics;

// Public exports
pub use client::RpcClient;

#[cfg(test)]
mod tests {
    use super::*;
    use rpc::RpcError;
    use tape_api::errors::TapeError;

    #[test]
    fn parse_hex() {
        let err = RpcError::Transaction("custom program error: 0x52".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyAdvanced));
    }

    #[test]
    fn parse_decimal() {
        let err = RpcError::Transaction("TransactionError::InstructionError(0, Custom(81))".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadySynced));
    }

    #[test]
    fn parse_already_certified() {
        let err = RpcError::Transaction("custom program error: 0x74".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyCertified));
    }

    #[test]
    fn parse_already_invalidated() {
        let err = RpcError::Transaction("custom program error: 0x73".to_string());
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyInvalidated));
    }

    #[test]
    fn skip_non_tx() {
        let err = RpcError::Request("boom".to_string());
        assert_eq!(parse_tape_error(&err), None);
    }

    #[test]
    fn parse_program_error_from_request() {
        let err = RpcError::Request(
            "RPC request failed: Error processing Instruction 1: custom program error: 0x12"
                .to_string(),
        );
        assert_eq!(parse_tape_error(&err), Some(TapeError::BadSignature));
    }

    #[test]
    fn skip_request_with_unrelated_hex() {
        let err = RpcError::Request("connection reset from 0x12".to_string());
        assert_eq!(parse_tape_error(&err), None);
    }
}
