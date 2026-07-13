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
//! ```ignore
//! use rpc_client::RpcClient;
//! use rpc_solana::RpcConfig;
//! use solana_keypair::Keypair;
//! use solana_signer::Signer;
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
//! ```ignore
//! use rpc_client::RpcClient;
//! use solana_keypair::Keypair;
//! use solana_signer::Signer;
//! use solana_system_interface::instruction as system_instruction;
//! # use tape_api::instruction::*; // Assuming tape-api provides instruction builders
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RpcClient::new(Default::default())?;
//! let payer = Keypair::new();
//!
//! // Build instruction using tape-api (example)
//! # let ix = system_instruction::transfer(&payer.pubkey(), &payer.pubkey(), 0);
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
mod snapshot;
mod transactions;

use rpc::{RpcError, looks_like_transaction_error};
use tape_api::errors::{ProgramError, TapeError};

/// Try to decode a typed `TapeError` from an RPC transaction error.
///
/// Reads the structured error when the RPC reported one, and falls back to
/// message parsing for proxies that flatten errors into text.
pub fn parse_tape_error(err: &RpcError) -> Option<TapeError> {
    if let Some(code) = err.custom_program_error() {
        return TapeError::from_code(code);
    }

    let msg = match err {
        RpcError::Transaction { err: None, message } => message,
        RpcError::Request(msg) if looks_like_transaction_error(msg) => msg,
        _ => return None,
    };

    match ProgramError::from_error_string(msg) {
        Some(ProgramError::Tape(e)) => Some(e),
        _ => None,
    }
}

#[cfg(feature = "metrics")]
pub mod metrics;

// Public exports
pub use client::RpcClient;

#[cfg(test)]
mod tests {
    use super::*;
    use rpc::{InstructionError, RpcError, TransactionError};
    use tape_api::errors::TapeError;

    fn program_error(code: u32) -> RpcError {
        RpcError::Transaction {
            err: Some(TransactionError::InstructionError(
                0,
                InstructionError::Custom(code),
            )),
            message: format!("custom program error: {code:#x}"),
        }
    }

    #[test]
    fn parse_structured() {
        assert_eq!(
            parse_tape_error(&program_error(0x52)),
            Some(TapeError::AlreadyAdvanced)
        );
        assert_eq!(
            parse_tape_error(&program_error(0x74)),
            Some(TapeError::AlreadyCertified)
        );
        assert_eq!(parse_tape_error(&program_error(0x999)), None);
    }

    #[test]
    fn parse_flattened_hex() {
        let err = RpcError::Transaction {
            err: None,
            message: "custom program error: 0x52".to_string(),
        };
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadyAdvanced));
    }

    #[test]
    fn parse_flattened_decimal() {
        let err = RpcError::Transaction {
            err: None,
            message: "TransactionError::InstructionError(0, Custom(81))".to_string(),
        };
        assert_eq!(parse_tape_error(&err), Some(TapeError::AlreadySynced));
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
