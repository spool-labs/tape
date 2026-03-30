//! RPC error types
//!
//! This module provides error types for RPC operations that are
//! implementation-agnostic. Specific implementations (like `rpc-solana`)
//! convert their internal errors into these types.

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::time::Duration;
use thiserror::Error;

/// Errors from RPC operations
///
/// These error types are designed to be implementation-agnostic so they
/// can be used by any RPC implementation (Solana, mock, test, etc.).
#[derive(Debug, Error)]
pub enum RpcError {
    /// RPC request failed with an error message
    #[error("RPC request failed: {0}")]
    Request(String),

    /// Request timed out
    #[error("Request timeout after {0:?}")]
    Timeout(Duration),

    /// All configured endpoints have been exhausted
    #[error("All endpoints exhausted after {attempts} attempts")]
    AllEndpointsFailed { attempts: u32 },

    /// Account does not exist at the given address
    #[error("Account not found: {0}")]
    AccountNotFound(Pubkey),

    /// Transaction does not exist for the given signature
    #[error("Transaction not found: {0}")]
    TransactionNotFound(Signature),

    /// Failed to deserialize account data
    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    /// Transaction execution failed
    #[error("Transaction failed: {0}")]
    Transaction(String),

    /// Blockhash has expired (transaction too old)
    #[error("Blockhash expired")]
    BlockhashExpired,

    /// Internal error (configuration, setup, etc.)
    #[error("Internal error: {0}")]
    Internal(String),
}

impl RpcError {
    /// Determines if this error should be retried
    pub fn is_retriable(&self) -> bool {
        match self {
            // Retriable errors
            RpcError::Timeout(_) => true,
            RpcError::BlockhashExpired => true,
            RpcError::Request(msg) => is_retriable_message(msg),

            // Non-retriable errors
            RpcError::AccountNotFound(_) => false,
            RpcError::TransactionNotFound(_) => false,
            RpcError::Deserialization(_) => false,
            RpcError::Transaction(_) => false,
            RpcError::AllEndpointsFailed { .. } => false,
            RpcError::Internal(_) => false,
        }
    }

    /// Should we try a different endpoint?
    pub fn should_failover(&self) -> bool {
        match self {
            RpcError::Timeout(_) => true,
            RpcError::Request(msg) => is_endpoint_error_message(msg),
            _ => false,
        }
    }

    /// Category for metrics
    pub fn category(&self) -> &'static str {
        match self {
            RpcError::Timeout(_) => "timeout",
            RpcError::Request(_) => "rpc_error",
            RpcError::AccountNotFound(_) => "not_found",
            RpcError::TransactionNotFound(_) => "not_found",
            RpcError::Deserialization(_) => "deser_error",
            RpcError::Transaction(_) => "tx_error",
            RpcError::BlockhashExpired => "blockhash_expired",
            RpcError::AllEndpointsFailed { .. } => "exhausted",
            RpcError::Internal(_) => "internal",
        }
    }

    /// Check if this error indicates a skipped slot.
    pub fn is_skipped_slot(&self) -> bool {
        match self {
            RpcError::Request(msg) => is_skipped_slot_message(msg),
            _ => false,
        }
    }
}

/// Check if error message indicates a retriable condition
fn is_retriable_message(msg: &str) -> bool {
    let msg = msg.to_lowercase();
    msg.contains("blockhash not found")
        || msg.contains("node is behind")
        || msg.contains("slot was skipped")
        || msg.contains("block not available")
        || msg.contains("timeout")
        || msg.contains("too many requests")
        || msg.contains("rate limit")
        || msg.contains("connection")
        || msg.contains("network")
}

/// Check if error message suggests trying a different endpoint
fn is_endpoint_error_message(msg: &str) -> bool {
    let msg = msg.to_lowercase();
    msg.contains("timeout")
        || msg.contains("node is behind")
        || msg.contains("too many requests")
        || msg.contains("rate limit")
        || msg.contains("connection")
        || msg.contains("503")
        || msg.contains("504")
        || msg.contains("429")
}

fn is_skipped_slot_message(msg: &str) -> bool {
    let msg = msg.to_lowercase();
    msg.contains("slotskipped")
        || msg.contains("slot was skipped")
        || msg.contains("skipped")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_categories() {
        assert_eq!(RpcError::Timeout(Duration::from_secs(1)).category(), "timeout");
        assert_eq!(
            RpcError::AccountNotFound(Pubkey::default()).category(),
            "not_found"
        );
        assert_eq!(
            RpcError::Deserialization("test".to_string()).category(),
            "deser_error"
        );
    }

    #[test]
    fn test_retriable_classification() {
        assert!(RpcError::Timeout(Duration::from_secs(1)).is_retriable());
        assert!(RpcError::BlockhashExpired.is_retriable());
        assert!(!RpcError::AccountNotFound(Pubkey::default()).is_retriable());
        assert!(!RpcError::Deserialization("test".to_string()).is_retriable());
    }

    #[test]
    fn test_failover_classification() {
        assert!(RpcError::Timeout(Duration::from_secs(1)).should_failover());
        assert!(!RpcError::BlockhashExpired.should_failover());
        assert!(!RpcError::AccountNotFound(Pubkey::default()).should_failover());
    }

    #[test]
    fn test_retriable_messages() {
        assert!(RpcError::Request("connection reset".to_string()).is_retriable());
        assert!(RpcError::Request("rate limit exceeded".to_string()).is_retriable());
        assert!(!RpcError::Request("invalid account".to_string()).is_retriable());
    }

    #[test]
    fn test_skipped_slot() {
        assert!(RpcError::Request("SlotSkipped: slot 10 was skipped or not produced".to_string()).is_skipped_slot());
        assert!(RpcError::Request("slot was skipped".to_string()).is_skipped_slot());
        assert!(!RpcError::Request("connection reset".to_string()).is_skipped_slot());
        assert!(!RpcError::Timeout(Duration::from_secs(1)).is_skipped_slot());
    }
}
