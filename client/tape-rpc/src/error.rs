use solana_client::client_error::{ClientError, ClientErrorKind};
use solana_client::rpc_request::RpcError as SolanaRpcError;
use solana_sdk::pubkey::Pubkey;
use std::time::Duration;
use thiserror::Error;

/// Errors from RPC operations
#[derive(Debug, Error)]
pub enum RpcError {
    #[error("RPC request failed: {0}")]
    Request(#[from] ClientError),

    #[error("Request timeout after {0:?}")]
    Timeout(Duration),

    #[error("All endpoints exhausted after {attempts} attempts")]
    AllEndpointsFailed { attempts: u32 },

    #[error("Account not found: {0}")]
    AccountNotFound(Pubkey),

    #[error("Deserialization failed: {0}")]
    Deserialization(String),

    #[error("Transaction failed: {0}")]
    Transaction(String),

    #[error("Blockhash expired")]
    BlockhashExpired,

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
            RpcError::Request(e) => classify_client_error(e),

            // Non-retriable errors
            RpcError::AccountNotFound(_) => false,
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
            RpcError::Request(e) => is_endpoint_error(e),
            _ => false,
        }
    }

    /// Category for metrics
    pub fn category(&self) -> &'static str {
        match self {
            RpcError::Timeout(_) => "timeout",
            RpcError::Request(_) => "rpc_error",
            RpcError::AccountNotFound(_) => "not_found",
            RpcError::Deserialization(_) => "deser_error",
            RpcError::Transaction(_) => "tx_error",
            RpcError::BlockhashExpired => "blockhash_expired",
            RpcError::AllEndpointsFailed { .. } => "exhausted",
            RpcError::Internal(_) => "internal",
        }
    }
}

/// Classify Solana client errors for retry decisions
fn classify_client_error(e: &ClientError) -> bool {
    match e.kind() {
        // Network errors are retriable
        ClientErrorKind::Io(_) => true,
        ClientErrorKind::Reqwest(_) => true,

        // RPC errors need deeper inspection
        ClientErrorKind::RpcError(rpc_err) => is_retriable_rpc_error(rpc_err),

        // Serialization errors are not retriable
        ClientErrorKind::SerdeJson(_) => false,

        // Signing errors are not retriable
        ClientErrorKind::SigningError(_) => false,

        // Transaction errors are not retriable
        ClientErrorKind::TransactionError(_) => false,

        // Custom errors - default to not retriable
        ClientErrorKind::Custom(_) => false,

        // Fallback for any other error kind
        _ => false,
    }
}

/// Classify RPC errors for retry decisions
fn is_retriable_rpc_error(rpc_err: &SolanaRpcError) -> bool {
    use SolanaRpcError::*;

    match rpc_err {
        // Parse errors - check message
        RpcResponseError { message, .. } => {
            let msg = message.to_lowercase();

            // Retriable error patterns
            msg.contains("blockhash not found")
                || msg.contains("node is behind")
                || msg.contains("slot was skipped")
                || msg.contains("block not available")
                || msg.contains("timeout")
                || msg.contains("too many requests")
                || msg.contains("rate limit")
        }

        // Request errors (network/http issues)
        RpcRequestError(_) => true,

        // Fallback
        _ => false,
    }
}

/// Check if error suggests trying a different endpoint
fn is_endpoint_error(e: &ClientError) -> bool {
    match e.kind() {
        // Network errors suggest endpoint issues
        ClientErrorKind::Io(_) => true,
        ClientErrorKind::Reqwest(_) => true,

        ClientErrorKind::RpcError(rpc_err) => match rpc_err {
            SolanaRpcError::RpcResponseError { message, code, .. } => {
                let msg = message.to_lowercase();

                // HTTP error codes that suggest trying different endpoint
                if code == &429 || code == &503 || code == &504 {
                    return true;
                }

                // Error messages suggesting endpoint issues
                msg.contains("timeout")
                    || msg.contains("node is behind")
                    || msg.contains("too many requests")
                    || msg.contains("rate limit")
            }
            SolanaRpcError::RpcRequestError(_) => true,
            _ => false,
        },

        _ => false,
    }
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
}
