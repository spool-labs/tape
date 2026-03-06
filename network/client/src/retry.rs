//! Retry helpers for node client operations.
//!
//! Thin wrappers around `tape_retry` that wire up `NodeError::is_retryable()`.

use std::future::Future;

use tape_retry::Retryable;

use crate::error::NodeError;

pub use tape_retry::RetryConfig;

impl Retryable for NodeError {
    fn is_retryable(&self) -> bool {
        NodeError::is_retryable(self)
    }
}

/// Execute an async operation with retry on transient failures.
///
/// Only retries errors where [`NodeError::is_retryable`] returns true.
pub async fn with_retry<T, F, Fut>(config: &RetryConfig, operation: F) -> Result<T, NodeError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, NodeError>>,
{
    tape_retry::retry_if(config.clone(), None, operation, Retryable::is_retryable).await
}

/// Execute an async operation with unconditional retry and exponential backoff.
///
/// Retries on ALL errors regardless of type.
pub async fn with_retry_all<T, E, F, Fut>(config: &RetryConfig, operation: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    tape_retry::retry(config.clone(), None, operation).await
}
