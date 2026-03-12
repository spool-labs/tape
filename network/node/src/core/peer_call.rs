use std::future::Future;

use peer_manager::PeerManager;
use tape_core::types::NodeId;
use tape_protocol::api::ApiError;
use tape_retry::{RetryConfig, Retryable, retry_if};
use tokio_util::sync::CancellationToken;

pub async fn call_peer<T, F, Fut>(
    peer_manager: &PeerManager,
    node_id: NodeId,
    cancel: Option<&CancellationToken>,
    f: F,
) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let result = retry_if(
        RetryConfig::three(),
        cancel,
        f,
        ApiError::is_retryable,
    ).await;

    match &result {
        Ok(_) => peer_manager.report_success(node_id),
        Err(err) if err.is_retryable() => peer_manager.report_failure(node_id),
        Err(_) => {}
    }

    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn retries_before_success() {
        let peer_manager = PeerManager::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();

        let result = call_peer(&peer_manager, NodeId(7), None, move || {
            let attempts = attempts_clone.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(ApiError::Timeout)
                } else {
                    Ok::<_, ApiError>(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert!(peer_manager.is_healthy(NodeId(7)));
    }
}
