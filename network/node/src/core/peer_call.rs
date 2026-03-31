use std::future::Future;

use peer_manager::PeerManager;
use tape_core::types::NodeId;
use tape_protocol::api::ApiError;
use tape_retry::{RetryConfig, Retryable, retry_if};
use tokio_util::sync::CancellationToken;
use tracing::info;

pub async fn call_peer<T, F, Fut>(
    peer_manager: &PeerManager,
    retry: RetryConfig,
    node_id: NodeId,
    cancel: Option<&CancellationToken>,
    f: F,
) -> Result<T, ApiError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    let result = retry_if(
        retry,
        cancel, 
        f, 
        |e| { 
            info!("peer call to {} failed: {e}", node_id);
            ApiError::is_retryable(e)
        }
    ).await;

    match &result {
        Ok(_) => peer_manager.report_success(node_id),
        Err(error) if error.is_retryable() => peer_manager.report_failure(node_id),
        Err(_) => {}
    }

    result
}
