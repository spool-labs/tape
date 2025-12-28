//! API routes and handlers.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use tape_metrics::OperationTimer;

use crate::error::ApiError;
use crate::metrics::NodeMetrics;

// Re-export shared constants from tape-core and tape-node-api
pub use tape_core::erasure::{MAX_SLICE_SIZE, SLICE_COUNT};
pub use tape_node_api::{
    HEALTH_PATH as HEALTH_ENDPOINT,
    INFO_PATH as INFO_ENDPOINT,
    METADATA_PATH as METADATA_ENDPOINT,
    SLICE_PATH as SLICE_ENDPOINT,
    STATUS_PATH as STATUS_ENDPOINT,
    SYNC_SHARD_PATH as SYNC_SHARD_ENDPOINT,
};

/// Shared state for API handlers.
#[derive(Clone)]
pub struct ApiState {
    pub metrics: Arc<NodeMetrics>,
    // TODO: Add service reference when service layer is implemented
}

/// Create the API router.
pub fn create_router(state: ApiState) -> Router {
    Router::new()
        // Slice operations
        .route(SLICE_ENDPOINT, get(get_slice).put(put_slice))
        // Metadata
        .route(METADATA_ENDPOINT, get(get_metadata).put(put_metadata))
        // Status
        .route(STATUS_ENDPOINT, get(get_status))
        // Health check
        .route(HEALTH_ENDPOINT, get(health_check))
        // Node info
        .route(INFO_ENDPOINT, get(get_info))
        // Shard sync (node-to-node)
        .route(SYNC_SHARD_ENDPOINT, post(sync_shard))
        .with_state(state)
}

/// GET /v1/tracks/:track_id/slices/:slice_index
pub async fn get_slice(
    State(state): State<ApiState>,
    Path((_track_id, slice_index)): Path<(String, u16)>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("get_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // TODO: Implement actual slice retrieval from storage
    // For now, return not found
    state
        .metrics
        .record_request("get_slice", "error", timer.elapsed_secs());
    Err(ApiError::NotFound)
}

/// PUT /v1/tracks/:track_id/slices/:slice_index
pub async fn put_slice(
    State(state): State<ApiState>,
    Path((_track_id, slice_index)): Path<(String, u16)>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // Validate slice index
    if slice_index >= SLICE_COUNT as u16 {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidSliceIndex);
    }

    // Validate body size
    if body.len() > MAX_SLICE_SIZE {
        state
            .metrics
            .record_request("put_slice", "error", timer.elapsed_secs());
        return Err(ApiError::InvalidBody);
    }

    // TODO: Implement actual slice storage
    // For now, just acknowledge
    state
        .metrics
        .record_request("put_slice", "success", timer.elapsed_secs());
    state.metrics.slices_stored_total.inc();
    state.metrics.bytes_stored_total.add(body.len() as i64);

    Ok(StatusCode::CREATED.into_response())
}

/// GET /v1/tracks/:track_id/metadata
pub async fn get_metadata(
    State(state): State<ApiState>,
    Path(_track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // TODO: Implement metadata retrieval
    state
        .metrics
        .record_request("get_metadata", "error", timer.elapsed_secs());
    Err(ApiError::TrackNotFound)
}

/// PUT /v1/tracks/:track_id/metadata
pub async fn put_metadata(
    State(state): State<ApiState>,
    Path(_track_id): Path<String>,
    _body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // TODO: Implement metadata storage
    state
        .metrics
        .record_request("put_metadata", "success", timer.elapsed_secs());
    Ok(StatusCode::CREATED.into_response())
}

/// GET /v1/tracks/:track_id/status
pub async fn get_status(
    State(state): State<ApiState>,
    Path(_track_id): Path<String>,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // TODO: Implement status check
    state
        .metrics
        .record_request("get_status", "error", timer.elapsed_secs());
    Err(ApiError::TrackNotFound)
}

/// GET /v1/health
pub async fn health_check() -> Response {
    StatusCode::OK.into_response()
}

/// GET /v1/info
pub async fn get_info(State(_state): State<ApiState>) -> Response {
    // TODO: Return node info (version, pubkey, etc.)
    let info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running"
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&info).unwrap_or_default(),
    )
        .into_response()
}

/// POST /v1/migrate/sync_shard
pub async fn sync_shard(
    State(state): State<ApiState>,
    _body: Bytes,
) -> Result<Response, ApiError> {
    let timer = OperationTimer::new();

    // TODO: Implement shard sync (Ed25519 signed request verification)
    state
        .metrics
        .record_request("sync_shard", "error", timer.elapsed_secs());
    Err(ApiError::Unauthorized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tape_metrics::MetricsRegistry;
    use tower::ServiceExt;

    fn create_test_state() -> ApiState {
        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        ApiState {
            metrics: Arc::new(NodeMetrics::new(registry.prometheus_registry())),
        }
    }

    #[tokio::test]
    async fn test_health_check() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_info() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/info")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_invalid_slice_index() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/tracks/test_track/slices/9999")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_put_slice() {
        let state = create_test_state();
        let app = create_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/tracks/test_track/slices/0")
                    .body(Body::from(vec![0u8; 1024]))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
    }
}
