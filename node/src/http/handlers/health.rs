//! Health, info, and stats handlers.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use store::Store;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/health — liveness check.
pub async fn health<S: Store>() -> StatusCode {
    StatusCode::OK
}

/// GET /v1/info — node identification.
pub async fn info<S: Store>(
    State(state): State<AppState<S>>,
) -> impl IntoResponse {
    let config = &state.context.config;
    let body = serde_json::json!({
        "name": config.name,
        "version": env!("CARGO_PKG_VERSION"),
        "public_host": config.public_host,
        "public_port": config.public_port,
    });
    axum::Json(body)
}

/// GET /v1/stats — node statistics.
pub async fn stats<S: Store>(
    State(state): State<AppState<S>>,
) -> Result<impl IntoResponse, ApiError> {
    use tape_node_api::NodeStats;
    use tape_store::ops::MetaOps;

    let store = &state.context.store;
    let current_epoch = store
        .get_current_epoch()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .map(|e| e.0)
        .unwrap_or(0);
    let last_slot = store
        .get_sync_cursor()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .map(|s| s.0)
        .unwrap_or(0);

    let stats = NodeStats {
        last_processed_slot: last_slot,
        blocks_processed: 0,
        epoch_transitions: 0,
        current_epoch,
        owned_spools: 0,
        tracks_stored: 0,
        storage_bytes_used: 0,
        slices_stored: 0,
        bytes_uploaded: 0,
        bytes_downloaded: 0,
        requests_total: 0,
    };

    Ok(axum::Json(stats))
}
