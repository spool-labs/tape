//! Node info and stats handlers.

use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use store::Store;
use tape_node_api::NodeStats;

use super::ApiState;

/// GET /v1/info
pub async fn get_info<S: Store>(State(state): State<ApiState<S>>) -> Response {
    let node_id = state.control_plane.our_node_id();
    let info = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "status": "running",
        "node_id": node_id.as_u64(),
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&info).unwrap_or_default(),
    )
        .into_response()
}

/// GET /v1/stats
///
/// Returns block processor and storage metrics.
pub async fn get_stats<S: Store>(State(state): State<ApiState<S>>) -> Response {
    let stats = NodeStats {
        last_processed_slot: state.metrics.last_processed_slot.get() as u64,
        blocks_processed: state.metrics.blocks_processed_total.get(),
        epoch_transitions: state.metrics.epoch_transitions_total.get(),
        current_epoch: state.metrics.current_epoch.get() as u64,
        owned_spools: state.metrics.owned_spools.get() as u64,
        tracks_stored: state.metrics.tracks_stored.get() as u64,
        storage_bytes_used: state.metrics.storage_bytes_used.get() as u64,
        slices_stored: state.metrics.slices_stored_total.get() as u64,
        bytes_uploaded: state.metrics.bytes_stored_total.get() as u64,
        bytes_downloaded: state.metrics.bytes_retrieved_total.get() as u64,
        requests_total: state.metrics.requests_handled_total.get(),
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&stats).unwrap_or_default(),
    )
        .into_response()
}
