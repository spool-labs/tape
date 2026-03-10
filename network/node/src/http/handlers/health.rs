//! Health, info, and stats handlers.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use rpc::Rpc;
use tape_protocol::Api;
use store::Store;

use crate::http::error::ApiError;
use crate::http::state::AppState;

/// GET /v1/health — liveness check.
pub async fn health<Db: Store, Cluster: Api, Blockchain: Rpc>() -> StatusCode {
    tracing::trace!("http health check");
    StatusCode::OK
}

/// GET /v1/info — node identification.
pub async fn info<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> impl IntoResponse {
    tracing::trace!(name = %state.context.config.name, "http node info");
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
pub async fn stats<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Result<impl IntoResponse, ApiError> {
    tracing::trace!("http stats start");
    use std::sync::atomic::Ordering::Relaxed;
    use tape_protocol::api::NodeStats;
    use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};

    let store = &state.context.store;
    let current_epoch = state.context.state().epoch.0;
    let last_slot = store
        .get_sync_cursor()
        .map_err(|e| ApiError::InternalError(e.to_string()))?
        .map(|s| s.0)
        .unwrap_or(0);

    let owned_spools_list = store.iter_all_spools().unwrap_or_default();
    let owned_spools = owned_spools_list.len() as u64;

    let mut slices_stored: u64 = 0;
    for (spool_id, _status) in &owned_spools_list {
        if let Ok(count) = store.count_slices_by_spool(*spool_id) {
            slices_stored += count as u64;
        }
    }

    let rs = &state.context.stats;
    let stats = NodeStats {
        last_processed_slot: last_slot,
        blocks_processed: rs.blocks_processed.load(Relaxed),
        epoch_transitions: rs.epoch_transitions.load(Relaxed),
        current_epoch,
        owned_spools,
        tracks_stored: store.count_tracks().unwrap_or(0) as u64,
        storage_bytes_used: 0,
        slices_stored,
        bytes_uploaded: rs.bytes_uploaded.load(Relaxed),
        bytes_downloaded: rs.bytes_downloaded.load(Relaxed),
        requests_total: 0,
    };
    tracing::trace!(
        current_epoch = stats.current_epoch,
        owned_spools = stats.owned_spools,
        tracks_stored = stats.tracks_stored,
        "http stats success"
    );

    Ok(axum::Json(stats))
}
