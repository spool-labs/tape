use std::fmt::Display;

use axum::extract::State;
use axum::Json;
use tracing::debug;

use rpc::Rpc;
use store::Store;
use tape_protocol::{Api, api::NodeStats};
use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};

use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

#[derive(Debug, serde::Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
}

pub async fn health<Db: Store, Cluster: Api, Blockchain: Rpc>() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: HealthStatus::Ready,
    })
}

pub async fn stats<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Result<Json<NodeStats>, RouteError> {

    let store = &state.context.store;
    let current_state = state.context.state();
    let metrics = state.context.metrics.snapshot();
    let last_processed_slot = store
        .get_sync_cursor()
        .map_err(store_error)?
        .map(|slot| slot.0)
        .unwrap_or(0);

    let ingest_progress = state.context.ingest.progress();
    let ingest_tip_raw = ingest_progress.last_known_tip();
    let ingest_dispatched = ingest_progress.last_dispatched_slot();
    let ingest_tip_slot = if ingest_tip_raw == u64::MAX { 0 } else { ingest_tip_raw };
    let ingest_lag_slots = if ingest_tip_raw == u64::MAX {
        0
    } else {
        ingest_tip_raw.saturating_sub(ingest_dispatched)
    };
    let ingest_state = state.context.ingest_state().label().to_string();

    let owned_spools = store
        .iter_all_spools()
        .map_err(store_error)?;

    let mut slices_stored = 0u64;
    let mut slice_payload_bytes = 0u64;

    for (spool_id, _) in &owned_spools {
        let slices = store
            .iter_slices_by_spool(*spool_id)
            .map_err(store_error)?;
        slices_stored += slices.len() as u64;
        slice_payload_bytes += slices.iter().map(|(_, data)| data.len() as u64).sum::<u64>();
    }

    let store_disk_bytes = store
        .inner()
        .inner()
        .actual_size_bytes()
        .map_err(store_error)?;
    let free_disk_bytes = store
        .inner()
        .inner()
        .available_disk_bytes()
        .map_err(store_error)?;

    let stats = NodeStats {
        last_processed_slot,
        blocks_processed: metrics.blocks_processed_total,
        epoch_transitions: metrics.epoch_transitions_total,
        current_epoch: current_state.epoch().0,
        owned_spools: owned_spools.len() as u64,
        tracks_stored: store
            .count_tracks()
            .map_err(store_error)? as u64,
        slice_payload_bytes,
        store_disk_bytes,
        free_disk_bytes,
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        requests_total: metrics.requests_total,
        ingest_state,
        ingest_lag_slots,
        ingest_tip_slot,
    };

    debug!(
        current_epoch = stats.current_epoch,
        owned_spools = stats.owned_spools,
        tracks_stored = stats.tracks_stored,
        "http stats served"
    );

    Ok(Json(stats))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}
