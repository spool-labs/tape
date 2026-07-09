use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use rpc::Rpc;
use store::{DiskVolume, Store, StoreVolume};
use tape_node::features::http::handlers::health::{HealthResponse, HealthStatus};
use tape_protocol::Api;
use tape_protocol::api::{NodeStats, VolumeStats};
use tape_store::TapeStore;
use tape_store::ops::{MetaOps, SliceOps, TrackOps};

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(crate) async fn health<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> (StatusCode, Json<HealthResponse>) {
    if state.context.bootstrap.is_ready() {
        return (
            StatusCode::OK,
            Json(HealthResponse {
                status: HealthStatus::Ready,
                bootstrap: None,
            }),
        );
    }

    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(HealthResponse {
            status: HealthStatus::Bootstrapping,
            bootstrap: Some(state.context.bootstrap.snapshot().into()),
        }),
    )
}

/// Map a backend disk volume to its wire representation.
fn volume_stats(volume: DiskVolume) -> VolumeStats {
    let name = match volume.volume {
        StoreVolume::Primary => "primary",
        StoreVolume::Bulk => "bulk",
    };
    VolumeStats {
        name: name.to_string(),
        store_disk_bytes: volume.used_bytes,
        free_disk_bytes: volume.free_bytes,
    }
}

pub(crate) async fn stats<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Result<Json<NodeStats>, RouteError> {
    let store = state.context.store.as_ref();
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
    let ingest_tip_slot = if ingest_tip_raw == u64::MAX {
        0
    } else {
        ingest_tip_raw
    };
    let ingest_lag_slots = if ingest_tip_raw == u64::MAX {
        0
    } else {
        ingest_tip_raw.saturating_sub(ingest_dispatched)
    };
    let ingest_state = state.context.ingest_state().label().to_string();
    let bootstrap = state.context.bootstrap.snapshot();
    let (slices_stored, slice_payload_bytes) = cached_slice_stats(store)?;
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
    let disk_volumes = store
        .inner()
        .inner()
        .disk_volumes()
        .map_err(store_error)?
        .into_iter()
        .map(volume_stats)
        .collect();

    Ok(Json(NodeStats {
        last_processed_slot,
        blocks_processed: metrics.blocks_processed_total,
        epoch_transitions: metrics.epoch_transitions_total,
        current_epoch: current_state.epoch().0,
        owned_spools: 0,
        tracks_stored: store.count_tracks().map_err(store_error)? as u64,
        slice_payload_bytes,
        store_disk_bytes,
        free_disk_bytes,
        disk_volumes,
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        requests_total: metrics.requests_total,
        ingest_state,
        ingest_lag_slots,
        ingest_tip_slot,
        ingest_fetch_slot: ingest_progress.last_fetch_slot(),
        ingest_queue_len: ingest_progress.queue_len(),
        bootstrap_done: state.context.bootstrap.is_ready(),
        bootstrap_phase: bootstrap.phase.label().to_string(),
        bootstrap_current_slot: bootstrap.current_slot,
        bootstrap_target_slot: bootstrap.target_slot,
    }))
}

fn cached_slice_stats<Db: Store>(store: &TapeStore<Db>) -> Result<(u64, u64), RouteError> {
    let (slices_stored, slice_payload_bytes) = store.slice_totals().map_err(store_error)?;
    Ok((slices_stored, slice_payload_bytes.as_u64()))
}
