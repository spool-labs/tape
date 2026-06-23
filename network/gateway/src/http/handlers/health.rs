use axum::extract::State;
use axum::Json;
use rpc::Rpc;
use store::{Column, Direction, Store};
use tape_protocol::Api;
use tape_protocol::api::NodeStats;
use tape_store::TapeStore;
use tape_store::columns::SliceCol;
use tape_store::ops::{MetaOps, TrackOps};
use tape_store::types::SliceValue;

use crate::http::error::RouteError;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

pub(crate) async fn health() -> &'static str {
    "ok"
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
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        requests_total: metrics.requests_total,
        ingest_state,
        ingest_lag_slots,
        ingest_tip_slot,
    }))
}

fn cached_slice_stats<Db: Store>(store: &TapeStore<Db>) -> Result<(u64, u64), RouteError> {
    let iter = store
        .inner()
        .inner()
        .iter_from(SliceCol::CF_NAME, &[], Direction::Asc)
        .map_err(store_error)?;
    let mut slices_stored = 0u64;
    let mut slice_payload_bytes = 0u64;

    for (_key, value_bytes) in iter {
        let data: SliceValue = wincode::deserialize(&value_bytes)
            .map_err(|error| RouteError::Internal(format!("slice value: {error}")))?;
        slices_stored = slices_stored.saturating_add(1);
        slice_payload_bytes = slice_payload_bytes.saturating_add(data.0.len() as u64);
    }

    Ok((slices_stored, slice_payload_bytes))
}
