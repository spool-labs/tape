use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use tape_rpc::Rpc;
use tape_store_api::Store;
use tape_node::features::http::handlers::health::{health_of, store_totals, HealthResponse};
use tape_protocol::Api;
use tape_protocol::api::NodeStats;
use tape_store::ops::MetaOps;

use crate::http::error::RouteError;
use crate::http::handlers::s3::routes::max_object_size;
use crate::http::handlers::store_error;
use crate::http::state::AppState;

/// Where the S3 listener is and which delegate it writes as.
pub const S3_INFO_PATH: &str = "/v1/s3";

#[derive(Serialize)]
pub struct S3Info {
    /// Public S3 endpoint, when the operator configured one.
    pub endpoint: Option<String>,
    /// Address tape owners delegate to so the gateway can write for them.
    pub delegate: Option<String>,
    /// Largest object a single upload may carry, in bytes.
    pub max_object_bytes: u64,
}

pub async fn s3_info<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> Json<S3Info> {
    Json(S3Info {
        endpoint: state.context.config.gateway.s3.public_endpoint.clone(),
        delegate: state.s3_delegate.map(|address| address.to_string()),
        max_object_bytes: max_object_size(
            state.context.config.gateway.s3.max_object_bytes,
            state.context.params().profile,
        ),
    })
}

pub async fn health<Db: Store, Cluster: Api, Blockchain: Rpc>(
    State(state): State<AppState<Db, Cluster, Blockchain>>,
) -> (StatusCode, Json<HealthResponse>) {
    let (status, body) = health_of(&state.context);
    (status, Json(body))
}


pub async fn stats<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
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
    let totals = store_totals(&state.context.store).await.map_err(store_error)?;

    Ok(Json(NodeStats {
        version: tape_node::VERSION.to_string(),
        store_engine: store.inner().inner().store_engine().label().to_string(),
        last_processed_slot,
        blocks_processed: metrics.blocks_processed_total,
        epoch_transitions: metrics.epoch_transitions_total,
        current_epoch: current_state.epoch().0,
        owned_spools: 0,
        tracks_stored: totals.tracks_stored,
        slice_payload_bytes: totals.slice_payload_bytes,
        store_disk_bytes: totals.store_disk_bytes,
        store_data_bytes: totals.store_data_bytes,
        free_disk_bytes: totals.free_disk_bytes,
        disk_volumes: totals.disk_volumes.clone(),
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored: totals.slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        sync_bytes_fetched: metrics.sync_bytes_fetched,
        repair_bytes_fetched: metrics.repair_bytes_fetched,
        recover_bytes_fetched: metrics.recover_bytes_fetched,
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
        fee_payer_lamports: state.context.fee_payer_balance().map(|b| b.0),
        restarts: tape_node::core::restarts::count(),
    }))
}
