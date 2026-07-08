use std::fmt::Display;

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use tracing::debug;

use rpc::Rpc;
use store::{DiskVolume, Store, StoreVolume};
use tape_protocol::{Api, api::{NodeStats, VolumeStats}};
use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};

use crate::core::bootstrap::BootstrapSnapshot;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

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

#[derive(Debug, serde::Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapProgress>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
    Bootstrapping,
}

#[derive(Debug, serde::Serialize)]
pub struct BootstrapProgress {
    pub phase: &'static str,
    pub snapshot_epoch: u64,
    pub start_slot: u64,
    pub current_slot: u64,
    pub target_slot: u64,
    pub percent_done: f64,
    pub slots_per_sec: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta_secs: Option<u64>,
    pub skipped_slots: u64,
}

impl From<BootstrapSnapshot> for BootstrapProgress {
    fn from(snapshot: BootstrapSnapshot) -> Self {
        Self {
            phase: snapshot.phase.label(),
            snapshot_epoch: snapshot.snapshot_epoch,
            start_slot: snapshot.start_slot,
            current_slot: snapshot.current_slot,
            target_slot: snapshot.target_slot,
            percent_done: snapshot.percent_done(),
            slots_per_sec: snapshot.slots_per_sec,
            eta_secs: snapshot.eta_secs,
            skipped_slots: snapshot.skipped_slots,
        }
    }
}

pub async fn health<Db: Store, Cluster: Api, Blockchain: Rpc>(
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

    let (ingest_tip_slot, _, ingest_lag_slots) = state.context.ingest.progress().tip_and_lag();
    let ingest_state = state.context.ingest_state().label().to_string();
    let bootstrap = state.context.bootstrap.snapshot();

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
    let disk_volumes = store
        .inner()
        .inner()
        .disk_volumes()
        .map_err(store_error)?
        .into_iter()
        .map(volume_stats)
        .collect();

    let stats = NodeStats {
        version: crate::VERSION.to_string(),
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
        disk_volumes,
        reclaim_pending: state.context.is_reclaim_pending(),
        slices_stored,
        bytes_uploaded: metrics.bytes_uploaded,
        bytes_downloaded: metrics.bytes_downloaded,
        requests_total: metrics.requests_total,
        ingest_state,
        ingest_lag_slots,
        ingest_tip_slot,
        bootstrap_done: state.context.bootstrap.is_ready(),
        bootstrap_phase: bootstrap.phase.label().to_string(),
        bootstrap_current_slot: bootstrap.current_slot,
        bootstrap_target_slot: bootstrap.target_slot,
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

#[cfg(test)]
mod tests {
    use axum::extract::State;
    use axum::http::StatusCode;

    use super::{health, stats, HealthStatus};
    use crate::features::http::state::AppState;
    use crate::harness::{NodeHarness, TestContext};

    async fn test_context() -> TestContext {
        NodeHarness::builder()
            .nodes(25)
            .no_prev_snapshot_tape()
            .build()
            .await
            .expect("build harness")
            .ctx_for(0)
    }

    #[tokio::test]
    async fn health_during_bootstrap() {
        let ctx = test_context().await;
        ctx.bootstrap.begin_block_replay(100, 200);
        ctx.bootstrap.record_slot(150);

        let (status, body) = health(State(AppState { context: ctx.clone() })).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(matches!(body.0.status, HealthStatus::Bootstrapping));
        let progress = body.0.bootstrap.expect("progress while bootstrapping");
        assert_eq!(progress.phase, "block_replay");
        assert_eq!(progress.current_slot, 150);
        assert_eq!(progress.target_slot, 200);
    }

    #[tokio::test]
    async fn health_when_ready() {
        let ctx = test_context().await;
        ctx.bootstrap.mark_ready();

        let (status, body) = health(State(AppState { context: ctx.clone() })).await;

        assert_eq!(status, StatusCode::OK);
        assert!(matches!(body.0.status, HealthStatus::Ready));
        assert!(body.0.bootstrap.is_none());
    }

    #[tokio::test]
    async fn stats_reports_build_version() {
        let ctx = test_context().await;

        let body = stats(State(AppState { context: ctx })).await.expect("stats");

        assert_eq!(body.0.version, crate::VERSION);
    }
}
