use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use tokio::sync::Mutex;
use tracing::debug;

use tape_core::types::SlotNumber;
use tape_rpc::Rpc;
use tape_store_api::{Column, DiskVolume, Store, StoreVolume};
use tape_protocol::{Api, api::{NodeStats, VolumeStats}};
use tape_store::columns::TrackCol;
use tape_store::ops::{MetaOps, SliceOps, SpoolOps, TrackOps};
use tape_store::TapeStore;

use crate::context::NodeContext;
use crate::core::bootstrap::BootstrapSnapshot;
use crate::core::error::NodeError;
use crate::core::ingest::IngestState;
use crate::features::http::error::RouteError;
use crate::features::http::state::AppState;

/// A read of the store totals that took this long or more is reused
const SLOW_TOTALS_READ: Duration = Duration::from_millis(100);
/// A slow read is reused for 100 times its own duration, so reads take at most 1% of the time
const TOTALS_REUSE_FACTOR: u32 = 100;
/// The longest a slow read is reused
const TOTALS_REUSE_MAX: Duration = Duration::from_secs(60);

/// The figures in `/v1/stats` that cost a read of the store
#[derive(Clone, Debug)]
pub struct StoreTotals {
    pub owned_spools: u64,
    pub tracks_stored: u64,
    pub slices_stored: u64,
    pub slice_payload_bytes: u64,
    pub store_disk_bytes: u64,
    pub store_data_bytes: u64,
    pub free_disk_bytes: Option<u64>,
    pub disk_volumes: Vec<VolumeStats>,
}

pub struct CachedTotals {
    pub fresh_until: Instant,
    pub totals: Arc<StoreTotals>,
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

#[derive(Debug, serde::Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<BootstrapProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingest: Option<IngestStall>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Ready,
    Bootstrapping,
    Stalled,
}

/// Ingest cursor positions while the node reports `Stalled`
#[derive(Debug, serde::Serialize)]
pub struct IngestStall {
    pub tip_slot: SlotNumber,
    pub fetch_slot: SlotNumber,
    pub dispatched_slot: SlotNumber,
    pub queue_len: u64,
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
    let (status, body) = health_of(&state.context);
    (status, Json(body))
}

/// Readiness of a node or gateway: still bootstrapping, ingest stalled, or ready
pub fn health_of<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> (StatusCode, HealthResponse) {
    if !context.bootstrap.is_ready() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HealthResponse {
                status: HealthStatus::Bootstrapping,
                bootstrap: Some(context.bootstrap.snapshot().into()),
                ingest: None,
            },
        );
    }

    if matches!(context.ingest_state(), IngestState::Stalled { .. }) {
        let progress = context.ingest.progress();
        let (tip_slot, dispatched_slot, _) = progress.tip_and_lag();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            HealthResponse {
                status: HealthStatus::Stalled,
                bootstrap: None,
                ingest: Some(IngestStall {
                    tip_slot: SlotNumber(tip_slot),
                    fetch_slot: SlotNumber(progress.last_fetch_slot()),
                    dispatched_slot: SlotNumber(dispatched_slot),
                    queue_len: progress.queue_len(),
                }),
            },
        );
    }

    (
        StatusCode::OK,
        HealthResponse {
            status: HealthStatus::Ready,
            bootstrap: None,
            ingest: None,
        },
    )
}

pub async fn stats<Db: Store + 'static, Cluster: Api, Blockchain: Rpc>(
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
    let (ingest_tip_slot, _, ingest_lag_slots) = ingest_progress.tip_and_lag();
    let ingest_state = state.context.ingest_state().label().to_string();
    let bootstrap = state.context.bootstrap.snapshot();
    let totals = store_totals(store).await.map_err(store_error)?;

    let stats = NodeStats {
        version: crate::VERSION.to_string(),
        store_engine: store.inner().inner().store_engine().label().to_string(),
        last_processed_slot,
        blocks_processed: metrics.blocks_processed_total,
        epoch_transitions: metrics.epoch_transitions_total,
        current_epoch: current_state.epoch().0,
        owned_spools: totals.owned_spools,
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
        restarts: crate::core::restarts::count(),
    };

    debug!(
        current_epoch = stats.current_epoch,
        owned_spools = stats.owned_spools,
        tracks_stored = stats.tracks_stored,
        "http stats served"
    );

    Ok(Json(stats))
}

/// One slot per store, since tests and simnet run several nodes in a process
fn totals_slot(key: usize) -> Arc<Mutex<Option<CachedTotals>>> {
    static SLOTS: OnceLock<StdMutex<HashMap<usize, Arc<Mutex<Option<CachedTotals>>>>>> = OnceLock::new();
    let slots = SLOTS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut slots = slots.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Arc::clone(slots.entry(key).or_default())
}

/// The store figures, read by one caller per store at a time, with a slow read reused for a while
pub async fn store_totals<Db: Store + 'static>(store: &Arc<TapeStore<Db>>) -> Result<Arc<StoreTotals>, NodeError> {
    let slot = totals_slot(Arc::as_ptr(store) as usize);
    let store = Arc::clone(store);
    // A spawned task finishes its read when the caller goes away, so a timed out caller never starts a second one
    let task = tokio::spawn(async move {
        let mut cached = slot.lock().await;
        if let Some(fresh) = cached.as_ref().filter(|cached| Instant::now() < cached.fresh_until) {
            return Ok(Arc::clone(&fresh.totals));
        }

        let started = Instant::now();
        let read = tokio::task::spawn_blocking(move || read_store_totals(&store)).await;
        let read = read.map_err(|error| NodeError::Store(format!("store totals task: {error}")))?;
        let totals = Arc::new(read?);
        *cached = reuse_for(started.elapsed())
            .map(|reuse| CachedTotals { fresh_until: Instant::now() + reuse, totals: Arc::clone(&totals) });
        Ok(totals)
    });
    task.await.map_err(|error| NodeError::Store(format!("store totals task: {error}")))?
}

/// How long to reuse a read that took `took`, and None for a fast read
pub fn reuse_for(took: Duration) -> Option<Duration> {
    if took < SLOW_TOTALS_READ {
        return None;
    }
    Some((took * TOTALS_REUSE_FACTOR).min(TOTALS_REUSE_MAX))
}

fn read_store_totals<Db: Store>(store: &TapeStore<Db>) -> Result<StoreTotals, NodeError> {
    let backend = store.inner().inner();
    let owned_spools = store.iter_all_spools().map_err(totals_error)?;
    // Zero bytes from a backend that can't weigh the column without reading it
    let (slices_stored, slice_bytes) = store.slice_totals().map_err(totals_error)?;

    let mut disk_volumes = Vec::new();
    for volume in backend.disk_volumes().map_err(totals_error)? {
        disk_volumes.push(volume_stats(volume));
    }

    Ok(StoreTotals {
        owned_spools: owned_spools.len() as u64,
        tracks_stored: track_count(store)?,
        slices_stored,
        slice_payload_bytes: slice_bytes.unwrap_or_default().as_u64(),
        store_disk_bytes: backend.actual_size_bytes().map_err(totals_error)?,
        store_data_bytes: backend.live_data_size_bytes().map_err(totals_error)?.unwrap_or(0),
        free_disk_bytes: backend.available_disk_bytes().map_err(totals_error)?,
        disk_volumes,
    })
}

/// The engine's cached track count, or an exact count from an engine without one
fn track_count<Db: Store>(store: &TapeStore<Db>) -> Result<u64, NodeError> {
    match store.inner().inner().key_count_estimate(TrackCol::CF_NAME).map_err(totals_error)? {
        Some(count) => Ok(count),
        None => Ok(store.count_tracks().map_err(totals_error)? as u64),
    }
}

fn totals_error(error: impl Display) -> NodeError {
    NodeError::Store(format!("store totals: {error}"))
}

fn store_error(error: impl Display) -> RouteError {
    RouteError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::extract::State;
    use axum::http::StatusCode;

    use tape_core::types::SlotNumber;

    use super::{health, reuse_for, stats, HealthStatus};
    use crate::core::ingest::IngestState;
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

    // a stalled ingest answers 503 with the cursor positions
    #[tokio::test]
    async fn health_when_stalled() {
        let ctx = test_context().await;
        ctx.bootstrap.mark_ready();
        let progress = ctx.ingest.progress();
        progress.record_tip(1000);
        progress.record_fetch(999);
        progress.record_dispatched(500);
        progress.record_queue_len(499);
        ctx.ingest.publish(IngestState::Stalled { since: Instant::now() });

        let (status, body) = health(State(AppState { context: ctx.clone() })).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(matches!(body.0.status, HealthStatus::Stalled));
        let stall = body.0.ingest.expect("stall detail");
        assert_eq!(stall.tip_slot, SlotNumber(1000));
        assert_eq!(stall.fetch_slot, SlotNumber(999));
        assert_eq!(stall.dispatched_slot, SlotNumber(500));
        assert_eq!(stall.queue_len, 499);
    }

    #[tokio::test]
    async fn stats_reports_build_version() {
        let ctx = test_context().await;

        let body = stats(State(AppState { context: ctx })).await.expect("stats");

        assert_eq!(body.0.version, crate::VERSION);
    }

    // a fast read is never reused, and a slow one for longer the slower it was
    #[test]
    fn reuse_rule() {
        assert_eq!(reuse_for(Duration::from_millis(5)), None);
        assert_eq!(reuse_for(Duration::from_millis(200)), Some(Duration::from_secs(20)));
        assert_eq!(reuse_for(Duration::from_secs(5)), Some(Duration::from_secs(60)));
    }

}
