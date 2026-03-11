use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Instant;

use arc_swap::ArcSwap;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::signature::Signer;
use tape_api::state::Track;
use tape_core::erasure::SPOOL_COUNT;
use peer_memory::MemoryApi;
use tape_node::core::NodeContext;
use tape_store::MemoryStore;
use tape_store::ops::SpoolOps;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::app::{
    NodeSnapshot, PollSnapshot, TrackSnapshot, TrackStatus,
};
use crate::log_layer::LogHistogram;

/// Shared snapshot handle created in main and passed to both poller and TUI.
pub type SnapshotHandle = Arc<ArcSwap<PollSnapshot>>;

pub enum PollerUpdate {
    AddNode(usize, Arc<NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>>),
    RemoveNode(usize),
    StakeFuzzStatus {
        enabled: bool,
        succeeded: u64,
        failed: u64,
    },
    UploadStatus {
        pending: u64,
        certified: u64,
        expired: u64,
        failed: u64,
        retries: u64,
        last_retry_error: Option<String>,
        next_retry_in_ms: Option<u64>,
        retry_in_progress: bool,
    },
}

pub struct PollerHandle {
    tx: mpsc::UnboundedSender<PollerUpdate>,
}

impl PollerHandle {
    pub fn spawn(
        rpc: LiteSvmRpc,
        snapshot: Arc<ArcSwap<PollSnapshot>>,
        histogram: LogHistogram,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(poller_task(rpc, snapshot, histogram, rx));
        Self { tx }
    }

    pub fn spawn_noop() -> Self {
        let (tx, _rx) = mpsc::unbounded_channel();
        Self { tx }
    }

    pub fn send(&self, update: PollerUpdate) {
        let _ = self.tx.send(update);
    }
}

struct TrackedNode {
    id: usize,
    ctx: Arc<NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>>,
    prev_sync: u64,
    prev_repair: u64,
    prev_recovery: u64,
    prev_upload: u64,
    prev_events: u64,
    pool_stake: u64,
    event_history: Vec<u64>,
    node_event_accum: u64,
}

struct PollerState {
    rpc: RpcClient<LiteSvmRpc>,
    nodes: Vec<TrackedNode>,
    start: Instant,
    prev_epoch: u64,
    epoch_start: Instant,
    epoch_duration_history: Vec<u64>,
    total_store_history: Vec<u64>,
    repair_bw_history: Vec<u64>,
    recovery_bw_history: Vec<u64>,
    sync_bw_history: Vec<u64>,
    upload_bw_history: Vec<u64>,
    sync_accum: u64,
    repair_accum: u64,
    recovery_accum: u64,
    upload_accum: u64,
    total_sync: u64,
    total_repair: u64,
    total_recovery: u64,
    total_upload: u64,
    stake_fuzz_enabled: bool,
    stake_fuzz_succeeded: u64,
    stake_fuzz_failed: u64,
    uploads_pending: u64,
    uploads_certified: u64,
    uploads_expired: u64,
    uploads_failed: u64,
    uploads_retries: u64,
    uploads_last_retry_error: Option<String>,
    uploads_next_retry_in_ms: Option<u64>,
    uploads_retry_in_progress: bool,
    track_next_refresh: Instant,
    chart_next_update: Instant,
    track_pending: u64,
    track_certified: u64,
    track_expired: u64,
    track_failed: u64,
    tracks: Vec<TrackSnapshot>,
}

const HISTORY_CAP: usize = 200;
const TRACK_POLL_INTERVAL: Duration = Duration::from_secs(2);
const CHART_UPDATE_INTERVAL: Duration = Duration::from_secs(10);

fn push_capped(buf: &mut Vec<u64>, val: u64) {
    buf.push(val);
    if buf.len() > HISTORY_CAP {
        buf.remove(0);
    }
}

async fn poller_task(
    rpc: LiteSvmRpc,
    snapshot: Arc<ArcSwap<PollSnapshot>>,
    histogram: LogHistogram,
    mut update_rx: mpsc::UnboundedReceiver<PollerUpdate>,
) {
    let mut state = PollerState {
        rpc: RpcClient::from_rpc(rpc),
        nodes: Vec::new(),
        start: Instant::now(),
        prev_epoch: 0,
        epoch_start: Instant::now(),
        epoch_duration_history: Vec::new(),
        total_store_history: Vec::new(),
        repair_bw_history: Vec::new(),
        recovery_bw_history: Vec::new(),
        sync_bw_history: Vec::new(),
        upload_bw_history: Vec::new(),
        sync_accum: 0,
        repair_accum: 0,
        recovery_accum: 0,
        upload_accum: 0,
        total_sync: 0,
        total_repair: 0,
        total_recovery: 0,
        total_upload: 0,
        stake_fuzz_enabled: false,
        stake_fuzz_succeeded: 0,
        stake_fuzz_failed: 0,
        uploads_pending: 0,
        uploads_certified: 0,
        uploads_expired: 0,
        uploads_failed: 0,
        uploads_retries: 0,
        uploads_last_retry_error: None,
        uploads_next_retry_in_ms: None,
        uploads_retry_in_progress: false,
        track_next_refresh: Instant::now(),
        chart_next_update: Instant::now() + CHART_UPDATE_INTERVAL,
        track_pending: 0,
        track_certified: 0,
        track_expired: 0,
        track_failed: 0,
        tracks: Vec::new(),
    };

    let mut interval = time::interval(Duration::from_secs(1));

    loop {
                tokio::select! {
            _ = interval.tick() => {
                poll_once(&mut state, &snapshot, &histogram).await;
            }
            msg = update_rx.recv() => {
                match msg {
                    Some(PollerUpdate::AddNode(id, ctx)) => {
                        state.nodes.push(TrackedNode {
                            id,
                            ctx,
                            prev_sync: 0,
                            prev_repair: 0,
                            prev_recovery: 0,
                            prev_upload: 0,
                            prev_events: 0,
                            pool_stake: 0,
                            event_history: Vec::new(),
                            node_event_accum: 0,
                        });
                    }
                    Some(PollerUpdate::RemoveNode(id)) => {
                        state.nodes.retain(|n| n.id != id);
                    }
                    Some(PollerUpdate::StakeFuzzStatus { enabled, succeeded, failed }) => {
                        state.stake_fuzz_enabled = enabled;
                        state.stake_fuzz_succeeded = succeeded;
                        state.stake_fuzz_failed = failed;
                    }
                    Some(PollerUpdate::UploadStatus {
                        pending,
                        certified,
                        expired,
                        failed,
                        retries,
                        last_retry_error,
                        next_retry_in_ms,
                        retry_in_progress,
                    }) => {
                        state.uploads_pending = pending;
                        state.uploads_certified = certified;
                        state.uploads_expired = expired;
                        state.uploads_failed = failed;
                        state.uploads_retries = retries;
                        state.uploads_last_retry_error = last_retry_error;
                        state.uploads_next_retry_in_ms = next_retry_in_ms;
                        state.uploads_retry_in_progress = retry_in_progress;
                    }
                    None => break,
                }
            }
        }
    }
}

async fn poll_once(
    state: &mut PollerState,
    snapshot: &Arc<ArcSwap<PollSnapshot>>,
    histogram: &LogHistogram,
) {
    let slot = state.rpc.get_slot().await.unwrap_or(0);
    let epoch_account = state.rpc.get_epoch().await.ok();
    let epoch = epoch_account.map(|e| e.id.as_u64()).unwrap_or(0);
    let (epoch_phase, epoch_phase_weight) = match &epoch_account {
        Some(e) => {
            let phase = match tape_core::system::EpochPhase::try_from(e.state.phase) {
                Ok(tape_core::system::EpochPhase::Syncing) => "Syncing",
                Ok(tape_core::system::EpochPhase::Settling) => "Settling",
                Ok(tape_core::system::EpochPhase::Active) => "Active",
                _ => "?",
            };
            (phase.to_string(), e.state.weight())
        }
        None => (String::new(), None),
    };
    let now = Instant::now();

    if now >= state.track_next_refresh {
        match state.rpc.get_all_tapes().await {
            Ok(tapes) => {
                let tape_expiry = tapes
                    .into_iter()
                    .map(|(pubkey, tape)| (pubkey, tape.expiry_epoch.as_u64()))
                    .collect::<HashMap<_, _>>();

                match state.rpc.get_all_tracks().await {
                    Ok(tracks) => {
                        let (
                            track_snapshots,
                            track_pending,
                            track_certified,
                            track_expired,
                            track_failed,
                        ) = classify_tracks(epoch, tracks, tape_expiry);
                        state.tracks = track_snapshots;
                        state.track_pending = track_pending;
                        state.track_certified = track_certified;
                        state.track_expired = track_expired;
                        state.track_failed = track_failed;
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "failed to refresh track status from chain"
                        );
                    }
                }
            }
            Err(error) => {
                tracing::warn!(error = %error, "failed to refresh tape accounts from chain");
            }
        }
        state.track_next_refresh = now + TRACK_POLL_INTERVAL;
    }

    let mut spool_owners = [0u8; SPOOL_COUNT];
    let mut committee_prev_size = 0;
    let mut committee_size = 0;
    let mut committee_next_size = 0;
    if let Ok(system) = state.rpc.get_system().await {
        for (i, owner) in system.spools.0.iter().enumerate() {
            if i < SPOOL_COUNT {
                spool_owners[i] = *owner;
            }
        }
        committee_prev_size = system.committee_prev.size();
        committee_size = system.committee.size();
        committee_next_size = system.committee_next.size();
    }

    // Build per-spool availability: a spool is available if ANY node has it Active.
    // Multiple nodes may have the same spool (Active on the current owner,
    // LockedToMove on the former owner), so we must not let a non-Active
    // status overwrite an Active one.
    let mut spool_available = [false; SPOOL_COUNT];
    for tracked in &state.nodes {
        if let Ok(spools) = tracked.ctx.store.iter_all_spools() {
            for (spool_id, spool_state) in spools {
                if (spool_id as usize) < SPOOL_COUNT && spool_state.is_active() {
                    spool_available[spool_id as usize] = true;
                }
            }
        }
    }

    let mut total_sync_delta = 0u64;
    let mut total_repair_delta = 0u64;
    let mut total_recovery_delta = 0u64;
    let mut total_upload_delta = 0u64;

    let mut node_snapshots = Vec::with_capacity(state.nodes.len());

    for tracked in &mut state.nodes {
        let sync = tracked.ctx.stats.sync_bytes_received.load(Ordering::Relaxed);
        let repair = tracked.ctx.stats.repair_bytes_received.load(Ordering::Relaxed);
        let recovery = tracked.ctx.stats.recovery_bytes_received.load(Ordering::Relaxed);
        let upload = tracked.ctx.stats.bytes_uploaded.load(Ordering::Relaxed);

        let sync_delta = sync.saturating_sub(tracked.prev_sync);
        let repair_delta = repair.saturating_sub(tracked.prev_repair);
        let recovery_delta = recovery.saturating_sub(tracked.prev_recovery);
        let upload_delta = upload.saturating_sub(tracked.prev_upload);
        let events = tracked.ctx.stats.events.load(Ordering::Relaxed);
        let event_delta = events.saturating_sub(tracked.prev_events);
        let transport_delta = sync_delta
            .saturating_add(repair_delta)
            .saturating_add(recovery_delta)
            .saturating_add(upload_delta);

        tracked.prev_sync = sync;
        tracked.prev_repair = repair;
        tracked.prev_recovery = recovery;
        tracked.prev_upload = upload;
        tracked.prev_events = events;
        tracked.node_event_accum = tracked
            .node_event_accum
            .saturating_add(event_delta)
            .saturating_add(transport_delta);

        total_sync_delta += sync_delta;
        total_repair_delta += repair_delta;
        total_recovery_delta += recovery_delta;
        total_upload_delta += upload_delta;

        let state = tracked.ctx.state();
        let spool_count = tracked.ctx.my_spools().len();
        let node_status = if !state.epoch.is_zero() {
            Some(tracked.ctx.node_status())
        } else {
            None
        };

        let authority = tracked.ctx.keypair.pubkey();
        if let Ok(node) = tracked.ctx.rpc.get_node(&authority).await {
            tracked.pool_stake = node.pool.stake.as_u64();
        }

        let mut ns = NodeSnapshot {
            id: tracked.id,
            sync_bytes: sync,
            repair_bytes: repair,
            recovery_bytes: recovery,
            upload_bytes: upload,
            spool_count,
                pool_stake: tracked.pool_stake,
                node_status,
                event_history: tracked.event_history.clone(),
                sync_bw_history: Vec::new(),
            };

        // We'll just store the delta as the latest bw for the sparkline
        ns.sync_bw_history.push(sync_delta);
        node_snapshots.push(ns);
    }

    state.sync_accum += total_sync_delta;
    state.repair_accum += total_repair_delta;
    state.recovery_accum += total_recovery_delta;
    state.upload_accum += total_upload_delta;
    state.total_sync += total_sync_delta;
    state.total_repair += total_repair_delta;
    state.total_recovery += total_recovery_delta;
    state.total_upload += total_upload_delta;

    // Epoch boundary: record duration and keep epoch-specific state.
    if epoch != state.prev_epoch {
        if state.prev_epoch != 0 {
            let dur_ms = state.epoch_start.elapsed().as_millis() as u64;
            push_capped(&mut state.epoch_duration_history, dur_ms);
            histogram.clear();
        }
        state.epoch_start = Instant::now();
        state.prev_epoch = epoch;
    }

    if now >= state.chart_next_update {
        for tracked in &mut state.nodes {
            push_capped(&mut tracked.event_history, tracked.node_event_accum);
            tracked.node_event_accum = 0;
        }

        let total_store: u64 = state
            .nodes
            .iter()
            .map(|n| n.ctx.store.inner().inner().total_size_bytes() as u64)
            .sum();

        push_capped(&mut state.sync_bw_history, state.sync_accum);
        push_capped(&mut state.repair_bw_history, state.repair_accum);
        push_capped(&mut state.recovery_bw_history, state.recovery_accum);
        push_capped(&mut state.upload_bw_history, state.upload_accum);
        push_capped(&mut state.total_store_history, total_store);

        state.sync_accum = 0;
        state.repair_accum = 0;
        state.recovery_accum = 0;
        state.upload_accum = 0;
        state.chart_next_update += CHART_UPDATE_INTERVAL;
    }

    let total_stake: u64 = node_snapshots.iter().map(|n| n.pool_stake).sum();

    let log = histogram.snapshot_top(20);

    let snap = PollSnapshot {
        slot,
        epoch,
        epoch_phase,
        epoch_phase_weight,
        committee_prev_size,
        committee_size,
        committee_next_size,
        tx_count: 0,
        runtime_secs: state.start.elapsed().as_secs_f64(),
        nodes: node_snapshots,
        spool_owners,
        spool_available,
        node_count: state.nodes.len(),
        epoch_duration_history: state.epoch_duration_history.clone(),
        total_store_history: state.total_store_history.clone(),
        repair_bw_history: state.repair_bw_history.clone(),
        recovery_bw_history: state.recovery_bw_history.clone(),
        sync_bw_history: state.sync_bw_history.clone(),
        upload_bw_history: state.upload_bw_history.clone(),
        total_sync_bytes: state.total_sync,
        total_repair_bytes: state.total_repair,
        total_recovery_bytes: state.total_recovery,
        total_upload_bytes: state.total_upload,
        total_stake,
        log,
        stake_fuzz_enabled: state.stake_fuzz_enabled,
        stake_fuzz_succeeded: state.stake_fuzz_succeeded,
        stake_fuzz_failed: state.stake_fuzz_failed,
        uploads_pending: state.track_pending,
        uploads_certified: state.track_certified,
        uploads_expired: state.track_expired,
        uploads_failed: state.track_failed,
        uploads_retries: state.uploads_retries,
        uploads_last_retry_error: state.uploads_last_retry_error.clone(),
        uploads_next_retry_in_ms: state.uploads_next_retry_in_ms,
        uploads_retry_in_progress: state.uploads_retry_in_progress,
        tracks: state.tracks.clone(),
    };

    snapshot.store(Arc::new(snap));
}

fn classify_tracks(
    epoch: u64,
    tracks: Vec<(solana_sdk::pubkey::Pubkey, Track)>,
    tape_expiry: HashMap<solana_sdk::pubkey::Pubkey, u64>,
) -> (
    Vec<TrackSnapshot>,
    u64,
    u64,
    u64,
    u64,
) {
    let mut ordered = tracks;
    ordered.sort_by_key(|(_, track)| track.id.as_u64());

    let mut pending = 0u64;
    let mut certified = 0u64;
    let mut expired = 0u64;
    let mut failed = 0u64;

    let result = ordered
        .into_iter()
        .map(|(_, track)| {
            let status = if track.data.is_invalidated() {
                failed += 1;
                TrackStatus::Failed
            } else if track.data.is_certified() {
                match tape_expiry.get(&track.tape) {
                    Some(expiry_epoch) if epoch >= *expiry_epoch => {
                        expired += 1;
                        TrackStatus::Expired
                    }
                    Some(_) => {
                        certified += 1;
                        TrackStatus::Certified
                    }
                    None => TrackStatus::Unknown,
                }
            } else if track.data.is_registered() {
                pending += 1;
                TrackStatus::Registered
            } else {
                TrackStatus::Unknown
            };
            TrackSnapshot {
                status,
            }
        })
        .collect::<Vec<_>>();

    (result, pending, certified, expired, failed)
}
