use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use peer_http::HttpApi;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use store_memory::MemoryStore;
use tape_core::erasure::GROUP_SIZE;
use tape_core::system::EpochPhase;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_node::context::NodeContext;
use tape_node::runtime::NodeRuntimeStatus;
use tape_store::ops::SpoolOps;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{info, warn};

use crate::app::{NodeSnapshot, PollSnapshot, SpoolSnapshot};
use crate::log_layer::LogHistogram;

/// Shared snapshot handle created in main and passed to both poller and TUI.
pub type SnapshotHandle = Arc<ArcSwap<PollSnapshot>>;

pub enum PollerUpdate {
    AddNode(
        usize,
        Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>,
        NodeRuntimeStatus,
    ),
    RemoveNode(usize),
    NodeHttpStatus {
        id: usize,
        healthy: bool,
    },
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
        running: u64,
        waiting_retry: u64,
        stalled: u64,
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

    #[cfg(test)]
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
    ctx: Arc<NodeContext<MemoryStore, HttpApi, LiteSvmRpc>>,
    runtime_status: NodeRuntimeStatus,
    prev_sync: u64,
    prev_repair: u64,
    prev_recovery: u64,
    prev_upload: u64,
    prev_events: u64,
    pool_stake: u64,
    http_healthy: Option<bool>,
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
    uploads_running: u64,
    uploads_waiting_retry: u64,
    uploads_stalled: u64,
    uploads_last_retry_error: Option<String>,
    uploads_next_retry_in_ms: Option<u64>,
    uploads_retry_in_progress: bool,
    chart_next_update: Instant,
}

const HISTORY_CAP: usize = 200;
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
        uploads_running: 0,
        uploads_waiting_retry: 0,
        uploads_stalled: 0,
        uploads_last_retry_error: None,
        uploads_next_retry_in_ms: None,
        uploads_retry_in_progress: false,
        chart_next_update: Instant::now() + CHART_UPDATE_INTERVAL,
    };

    let mut interval = time::interval(Duration::from_secs(1));

    loop {
                tokio::select! {
            _ = interval.tick() => {
                poll_once(&mut state, &snapshot, &histogram).await;
            }
            msg = update_rx.recv() => {
                match msg {
                    Some(PollerUpdate::AddNode(id, ctx, runtime_status)) => {
                        state.nodes.push(TrackedNode {
                            id,
                            ctx,
                            runtime_status,
                            prev_sync: 0,
                            prev_repair: 0,
                            prev_recovery: 0,
                            prev_upload: 0,
                            prev_events: 0,
                            pool_stake: 0,
                            http_healthy: None,
                            event_history: Vec::new(),
                            node_event_accum: 0,
                        });
                    }
                    Some(PollerUpdate::RemoveNode(id)) => {
                        state.nodes.retain(|n| n.id != id);
                    }
                    Some(PollerUpdate::NodeHttpStatus { id, healthy }) => {
                        if let Some(node) = state.nodes.iter_mut().find(|n| n.id == id) {
                            if node.http_healthy != Some(healthy) {
                                if healthy {
                                    info!(id, "node HTTP health recovered");
                                } else {
                                    warn!(id, "node HTTP health probe failed");
                                }
                            }
                            node.http_healthy = Some(healthy);
                        }
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
                        running,
                        waiting_retry,
                        stalled,
                        last_retry_error,
                        next_retry_in_ms,
                        retry_in_progress,
                    }) => {
                        state.uploads_pending = pending;
                        state.uploads_certified = certified;
                        state.uploads_expired = expired;
                        state.uploads_failed = failed;
                        state.uploads_retries = retries;
                        state.uploads_running = running;
                        state.uploads_waiting_retry = waiting_retry;
                        state.uploads_stalled = stalled;
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
    let system = state.rpc.get_system().await.ok();
    let current_epoch = system.map(|system| system.current_epoch);
    let epoch_account = match current_epoch {
        Some(epoch) => state.rpc.get_epoch(epoch).await.ok(),
        None => None,
    };
    let epoch = epoch_account.map(|e| e.id.as_u64()).unwrap_or(0);
    let (epoch_phase, epoch_phase_weight) = match &epoch_account {
        Some(e) => {
            let phase = match e.state.phase() {
                Some(EpochPhase::Sync) => "Sync",
                Some(EpochPhase::Settle) => "Settle",
                Some(EpochPhase::Snapshot) => "Snapshot",
                Some(EpochPhase::Active) => "Active",
                Some(EpochPhase::Closing) => "Closing",
                Some(EpochPhase::Completed) => "Completed",
                Some(EpochPhase::Unknown) | None => "?",
            };
            let weight = match e.state.phase() {
                Some(EpochPhase::Sync) => Some(e.state.synced_count),
                Some(EpochPhase::Settle) => Some(e.state.settled_count),
                _ => None,
            };
            (phase.to_string(), weight)
        }
        None => (String::new(), None),
    };
    let now = Instant::now();

    let mut previous_committee_size = 0;
    let mut current_committee_size = 0;
    let mut next_committee_size = 0;
    let mut target_group_count = 0;
    let mut live_group_count = 0;
    let mut spools = Vec::new();

    if let Some(system) = system {
        target_group_count = system.target_group_count;
        live_group_count = system.live_group_count;

        if let Ok(members) = state.rpc.get_committee(system.current_epoch).await {
            current_committee_size = members.len();
        }
        if let Ok(members) = state
            .rpc
            .get_committee(system.current_epoch.saturating_sub(EpochNumber(1)))
            .await
        {
            previous_committee_size = members.len();
        }
        if let Ok(members) = state
            .rpc
            .get_committee(system.current_epoch + EpochNumber(1))
            .await
        {
            next_committee_size = members.len();
        }

        let node_ids_by_address: HashMap<Address, usize> = state
            .nodes
            .iter()
            .map(|node| (node.ctx.node_address(), node.id))
            .collect();

        let total_spools = usize::try_from(live_group_count)
            .ok()
            .and_then(|groups| groups.checked_mul(GROUP_SIZE))
            .unwrap_or(0);
        spools = vec![SpoolSnapshot::default(); total_spools];

        if let Ok(groups) = state
            .rpc
            .get_groups(system.current_epoch, live_group_count)
            .await
        {
            for group in groups {
                let group_id = group.id;
                for (position, spool) in group.spools.iter().enumerate() {
                    let index = group_id.spool_at(position).as_usize();
                    if index >= spools.len() {
                        continue;
                    }
                    spools[index].owner = node_ids_by_address.get(&spool.node).copied();
                }
            }
        }
    }

    // Build per-spool availability: a spool is available if ANY node has it Active.
    // Multiple nodes may have the same spool (Active on the current owner,
    // LockedToMove on the former owner), so we must not let a non-Active
    // status overwrite an Active one.
    for tracked in &state.nodes {
        if !tracked.runtime_status.is_running() {
            continue;
        }

        if let Ok(local_spools) = tracked.ctx.store.iter_all_spools() {
            for (spool_id, spool_state) in local_spools {
                let index = spool_id.as_usize();
                if let Some(spool) = spools.get_mut(index) {
                    if spool_state.is_active() {
                        spool.available = true;
                    }
                }
            }
        }
    }

    let mut total_sync_delta = 0u64;
    let mut total_repair_delta = 0u64;
    let mut total_recovery_delta = 0u64;
    let mut total_upload_delta = 0u64;
    let mut running_node_count = 0usize;

    let mut node_snapshots = Vec::with_capacity(state.nodes.len());

    for tracked in &mut state.nodes {
        let is_running = tracked.runtime_status.is_running();
        if is_running {
            running_node_count += 1;
        }

        let metrics = tracked.ctx.metrics.snapshot();
        let sync = metrics.sync_bytes_fetched;
        let repair = metrics.repair_bytes_fetched;
        let recovery = metrics.recover_bytes_fetched;
        let upload = metrics.bytes_uploaded;

        let sync_delta = sync.saturating_sub(tracked.prev_sync);
        let repair_delta = repair.saturating_sub(tracked.prev_repair);
        let recovery_delta = recovery.saturating_sub(tracked.prev_recovery);
        let upload_delta = upload.saturating_sub(tracked.prev_upload);
        let events = metrics.events_total;
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

        let protocol = tracked.ctx.state();
        let spool_count = tracked.ctx.my_spools().len();
        let node_status = if is_running && !protocol.epoch().is_zero() {
            Some(tracked.ctx.node_status())
        } else {
            None
        };

        let authority = tracked.ctx.pubkey().address();
        if let Ok(node) = tracked.ctx.rpc.get_node(&authority).await {
            tracked.pool_stake = node.pool.stake.as_u64();
        }

        let mut ns = NodeSnapshot {
            id: tracked.id,
            is_running,
            http_healthy: if is_running { tracked.http_healthy } else { None },
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

    let tracked_node_count = state.nodes.len();
    let dead_node_count = tracked_node_count.saturating_sub(running_node_count);
    let http_unhealthy_count = node_snapshots
        .iter()
        .filter(|node| node.is_running && matches!(node.http_healthy, Some(false)))
        .count();

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
        previous_committee_size,
        current_committee_size,
        next_committee_size,
        target_group_count,
        live_group_count,
        tx_count: 0,
        runtime_secs: state.start.elapsed().as_secs_f64(),
        nodes: node_snapshots,
        spools,
        node_count: running_node_count,
        tracked_node_count,
        dead_node_count,
        http_unhealthy_count,
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
        uploads_pending: state.uploads_pending,
        uploads_certified: state.uploads_certified,
        uploads_expired: state.uploads_expired,
        uploads_failed: state.uploads_failed,
        uploads_retries: state.uploads_retries,
        uploads_running: state.uploads_running,
        uploads_waiting_retry: state.uploads_waiting_retry,
        uploads_stalled: state.uploads_stalled,
        uploads_last_retry_error: state.uploads_last_retry_error.clone(),
        uploads_next_retry_in_ms: state.uploads_next_retry_in_ms,
        uploads_retry_in_progress: state.uploads_retry_in_progress,
    };

    snapshot.store(Arc::new(snap));
}
