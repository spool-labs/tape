use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use tape_core::erasure::SPOOL_COUNT;
use tape_node::core::NodeContext;
use tape_store::MemoryStore;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};

use crate::app::{NodeSnapshot, PollSnapshot};
use crate::log_layer::LogHistogram;

/// Shared snapshot handle created in main and passed to both poller and TUI.
pub type SnapshotHandle = Arc<ArcSwap<PollSnapshot>>;

pub enum PollerUpdate {
    AddNode(usize, Arc<NodeContext<MemoryStore, LiteSvmRpc>>),
    RemoveNode(usize),
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
    ctx: Arc<NodeContext<MemoryStore, LiteSvmRpc>>,
    prev_sync: u64,
    prev_repair: u64,
    prev_upload: u64,
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
    sync_bw_history: Vec<u64>,
    upload_bw_history: Vec<u64>,
}

const HISTORY_CAP: usize = 200;

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
        sync_bw_history: Vec::new(),
        upload_bw_history: Vec::new(),
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
                            prev_upload: 0,
                        });
                    }
                    Some(PollerUpdate::RemoveNode(id)) => {
                        state.nodes.retain(|n| n.id != id);
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
    let epoch = state
        .rpc
        .get_epoch()
        .await
        .map(|e| e.id.as_u64())
        .unwrap_or(0);

    let mut spool_owners = [0u8; SPOOL_COUNT];
    if let Ok(system) = state.rpc.get_system().await {
        for (i, owner) in system.spools.0.iter().enumerate() {
            if i < SPOOL_COUNT {
                spool_owners[i] = *owner;
            }
        }
    }

    let mut total_sync_delta = 0u64;
    let mut total_repair_delta = 0u64;
    let mut total_upload_delta = 0u64;

    let mut node_snapshots = Vec::with_capacity(state.nodes.len());

    for tracked in &mut state.nodes {
        let sync = tracked.ctx.stats.sync_bytes_received.load(Ordering::Relaxed);
        let repair = tracked.ctx.stats.repair_bytes_received.load(Ordering::Relaxed);
        let upload = tracked.ctx.stats.bytes_uploaded.load(Ordering::Relaxed);

        let sync_delta = sync.saturating_sub(tracked.prev_sync);
        let repair_delta = repair.saturating_sub(tracked.prev_repair);
        let upload_delta = upload.saturating_sub(tracked.prev_upload);

        tracked.prev_sync = sync;
        tracked.prev_repair = repair;
        tracked.prev_upload = upload;

        total_sync_delta += sync_delta;
        total_repair_delta += repair_delta;
        total_upload_delta += upload_delta;

        let cs = tracked.ctx.chain_state.load();
        let spool_count = cs.spools.len();
        let node_status = if cs.has_epoch() {
            Some(cs.node_status.clone())
        } else {
            None
        };

        let mut ns = NodeSnapshot {
            id: tracked.id,
            sync_bytes: sync,
            repair_bytes: repair,
            upload_bytes: upload,
            spool_count,
            node_status,
            sync_bw_history: Vec::new(),
        };

        // We'll just store the delta as the latest bw for the sparkline
        ns.sync_bw_history.push(sync_delta);
        node_snapshots.push(ns);
    }

    push_capped(&mut state.sync_bw_history, total_sync_delta);
    push_capped(&mut state.repair_bw_history, total_repair_delta);
    push_capped(&mut state.upload_bw_history, total_upload_delta);

    // Epoch duration: record on epoch change, clear histogram
    if epoch != state.prev_epoch {
        if state.prev_epoch != 0 {
            let dur_ms = state.epoch_start.elapsed().as_millis() as u64;
            push_capped(&mut state.epoch_duration_history, dur_ms);
            histogram.clear();
        }
        state.epoch_start = Instant::now();
        state.prev_epoch = epoch;
    }

    // Total store size across all tracked nodes
    let total_store: u64 = state
        .nodes
        .iter()
        .map(|n| n.ctx.store.inner().inner().total_size_bytes() as u64)
        .sum();
    push_capped(&mut state.total_store_history, total_store);

    let log = histogram.snapshot_top(20);

    let snap = PollSnapshot {
        slot,
        epoch,
        tx_count: 0,
        runtime_secs: state.start.elapsed().as_secs_f64(),
        nodes: node_snapshots,
        spool_owners,
        node_count: state.nodes.len(),
        epoch_duration_history: state.epoch_duration_history.clone(),
        total_store_history: state.total_store_history.clone(),
        repair_bw_history: state.repair_bw_history.clone(),
        sync_bw_history: state.sync_bw_history.clone(),
        upload_bw_history: state.upload_bw_history.clone(),
        log,
    };

    snapshot.store(Arc::new(snap));
}
