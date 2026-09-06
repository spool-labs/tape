use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use rpc::Rpc;
use store::Store;
use tape_core::prelude::NodeStatus;
use tape_metrics::prometheus::core::{Collector, Desc};
use tape_metrics::prometheus::proto::MetricFamily;
use tape_metrics::prometheus::{IntGauge, IntGaugeVec, Opts};
use tape_protocol::Api;
use tape_store::TapeStore;

use crate::context::NodeContext;

pub type CapacityFn = Box<dyn Fn() -> (usize, usize) + Send + Sync>;

const DISK_USED: (&str, &str) = ("tape_store_disk_used_bytes", "On-disk size of live store data");
const DISK_AVAILABLE: (&str, &str) = ("tape_store_disk_available_bytes", "Free disk space on the store volume");
const CHANNEL_DEPTH: (&str, &str) = ("tape_node_channel_depth", "Queued items in internal channels");
const NODE_STATUS: (&str, &str) = ("tape_node_status", "1 when active in committee, else 0");
const SHARDS_OWNED: (&str, &str) = ("tape_node_shards_owned", "Spools currently owned by this node");
const INGEST_TIP: (&str, &str) = ("tape_node_ingest_tip_slot", "Last known confirmed tip slot");
const INGEST_LAG: (&str, &str) = ("tape_node_ingest_lag_slots", "Slots between tip and last dispatched");
const EPOCH: (&str, &str) = ("tape_node_epoch", "Current committee epoch number");
const EPOCH_PHASE: (&str, &str) = ("tape_node_epoch_phase", "Current epoch phase (EpochPhase discriminant)");
const COMMITTEE_SIZE: (&str, &str) = ("tape_node_committee_size", "Members in the current committee");
const GROUPS_TOTAL: (&str, &str) = ("tape_node_groups_total", "Spool groups in the current epoch");
const PEERS_TOTAL: (&str, &str) = ("tape_node_peers_total", "Known peers in the directory");
const PEER_CAPACITY: (&str, &str) = ("tape_node_peer_capacity", "Configured peer directory capacity");
const EPOCH_SYNCED: (&str, &str) = ("tape_node_epoch_synced_groups", "Groups past the sync threshold this epoch");
const FEE_PAYER_BALANCE: (&str, &str) = ("tape_node_fee_payer_lamports", "Fee payer balance sampled by the balance monitor");

fn int_gauge((name, help): (&str, &str)) -> IntGauge {
    IntGauge::new(name, help).expect("int gauge")
}

/// Samples cheap store size figures on scrape.
pub struct StoreStatsCollector<Db: Store> {
    store: Arc<TapeStore<Db>>,
    used: IntGauge,
    available: IntGauge,
}

impl<Db: Store> StoreStatsCollector<Db> {
    pub fn new(store: Arc<TapeStore<Db>>) -> Self {
        Self {
            store,
            used: int_gauge(DISK_USED),
            available: int_gauge(DISK_AVAILABLE),
        }
    }
}

impl<Db: Store + 'static> Collector for StoreStatsCollector<Db> {
    fn desc(&self) -> Vec<&Desc> {
        self.used.desc().into_iter().chain(self.available.desc()).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let backend = self.store.inner().inner();
        let mut families = Vec::with_capacity(2);
        if let Ok(Some(used)) = backend.live_data_size_bytes() {
            self.used.set(used as i64);
            families.extend(self.used.collect());
        }
        if let Ok(Some(available)) = backend.available_disk_bytes() {
            self.available.set(available as i64);
            families.extend(self.available.collect());
        }
        families
    }
}

/// Reports queue depth of the internal block/replay channels.
pub struct ChannelCollector {
    channels: Vec<(&'static str, CapacityFn)>,
    depth: IntGaugeVec,
}

impl ChannelCollector {
    pub fn new(channels: Vec<(&'static str, CapacityFn)>) -> Self {
        let depth = IntGaugeVec::new(Opts::new(CHANNEL_DEPTH.0, CHANNEL_DEPTH.1), &["channel"])
            .expect("channel depth gauge vec");
        Self { channels, depth }
    }
}

impl Collector for ChannelCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.depth.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        for (name, capacity) in &self.channels {
            let (available, max) = capacity();
            self.depth.with_label_values(&[name]).set(max.saturating_sub(available) as i64);
        }
        self.depth.collect()
    }
}

/// Reports committee status, shard ownership, epoch, and ingest lag on scrape.
pub struct NodeStatusCollector<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    status: IntGauge,
    shards: IntGauge,
    tip: IntGauge,
    lag: IntGauge,
    epoch: IntGauge,
    phase: IntGauge,
    committee: IntGauge,
    groups: IntGauge,
    peers: IntGauge,
    peer_capacity: IntGauge,
    synced: IntGauge,
    fee_payer: IntGauge,
}

impl<Db: Store, Cluster: Api, Blockchain: Rpc> NodeStatusCollector<Db, Cluster, Blockchain> {
    pub fn new(context: Arc<NodeContext<Db, Cluster, Blockchain>>) -> Self {
        Self {
            context,
            status: int_gauge(NODE_STATUS),
            shards: int_gauge(SHARDS_OWNED),
            tip: int_gauge(INGEST_TIP),
            lag: int_gauge(INGEST_LAG),
            epoch: int_gauge(EPOCH),
            phase: int_gauge(EPOCH_PHASE),
            committee: int_gauge(COMMITTEE_SIZE),
            groups: int_gauge(GROUPS_TOTAL),
            peers: int_gauge(PEERS_TOTAL),
            peer_capacity: int_gauge(PEER_CAPACITY),
            synced: int_gauge(EPOCH_SYNCED),
            fee_payer: int_gauge(FEE_PAYER_BALANCE),
        }
    }

    fn gauges(&self) -> [&IntGauge; 11] {
        [
            &self.status,
            &self.shards,
            &self.tip,
            &self.lag,
            &self.epoch,
            &self.phase,
            &self.committee,
            &self.groups,
            &self.peers,
            &self.peer_capacity,
            &self.synced,
        ]
    }
}

impl<Db: Store + 'static, Cluster: Api + 'static, Blockchain: Rpc + 'static> Collector
    for NodeStatusCollector<Db, Cluster, Blockchain>
{
    fn desc(&self) -> Vec<&Desc> {
        // The fee payer gauge is described but only collected once a sample
        // lands, so it stays out of the array the collect path walks
        // unconditionally. Folding it in would export a zero that reads as a
        // node with no money.
        self.gauges()
            .into_iter()
            .chain([&self.fee_payer])
            .flat_map(|g| g.desc())
            .collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let state = self.context.state();
        let active = matches!(self.context.node_status(), NodeStatus::Active);
        let status = i64::from(active);
        if active {
            note_first_active(self.context.node_id().0);
        }
        let shards = self.context.my_spools().len() as i64;
        let (tip, _, lag) = self.context.ingest.progress().tip_and_lag();
        let (tip, lag) = (tip as i64, lag as i64);

        self.status.set(status);
        self.shards.set(shards);
        self.tip.set(tip);
        self.lag.set(lag);
        super::epoch::sample_lag(lag as u64);
        self.epoch.set(state.epoch().0 as i64);
        self.phase.set(u64::from(state.phase()) as i64);
        self.committee.set(state.current.committee.len() as i64);
        self.groups.set(state.current.groups.len() as i64);
        self.peers.set(state.peers.len() as i64);
        self.peer_capacity.set(state.peer_capacity as i64);
        self.synced.set(state.current.epoch.state.synced_count as i64);

        let mut families: Vec<MetricFamily> =
            self.gauges().into_iter().flat_map(|g| g.collect()).collect();

        // Only exported once sampled, so a process without a balance monitor
        // (the gateway) never reports a zero balance that reads as broke.
        if let Some(balance) = self.context.fee_payer_balance() {
            self.fee_payer.set(balance.0 as i64);
            families.extend(self.fee_payer.collect());
        }

        families
    }
}

/// When this process began, for measuring how long a restart takes to matter.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Logged once: wall time from process start to first sitting in committee,
/// which is what an operator means by "how long until the node is back".
static ANNOUNCED_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Call at startup so the elapsed figure counts from process start, not first scrape.
pub fn mark_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

fn note_first_active(node_id: u64) {
    if ANNOUNCED_ACTIVE.swap(true, Ordering::Relaxed) {
        return;
    }
    let since_start = PROCESS_START.get().map(|t| t.elapsed().as_millis()).unwrap_or_default();
    tracing::info!(node_id, active_after_ms = since_start, "node: active in committee");
}
