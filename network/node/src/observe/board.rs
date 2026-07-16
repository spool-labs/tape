//! Builds a node's board from live context and the metric set.

use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rpc::Rpc;
use store::{Column, Store, StoreVolume};
use tape_core::system::NodeStatus;
use tape_metrics::prometheus::proto::{Histogram, MetricFamily};
use tape_store::columns::{ObjectInfoCol, TapeCol, TrackCol};
use tape_store::ops::{SliceOps, SpoolOps};
use tape_observe_api::{
    phase_name, BootstrapInfo, Bucket, CacheStats, Board, ChainStats, DecodeStats, EpochInfo,
    HttpStats, IngestInfo, Labeled, LinkStatus, NetworkNode, Network, NetworkSpool, NodeInfo,
    NodeStats, ResourceInfo, SpoolStat, StatsSource, StorageContents, StorageInfo, StorageVolume,
    StoreIo, ThroughputTotals, CACHE_RESULTS, DECODE_RESULTS, DECODE_SLICE_OUTCOMES, SPOOL_OPS,
    SPOOL_STAGES,
};
use tape_protocol::Api;

use crate::context::NodeContext;

static STARTED: OnceLock<Instant> = OnceLock::new();

/// Stamp the process start so the board can report uptime.
pub fn init() {
    let _ = STARTED.get_or_init(Instant::now);
}

/// Fold one histogram's bucket counts into the running totals, seeding the
/// bucket boundaries on the first call.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn accumulate_buckets(h: &Histogram, les: &mut Vec<f64>, sums: &mut Vec<u64>) {
    let buckets = h.get_bucket();
    if sums.is_empty() {
        *les = buckets.iter().map(|b| b.get_upper_bound()).collect();
        *sums = vec![0u64; buckets.len()];
    }
    for (i, b) in buckets.iter().enumerate() {
        if let Some(s) = sums.get_mut(i) {
            *s += b.get_cumulative_count();
        }
    }
}

/// Pair accumulated bucket boundaries with their cumulative counts.
pub(super) fn to_buckets(les: &[f64], sums: &[u64]) -> Vec<Bucket> {
    les.iter().zip(sums).map(|(le, c)| Bucket { le_secs: *le, count: *c }).collect()
}

/// Sum of a counter family across all label combinations.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn counter_sum(family: &MetricFamily) -> u64 {
    family.get_metric().iter().map(|metric| metric.get_counter().value() as u64).sum()
}

/// Largest gauge value across a family's label combinations.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn gauge_max(family: &MetricFamily) -> u64 {
    family.get_metric().iter().map(|metric| metric.get_gauge().value() as u64).max().unwrap_or(0)
}

/// Split the rpc error family into general errors and transaction errors.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn split_rpc_errors(family: &MetricFamily) -> (u64, u64) {
    let mut rpc = 0;
    let mut tx = 0;
    for metric in family.get_metric() {
        let count = metric.get_counter().value() as u64;
        let is_tx_error = metric
            .get_label()
            .iter()
            .any(|label| label.get_name() == "error_type" && label.get_value() == "tx_error");
        if is_tx_error {
            tx += count;
        } else {
            rpc += count;
        }
    }
    (rpc, tx)
}

/// One named histogram family folded into flat buckets and a sample count.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn histogram_snapshot(families: &[MetricFamily], name: &str) -> (Vec<Bucket>, u64) {
    let mut les: Vec<f64> = Vec::new();
    let mut sums: Vec<u64> = Vec::new();
    let mut total = 0;
    for family in families.iter().filter(|family| family.get_name() == name) {
        for metric in family.get_metric() {
            let h = metric.get_histogram();
            total += h.get_sample_count();
            accumulate_buckets(h, &mut les, &mut sums);
        }
    }
    (to_buckets(&les, &sums), total)
}

/// Sum of one named counter family, zero when absent.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn family_counter(families: &[MetricFamily], name: &str) -> u64 {
    families.iter().filter(|family| family.get_name() == name).map(counter_sum).sum()
}

/// Largest gauge of one named family, zero when absent.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
pub(super) fn family_gauge(families: &[MetricFamily], name: &str) -> u64 {
    families.iter().filter(|family| family.get_name() == name).map(gauge_max).max().unwrap_or(0)
}

/// Fold one request-duration histogram family into buckets, per-status and
/// optional per-route totals, and a bytes counter.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
fn request_stats(
    families: &[MetricFamily],
    duration_family: &str,
    status_label: &str,
    route_label: Option<&str>,
    bytes_family: &str,
) -> HttpStats {
    let mut les: Vec<f64> = Vec::new();
    let mut sums: Vec<u64> = Vec::new();
    let mut by_status: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut by_route: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut total: u64 = 0;

    for family in families.iter().filter(|family| family.get_name() == duration_family) {
        for metric in family.get_metric() {
            let h = metric.get_histogram();
            total += h.get_sample_count();
            let label_value = |name: &str| {
                metric
                    .get_label()
                    .iter()
                    .find(|label| label.get_name() == name)
                    .map(|label| label.get_value().to_string())
            };
            let status = label_value(status_label).unwrap_or_else(|| "other".into());
            *by_status.entry(status).or_default() += h.get_sample_count();
            if let Some(route_label) = route_label {
                let route = label_value(route_label).unwrap_or_else(|| "unknown".into());
                *by_route.entry(route).or_default() += h.get_sample_count();
            }
            accumulate_buckets(h, &mut les, &mut sums);
        }
    }

    HttpStats {
        buckets: to_buckets(&les, &sums),
        by_status: by_status.into_iter().map(|(label, value)| Labeled { label, value }).collect(),
        by_route: by_route.into_iter().map(|(label, value)| Labeled { label, value }).collect(),
        total,
        response_bytes: family_counter(families, bytes_family),
    }
}

/// This node's own serving stats.
fn http_stats(families: &[MetricFamily]) -> HttpStats {
    request_stats(
        families,
        "tape_http_request_duration_seconds",
        "status_class",
        Some("route"),
        "tape_http_response_bytes_total",
    )
}

/// Aggregate Solana RPC and transaction-submission metrics into chain health.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
fn chain_stats(families: &[MetricFamily]) -> ChainStats {
    let mut c = ChainStats::default();
    for family in families {
        match family.get_name() {
            "rpc_requests_total" => c.rpc_total += counter_sum(family),
            "rpc_errors_total" => {
                let (rpc, tx) = split_rpc_errors(family);
                c.rpc_errors += rpc;
                c.tx_errors += tx;
            }
            "rpc_current_endpoint" => c.endpoint = gauge_max(family),
            "rpc_endpoints_configured" => c.endpoints = gauge_max(family),
            "tape_client_transactions_total" => c.tx_total += counter_sum(family),
            _ => {}
        }
    }
    let (rpc_buckets, rpc_latency_total) = histogram_snapshot(families, "rpc_request_duration_seconds");
    c.rpc_buckets = rpc_buckets;
    c.rpc_latency_total = rpc_latency_total;
    let (confirm_buckets, confirm_total) =
        histogram_snapshot(families, "tape_client_transaction_confirmation_duration_seconds");
    c.confirm_buckets = confirm_buckets;
    c.confirm_total = confirm_total;
    c
}

/// Process CPU, fd, and internal-queue figures from the gathered registry.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
fn resource_extras(families: &[MetricFamily]) -> (f64, u64, Vec<Labeled>) {
    let mut cpu = 0.0;
    let mut fds = 0;
    let mut queues = Vec::new();
    for fam in families {
        match fam.get_name() {
            "process_cpu_seconds_total" => {
                cpu = fam.get_metric().iter().map(|m| m.get_counter().value()).sum();
            }
            "process_open_fds" => {
                fds = fam.get_metric().iter().map(|m| m.get_gauge().value() as u64).sum();
            }
            "tape_node_channel_depth" => {
                for metric in fam.get_metric() {
                    let label = metric
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "channel")
                        .map(|l| l.get_value().to_string())
                        .unwrap_or_else(|| "unknown".into());
                    queues.push(Labeled { label, value: metric.get_gauge().value() as u64 });
                }
            }
            _ => {}
        }
    }
    queues.sort_by(|a, b| b.value.cmp(&a.value));
    (cpu, fds, queues)
}

/// Aggregate the object-decode duration histogram into cumulative buckets and a
/// total, for decode-latency quantiles.
fn decode_latency(families: &[MetricFamily]) -> (Vec<Bucket>, u64) {
    histogram_snapshot(families, "tape_gw_decode_duration_seconds")
}

/// This node's outbound calls to other nodes, as inter-node latency.
fn peer_stats(families: &[MetricFamily]) -> HttpStats {
    request_stats(
        families,
        "peer_client_request_duration_seconds",
        "status",
        None,
        "peer_client_bytes_received_total",
    )
}

/// Aggregate the store metrics from the gathered registry into store-engine
/// I/O figures.
#[allow(deprecated)] // prometheus proto getters are deprecated but stable
fn store_io_stats(families: &[MetricFamily]) -> StoreIo {
    let mut ops: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut io = StoreIo::default();
    let (mut get_sum, mut get_cnt, mut put_sum, mut put_cnt) = (0.0_f64, 0u64, 0.0_f64, 0u64);

    for fam in families {
        match fam.get_name() {
            "tape_store_operations_total" => {
                for m in fam.get_metric() {
                    let op = m
                        .get_label()
                        .iter()
                        .find(|l| l.get_name() == "operation")
                        .map(|l| l.get_value().to_string())
                        .unwrap_or_else(|| "other".into());
                    let v = m.get_counter().value() as u64;
                    *ops.entry(op).or_default() += v;
                    io.total_ops += v;
                }
            }
            "tape_store_bytes_read_total" => io.bytes_read += counter_sum(fam),
            "tape_store_bytes_written_total" => io.bytes_written += counter_sum(fam),
            "tape_store_errors_total" => io.errors += counter_sum(fam),
            "tape_store_get_duration_seconds" => {
                for m in fam.get_metric() {
                    let h = m.get_histogram();
                    get_sum += h.get_sample_sum();
                    get_cnt += h.get_sample_count();
                }
            }
            "tape_store_put_duration_seconds" => {
                for m in fam.get_metric() {
                    let h = m.get_histogram();
                    put_sum += h.get_sample_sum();
                    put_cnt += h.get_sample_count();
                }
            }
            _ => {}
        }
    }

    io.ops = ops.into_iter().map(|(label, value)| Labeled { label, value }).collect();
    io.get_avg_ms = if get_cnt > 0 { get_sum / get_cnt as f64 * 1000.0 } else { 0.0 };
    io.put_avg_ms = if put_cnt > 0 { put_sum / put_cnt as f64 * 1000.0 } else { 0.0 };
    io
}

/// Real slice count across the node's spools. The RocksDB key estimate is
/// unreliable for the blob-backed slice column, so count keys directly, but
/// cache the result: the count is a full key scan and the board polls hot.
fn stored_slices<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> u64
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    const SLICE_COUNT_TTL: std::time::Duration = std::time::Duration::from_secs(30);
    static CACHE: Mutex<Option<(Instant, u64)>> = Mutex::new(None);
    static REFRESHING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    fn refresh<Db: Store>(store: &tape_store::TapeStore<Db>) -> u64 {
        let count = store
            .iter_all_spools()
            .map(|spools| {
                spools
                    .iter()
                    .map(|(id, _)| store.count_slices_by_spool(*id).unwrap_or(0) as u64)
                    .sum()
            })
            .unwrap_or(0);
        if let Ok(mut cache) = CACHE.lock() {
            *cache = Some((Instant::now(), count));
        }
        count
    }

    // Serve the cached count and refresh it off the request path: the count is
    // a full key scan and the board polls hot.
    if let Some((at, count)) = CACHE.lock().ok().and_then(|cache| *cache) {
        if at.elapsed() >= SLICE_COUNT_TTL
            && !REFRESHING.swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            let store = context.store.clone();
            tokio::task::spawn_blocking(move || {
                refresh(&store);
                REFRESHING.store(false, std::sync::atomic::Ordering::Release);
            });
        }
        return count;
    }

    refresh(&context.store)
}

/// The local node's own stats for the network table, from cheap estimates only.
fn local_stats<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> NodeStats
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    let backend = context.store.inner().inner();
    let estimate = |cf: &str| backend.key_count_estimate(cf).ok().flatten().unwrap_or(0);

    let (_, _, ingest_lag_slots) = context.ingest.progress().tip_and_lag();

    let bootstrap = context.bootstrap.snapshot();
    let bootstrap_ready = context.bootstrap.is_ready();

    let volumes = backend.disk_volumes().unwrap_or_default();
    let store_disk_bytes = volumes.iter().map(|v| v.used_bytes).sum();
    let slice_payload_bytes = volumes
        .iter()
        .find(|v| matches!(v.volume, StoreVolume::Bulk))
        .map(|v| v.used_bytes)
        .unwrap_or(0);

    NodeStats {
        version: crate::VERSION.to_string(),
        owned_spools: context.my_spools().len() as u64,
        tracks_stored: estimate(TrackCol::CF_NAME),
        slices_stored: stored_slices(context),
        slice_payload_bytes,
        store_disk_bytes,
        free_disk_bytes: backend.available_disk_bytes().ok().flatten().unwrap_or(0),
        current_epoch: context.state().epoch().0,
        ingest_state: context.ingest_state().label().to_string(),
        ingest_lag_slots,
        reclaim_pending: context.is_reclaim_pending(),
        blocks_processed: tape_metrics::metrics().blocks_processed_total.get(),
        bootstrap_ready,
        bootstrap_behind_slots: if bootstrap_ready {
            0
        } else {
            bootstrap.target_slot.saturating_sub(bootstrap.current_slot)
        },
    }
}

/// A lite board synthesized from a node's public stats, for peers that don't
/// serve the full observe board.
pub fn lite_board(address: String, stats: &NodeStats) -> Board {
    Board {
        source: StatsSource::Public,
        node: NodeInfo {
            address,
            status: "active".to_string(),
            version: stats.version.clone(),
            uptime_secs: 0,
        },
        epoch: EpochInfo {
            number: stats.current_epoch,
            shards_owned: stats.owned_spools,
            ..Default::default()
        },
        ingest: IngestInfo {
            lag_slots: stats.ingest_lag_slots,
            state: stats.ingest_state.clone(),
            at_tip: stats.bootstrap_ready && stats.ingest_state == "at_tip",
            ..Default::default()
        },
        bootstrap: BootstrapInfo {
            ready: stats.bootstrap_ready,
            target_slot: stats.bootstrap_behind_slots,
            ..Default::default()
        },
        storage: StorageInfo {
            disk_used_bytes: stats.store_disk_bytes,
            disk_free_bytes: stats.free_disk_bytes,
            owned_spools: stats.owned_spools,
            volumes: Vec::new(),
        },
        contents: StorageContents {
            tracks: stats.tracks_stored,
            slices: stats.slices_stored,
            ..Default::default()
        },
        throughput: ThroughputTotals {
            blocks_processed: stats.blocks_processed,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Build the whole-network view from this node's on-chain state: the committee
/// and every spool's owner.
pub fn build_network<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Network
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    let state = context.state();
    let me = context.node_address();

    // Identify ourselves; everyone else takes status/source/stats from the latest
    // aggregator probe, falling back to Unknown when the aggregator is off or stale.
    let liveness = |node: tape_crypto::Address| {
        if node == me {
            (LinkStatus::Local, StatsSource::Observe, Some(local_stats(context)))
        } else {
            super::peers::lookup(node).unwrap_or((LinkStatus::Unknown, StatsSource::None, None))
        }
    };
    let name_of = |node: tape_crypto::Address| {
        context.peer_manager.get(node).map(|p| tape_api::utils::from_name(&p.name)).unwrap_or_default()
    };

    // Committee members first, keeping their committee index; then every other
    // registered node so operators that opted out of the committee still show.
    let in_committee: std::collections::HashSet<_> =
        state.current.committee.iter().map(|m| m.node).collect();
    let mut committee: Vec<NetworkNode> = state
        .current
        .committee
        .iter()
        .enumerate()
        .map(|(index, m)| {
            let (status, source, stats) = liveness(m.node);
            NetworkNode {
                index,
                address: m.node.to_string(),
                name: name_of(m.node),
                spools: m.spools,
                status,
                source,
                non_committee: false,
                endpoint: None,
                stake: Some(m.stake.as_u64()),
                stats,
            }
        })
        .collect();

    let mut extras: Vec<_> = context
        .peer_manager
        .all()
        .into_iter()
        .filter(|p| !in_committee.contains(&p.node))
        .collect();
    extras.sort_by_key(|p| p.node.to_string());
    for (offset, p) in extras.into_iter().enumerate() {
        let (status, source, stats) = liveness(p.node);
        committee.push(NetworkNode {
            index: in_committee.len() + offset,
            address: p.node.to_string(),
            name: tape_api::utils::from_name(&p.name),
            spools: 0,
            status,
            source,
            non_committee: true,
            endpoint: None,
            stake: Some(p.stake.as_u64()),
            stats,
        });
    }

    let idx: std::collections::HashMap<String, usize> =
        committee.iter().map(|m| (m.address.clone(), m.index)).collect();

    let mut spools: Vec<NetworkSpool> = Vec::new();
    for group in &state.current.groups {
        for (spool, owner) in state.group_peers(group.id) {
            let owner = owner.to_string();
            let owner_index = idx.get(&owner).copied();
            spools.push(NetworkSpool { spool: spool.0, owner: Some(owner), owner_index });
        }
    }
    spools.sort_by_key(|s| s.spool);

    let (slot, _, _) = context.ingest.progress().tip_and_lag();

    Network {
        epoch: state.epoch().0,
        phase: phase_name(u64::from(state.phase()) as u8).to_string(),
        phase_index: u64::from(state.phase()) as u8,
        slot,
        groups: state.current.groups.len() as u64,
        prev_committee_size: state
            .previous
            .as_ref()
            .map(|p| p.committee.len() as u64)
            .unwrap_or(0),
        committee_size: state.current.committee.len() as u64,
        next_committee_size: state.next_committee.as_ref().map(|c| c.len() as u64).unwrap_or(0),
        peers: state.peers.len() as u64,
        committee,
        spools,
    }
}

fn node_status_label(status: &NodeStatus) -> &'static str {
    match status {
        NodeStatus::Standby => "standby",
        NodeStatus::Active => "active",
        NodeStatus::RecoverMetadata => "recover_metadata",
        NodeStatus::RecoveryReplay => "recovery_replay",
        NodeStatus::RecoveryInProgress { .. } => "recovering",
        NodeStatus::PartialReplay { .. } => "partial_replay",
    }
}

/// Assemble one board. All reads are cheap in-memory state and a couple of
/// RocksDB property reads, so this is safe to poll every second.
pub fn build<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Board
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    let m = tape_metrics::metrics();
    // Walk the global registry once; peer_stats + store_io_stats both read it.
    let gathered = tape_metrics::prometheus::gather();
    let state = context.state();
    let backend = context.store.inner().inner();

    let (tip_slot, dispatched, lag_slots) = context.ingest.progress().tip_and_lag();
    let bootstrap = context.bootstrap.snapshot();
    let owned_spool_count = context.my_spools().len() as u64;

    let current_epoch = super::current_epoch_progress(state.epoch().0, &gathered);

    let labeled = |series: &[&str], read: &dyn Fn(&str) -> u64| -> Vec<Labeled> {
        series
            .iter()
            .copied()
            .map(|label| Labeled { label: label.to_string(), value: read(label) })
            .collect()
    };

    let mut spool = Vec::with_capacity(SPOOL_OPS.len() * SPOOL_STAGES.len());
    for &op in SPOOL_OPS {
        for &stage in SPOOL_STAGES {
            spool.push(SpoolStat {
                op: op.to_string(),
                stage: stage.to_string(),
                bytes: m.spool_bytes_total.with_label_values(&[op, stage]).get(),
            });
        }
    }

    Board {
        source: StatsSource::Observe,
        kind: super::board_kind(),
        generated_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        node: NodeInfo {
            address: context.node_address().to_string(),
            status: node_status_label(&context.node_status()).to_string(),
            version: crate::VERSION.to_string(),
            uptime_secs: STARTED.get().map(|s| s.elapsed().as_secs()).unwrap_or(0),
        },
        epoch: EpochInfo {
            number: state.epoch().0,
            phase: phase_name(u64::from(state.phase()) as u8).to_string(),
            phase_index: u64::from(state.phase()) as u8,
            synced_count: state.current.epoch.state.synced_count,
            committee_size: state.current.committee.len() as u64,
            groups: state.current.groups.len() as u64,
            peers: state.peers.len() as u64,
            peer_capacity: state.peer_capacity,
            shards_owned: owned_spool_count,
            next_epoch: state.next_epoch.as_ref().map(|e| e.id.0),
            next_committee_size: state.next_committee.as_ref().map(|c| c.len() as u64),
        },
        ingest: IngestInfo {
            tip_slot,
            dispatched_slot: dispatched,
            lag_slots,
            state: context.ingest_state().label().to_string(),
            at_tip: context.is_at_tip(),
        },
        bootstrap: BootstrapInfo {
            ready: context.bootstrap.is_ready(),
            phase: bootstrap.phase.label().to_string(),
            current_slot: bootstrap.current_slot,
            target_slot: bootstrap.target_slot,
        },
        storage: StorageInfo {
            disk_used_bytes: backend.live_data_size_bytes().ok().flatten().unwrap_or(0),
            disk_free_bytes: backend.available_disk_bytes().ok().flatten().unwrap_or(0),
            owned_spools: owned_spool_count,
            volumes: backend
                .disk_volumes()
                .unwrap_or_default()
                .into_iter()
                .map(|v| StorageVolume {
                    name: match v.volume {
                        StoreVolume::Primary => "meta",
                        StoreVolume::Bulk => "bulk",
                    }
                    .to_string(),
                    used_bytes: v.used_bytes,
                    free_bytes: v.free_bytes.unwrap_or(0),
                })
                .collect(),
        },
        contents: StorageContents {
            tapes: backend.key_count_estimate(TapeCol::CF_NAME).ok().flatten().unwrap_or(0),
            tracks: backend.key_count_estimate(TrackCol::CF_NAME).ok().flatten().unwrap_or(0),
            objects: backend.key_count_estimate(ObjectInfoCol::CF_NAME).ok().flatten().unwrap_or(0),
            slices: stored_slices(context),
        },
        store_io: store_io_stats(&gathered),
        resources: {
            let (rss, vsz) = memory_stats::memory_stats()
                .map(|m| (m.physical_mem as u64, m.virtual_mem as u64))
                .unwrap_or((0, 0));
            let (cpu_seconds, open_fds, queues) = resource_extras(&gathered);
            ResourceInfo { rss_bytes: rss, virtual_bytes: vsz, cpu_seconds, open_fds, queues }
        },
        throughput: ThroughputTotals {
            blocks_processed: m.blocks_processed_total.get(),
            replay_events: m.replay_events_total.get(),
            repair_escalations: m.repair_escalations_total.get(),
        },
        http: http_stats(&gathered),
        peers: peer_stats(&gathered),
        chain: chain_stats(&gathered),
        decode: {
            let (latency_buckets, latency_total) = decode_latency(&gathered);
            DecodeStats {
                results: labeled(DECODE_RESULTS, &|r| m.decode_total.with_label_values(&[r]).get()),
                slices: labeled(DECODE_SLICE_OUTCOMES, &|o| {
                    m.decode_slices_total.with_label_values(&[o]).get()
                }),
                latency_buckets,
                latency_total,
            }
        },
        cache: CacheStats {
            results: labeled(CACHE_RESULTS, &|r| m.cache_requests_total.with_label_values(&[r]).get()),
            evicted: m.cache_evicted_total.get(),
        },
        spool,
        last_epoch: super::last_epoch(),
        current_epoch: current_epoch.clone(),
        lifetime: super::epoch::lifetime_including(&current_epoch),
    }
}
