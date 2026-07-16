//! Shared data the node produces and the dashboard renders.
//!
//! Keeping these types and label names in one crate keeps the two sides from
//! drifting apart.

use serde::{Deserialize, Serialize};

/// Path the node serves one node's board from.
pub const BOARD_PATH: &str = "/v1/observe/board";

/// Path the node serves the whole-network view from.
pub const NETWORK_PATH: &str = "/v1/observe/network";

/// Path prefix for a peer's board, proxied through the serving node.
pub const PEER_BOARD_PREFIX: &str = "/v1/observe/peer/";

/// Route template for a peer's board.
pub const PEER_BOARD_PATH: &str = "/v1/observe/peer/{addr}/board";

/// How reachable a committee member is from the node serving this board.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LinkStatus {
    Local,
    Up,
    Down,
    #[default]
    Unknown,
}

/// Which process produced a board: a storage node or a read gateway.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BoardKind {
    #[default]
    Node,
    Gateway,
}

/// Where a node's figures came from: its full observe board, its always-on
/// public stats endpoint, or nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StatsSource {
    #[default]
    None,
    Observe,
    Public,
}

/// Per-node liveness stats shown in the network table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeStats {
    #[serde(default)] pub version: String,
    #[serde(default)] pub owned_spools: u64,
    #[serde(default)] pub tracks_stored: u64,
    #[serde(default)] pub slices_stored: u64,
    #[serde(default)] pub slice_payload_bytes: u64,
    #[serde(default)] pub store_disk_bytes: u64,
    #[serde(default)] pub free_disk_bytes: u64,
    #[serde(default)] pub current_epoch: u64,
    #[serde(default)] pub ingest_state: String,
    #[serde(default)] pub ingest_lag_slots: u64,
    #[serde(default)] pub reclaim_pending: bool,
    #[serde(default)] pub blocks_processed: u64,
    #[serde(default)] pub bootstrap_ready: bool,
    #[serde(default)] pub bootstrap_behind_slots: u64,
}

/// One committee member, as seen on-chain and optionally enriched with liveness
/// stats by the serving node.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NetworkNode {
    /// Position in the committee, used for coloring and selection.
    pub index: usize,
    /// Node account address.
    pub address: String,
    /// On-chain operator name, empty when unknown.
    #[serde(default)]
    pub name: String,
    /// Spools this member owns this epoch.
    pub spools: u64,
    /// Reachability from the serving node.
    #[serde(default)]
    pub status: LinkStatus,
    /// Where this node's stats came from.
    #[serde(default)]
    pub source: StatsSource,
    /// True when the node is registered but not in the current committee.
    #[serde(default)]
    pub non_committee: bool,
    /// Base URL from the on-chain network address, if known.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Pool stake, if known.
    #[serde(default)]
    pub stake: Option<u64>,
    /// Liveness stats, present for the local node and for aggregated peers.
    #[serde(default)]
    pub stats: Option<NodeStats>,
}

/// One spool and the node that owns it this epoch.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NetworkSpool {
    pub spool: u64,
    pub owner: Option<String>,
    pub owner_index: Option<usize>,
}

/// The committee and spool ownership for the current epoch, derived from one
/// node's on-chain state.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Network {
    pub epoch: u64,
    pub phase: String,
    pub phase_index: u8,
    #[serde(default)]
    pub slot: u64,
    pub groups: u64,
    #[serde(default)]
    pub prev_committee_size: u64,
    pub committee_size: u64,
    pub next_committee_size: u64,
    pub peers: u64,
    pub committee: Vec<NetworkNode>,
    pub spools: Vec<NetworkSpool>,
}

/// All decode outcome labels.
pub const DECODE_RESULTS: &[&str] = &[
    "ok",
    "decode_error",
    "commitment_mismatch",
    "truncated",
    "insufficient_slices",
    "inline_hash_mismatch",
    "data_mismatch",
];

/// The decode outcome labels that count as failures.
pub const DECODE_FAILURES: &[&str] = &[
    "decode_error",
    "commitment_mismatch",
    "truncated",
    "insufficient_slices",
    "inline_hash_mismatch",
    "data_mismatch",
];

/// All slice fetch outcome labels.
pub const DECODE_SLICE_OUTCOMES: &[&str] = &["used", "rejected_leaf", "rejected_group", "fetch_failed"];

/// The slice fetch outcomes that count as wasted work.
pub const DECODE_SLICES_WASTED: &[&str] = &["rejected_leaf", "rejected_group", "fetch_failed"];

/// All slice cache result labels.
pub const CACHE_RESULTS: &[&str] = &["hit", "miss", "coalesced"];

/// All spool pipeline operation labels.
pub const SPOOL_OPS: &[&str] = &["sync", "repair", "recover"];

/// All spool pipeline stage labels.
pub const SPOOL_STAGES: &[&str] = &["fetched", "persisted"];

/// Epoch phase names, in phase-index order.
pub const EPOCH_PHASES: &[&str] = &["Unknown", "Sync", "Snapshot", "Active", "Closing", "Completed"];

/// All HTTP status class labels.
pub const STATUS_CLASSES: &[&str] = &["1xx", "2xx", "3xx", "4xx", "5xx"];

/// The phase name for a phase index.
pub fn phase_name(index: u8) -> &'static str {
    EPOCH_PHASES.get(index as usize).copied().unwrap_or("Unknown")
}

/// One labeled counter value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Labeled {
    pub label: String,
    pub value: u64,
}

/// Node identity and lifecycle.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub address: String,
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
}

/// The current committee epoch and its membership.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EpochInfo {
    pub number: u64,
    pub phase: String,
    pub phase_index: u8,
    pub synced_count: u64,
    pub committee_size: u64,
    pub groups: u64,
    pub peers: u64,
    pub peer_capacity: u64,
    pub shards_owned: u64,
    pub next_epoch: Option<u64>,
    pub next_committee_size: Option<u64>,
}

/// Solana block ingest progress.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IngestInfo {
    pub tip_slot: u64,
    pub dispatched_slot: u64,
    pub lag_slots: u64,
    pub state: String,
    pub at_tip: bool,
}

/// Bootstrap catch-up progress, the same signal the health endpoint reports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BootstrapInfo {
    pub ready: bool,
    pub phase: String,
    pub current_slot: u64,
    pub target_slot: u64,
}

impl BootstrapInfo {
    /// Slots left to replay before the node is caught up, zero once ready.
    pub fn behind_slots(&self) -> u64 {
        if self.ready {
            0
        } else {
            self.target_slot.saturating_sub(self.current_slot)
        }
    }
}

/// One store volume's on-disk size and free space.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageVolume {
    pub name: String,
    pub used_bytes: u64,
    #[serde(default)]
    pub free_bytes: u64,
}

/// Cheap on-disk footprint figures.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageInfo {
    pub disk_used_bytes: u64,
    pub disk_free_bytes: u64,
    pub owned_spools: u64,
    #[serde(default)]
    pub volumes: Vec<StorageVolume>,
}

/// Process resource usage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceInfo {
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    #[serde(default)]
    pub cpu_seconds: f64,
    #[serde(default)]
    pub open_fds: u64,
    #[serde(default)]
    pub queues: Vec<Labeled>,
}

/// Approximate counts of stored entities.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StorageContents {
    pub tapes: u64,
    pub tracks: u64,
    pub objects: u64,
    pub slices: u64,
}

/// Store-engine I/O totals.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct StoreIo {
    pub ops: Vec<Labeled>,
    pub total_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub errors: u64,
    pub get_avg_ms: f64,
    pub put_avg_ms: f64,
}

/// Cumulative counters since process start that feed the rate tiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ThroughputTotals {
    pub blocks_processed: u64,
    pub replay_events: u64,
    pub repair_escalations: u64,
}

/// Object decode breakdowns, plus the decode-duration histogram the dashboard
/// turns into windowed quantiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DecodeStats {
    pub results: Vec<Labeled>,
    pub slices: Vec<Labeled>,
    #[serde(default)]
    pub latency_buckets: Vec<Bucket>,
    #[serde(default)]
    pub latency_total: u64,
}

/// Slice-cache breakdowns (cumulative).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CacheStats {
    pub results: Vec<Labeled>,
    pub evicted: u64,
}

/// Spool pipeline bytes by op and stage (cumulative).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpoolStat {
    pub op: String,
    pub stage: String,
    pub bytes: u64,
}

/// One cumulative histogram bucket.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Bucket {
    pub le_secs: f64,
    pub count: u64,
}

/// HTTP serving stats the dashboard turns into windowed rate, error rate, and
/// latency percentiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HttpStats {
    /// Request-duration buckets.
    pub buckets: Vec<Bucket>,
    /// Requests by status class.
    pub by_status: Vec<Labeled>,
    /// Requests by matched route.
    #[serde(default)]
    pub by_route: Vec<Labeled>,
    /// Total requests in the latency histogram.
    pub total: u64,
    /// Response body bytes served.
    pub response_bytes: u64,
}

/// Solana RPC and transaction-submission health.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ChainStats {
    pub rpc_total: u64,
    pub rpc_errors: u64,
    pub rpc_buckets: Vec<Bucket>,
    pub rpc_latency_total: u64,
    pub endpoint: u64,
    pub endpoints: u64,
    pub tx_total: u64,
    pub tx_errors: u64,
    pub confirm_buckets: Vec<Bucket>,
    pub confirm_total: u64,
}

impl HttpStats {
    /// The counts accumulated between two cumulative snapshots of the same
    /// histogram, for windowed quantiles.
    pub fn bucket_delta(newer: &[Bucket], older: &[Bucket]) -> Vec<Bucket> {
        newer
            .iter()
            .enumerate()
            .map(|(i, b)| Bucket {
                le_secs: b.le_secs,
                count: b.count.saturating_sub(older.get(i).map(|x| x.count).unwrap_or(0)),
            })
            .collect()
    }

    /// The quantile in seconds over the bucket counts, with linear interpolation
    /// inside the matched bucket.
    pub fn quantile(buckets: &[Bucket], total: u64, q: f64) -> f64 {
        if total == 0 || buckets.is_empty() {
            return 0.0;
        }
        let rank = q * total as f64;
        let mut prev_le = 0.0;
        let mut prev_count = 0.0;
        for b in buckets {
            let c = b.count as f64;
            if c >= rank {
                let span = c - prev_count;
                if span <= 0.0 {
                    return b.le_secs;
                }
                return prev_le + (b.le_secs - prev_le) * ((rank - prev_count) / span);
            }
            prev_le = b.le_secs;
            prev_count = c;
        }
        buckets.last().map(|b| b.le_secs).unwrap_or(0.0)
    }
}

/// Deltas captured at the close of the last completed epoch, or zero before the
/// first epoch boundary this process has seen.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LastEpoch {
    pub number: u64,
    pub blocks: u64,
    pub replay_events: u64,
    pub decoded_objects: u64,
    pub decoded_bytes: u64,
    pub decode_failures: u64,
    pub slices_used: u64,
    pub slices_wasted: u64,
    pub spool_bytes_persisted: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub repair_escalations: u64,
    pub requests: u64,
    pub egress_bytes: u64,
    #[serde(default)]
    pub bytes_uploaded: u64,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub tx_total: u64,
    #[serde(default)]
    pub tx_errors: u64,
    #[serde(default)]
    pub rpc_errors: u64,
    #[serde(default)]
    pub store_ops: u64,
    #[serde(default)]
    pub store_bytes_read: u64,
    #[serde(default)]
    pub store_bytes_written: u64,
    #[serde(default)]
    pub spool_bytes_fetched: u64,
    #[serde(default)]
    pub serving_p95_ms: f64,
    #[serde(default)]
    pub decode_p95_ms: f64,
    #[serde(default)]
    pub max_lag_slots: u64,
    #[serde(default)]
    pub shards_owned: u64,
    #[serde(default)]
    pub synced_groups: u64,
}

/// Everything one node reports for its board in a single poll.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Board {
    pub generated_at: u64,
    #[serde(default)]
    pub source: StatsSource,
    #[serde(default)]
    pub kind: BoardKind,
    pub node: NodeInfo,
    pub epoch: EpochInfo,
    pub ingest: IngestInfo,
    #[serde(default)]
    pub bootstrap: BootstrapInfo,
    pub storage: StorageInfo,
    pub contents: StorageContents,
    pub store_io: StoreIo,
    pub resources: ResourceInfo,
    pub throughput: ThroughputTotals,
    pub http: HttpStats,
    pub peers: HttpStats,
    #[serde(default)]
    pub chain: ChainStats,
    pub decode: DecodeStats,
    pub cache: CacheStats,
    pub spool: Vec<SpoolStat>,
    pub last_epoch: LastEpoch,
    #[serde(default)]
    pub current_epoch: LastEpoch,
    #[serde(default)]
    pub lifetime: LastEpoch,
}

impl Board {
    /// Sum of every decode result counter.
    pub fn decode_total(&self) -> u64 {
        self.decode.results.iter().map(|l| l.value).sum()
    }

    /// Look up a labeled value, defaulting to 0.
    pub fn lookup(series: &[Labeled], label: &str) -> u64 {
        series.iter().find(|l| l.label == label).map(|l| l.value).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The route template and the URL-building prefix must describe one route.
    #[test]
    fn peer_board_path_matches_prefix() {
        assert!(PEER_BOARD_PATH.starts_with(PEER_BOARD_PREFIX));
        assert!(PEER_BOARD_PATH.ends_with("/board"));
    }

    // ready clears the replay distance; replaying reports remaining slots
    #[test]
    fn behind_slots() {
        let replaying = BootstrapInfo {
            ready: false,
            phase: "block_replay".into(),
            current_slot: 900,
            target_slot: 1000,
        };
        assert_eq!(replaying.behind_slots(), 100);

        let ready = BootstrapInfo { ready: true, ..replaying };
        assert_eq!(ready.behind_slots(), 0);
    }
}
