//! Shared data the node produces and the dashboard renders.
//!
//! Keeping these types and label names in one crate keeps the two sides from
//! drifting apart.

use serde::{Deserialize, Serialize};
use wincode::containers::{Pod, Vec as WincodeVec};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Balance in lamports below which a fee payer is treated as low
pub const LOW_BALANCE_LAMPORTS: u64 = 50_000_000;

/// Path the node serves one node's board from.
pub const BOARD_PATH: &str = "/v1/observe/board";

/// Path the node serves the whole-network view from.
pub const NETWORK_PATH: &str = "/v1/observe/network";

/// Path prefix for a peer's board, proxied through the serving node.
pub const PEER_BOARD_PREFIX: &str = "/v1/observe/peer/";

/// Route template for a peer's board.
pub const PEER_BOARD_PATH: &str = "/v1/observe/peer/{addr}/board";

/// Path the node serves its recent-traffic snapshot from, for the atlas
/// collector. Gated to configured observer identities over mTLS.
pub const ATLAS_PATH: &str = "/v1/observe/atlas";

/// Path the node streams live board updates from, as server-sent events
pub const STREAM_PATH: &str = "/v1/observe/stream";

/// Event names on the stream, so a client dispatches on the event name rather
/// than sniffing the body
pub const EVENT_HELLO: &str = "hello";
pub const EVENT_TICK: &str = "tick";
pub const EVENT_BOARD: &str = "board";
pub const EVENT_TOPOLOGY: &str = "topology";
/// One frame of recent history, so a chart opens full
pub const EVENT_BACKFILL: &str = "backfill";

/// One round trace, pushed as its evidence arrives rather than waiting for
/// the next whole board: a round is over in a few slots.
pub const EVENT_ROUND: &str = "round";

/// How often the node samples counters into a tick
pub const TICK_PERIOD_MS: u64 = 250;

/// How often the node repeats the full board; faster movers ride the tick
pub const BOARD_PERIOD_MS: u64 = 5_000;

/// How often the node repeats the topology; peer stats ride in it, and a
/// change in committee shape resends it sooner
pub const TOPOLOGY_PERIOD_MS: u64 = 10_000;

/// How much recent history a connecting client is sent, and how coarsely
pub const BACKFILL_SPAN_MS: u64 = 90_000;
pub const BACKFILL_STEP_MS: u64 = 1_000;

/// Bumped when a frame changes shape in a way an older client cannot read
pub const STREAM_PROTOCOL: u32 = 2;

/// Opening frame, so a client can size its interpolation window from the
/// producer's own cadences rather than hardcoding them
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Hello {
    pub protocol: u32,
    pub tick_ms: u64,
    pub address: String,
}

/// Per-second rates and fast-moving gauges, sampled by the node
///
/// The node owns the differencing because it knows the exact interval between
/// samples. Rates are per second whatever the sampling period.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Tick {
    /// Node wall clock in milliseconds, the x axis for every series
    pub at_ms: u64,
    /// Interval this frame's rates were measured over
    #[wincode(with = "Pod<f32>")]
    pub interval_secs: f32,

    #[wincode(with = "Pod<f32>")]
    pub req_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub egress_per_s: f32,
    /// Upload bytes received by the serving path
    #[wincode(with = "Pod<f32>")]
    pub ingress_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub err_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub serving_p50_ms: f32,
    #[wincode(with = "Pod<f32>")]
    pub serving_p95_ms: f32,
    #[wincode(with = "Pod<f32>")]
    pub serving_p99_ms: f32,

    #[wincode(with = "Pod<f32>")]
    pub peer_req_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub peer_ingress_per_s: f32,
    /// Bytes pushed to peers, the outbound half of peer traffic
    #[wincode(with = "Pod<f32>")]
    pub peer_egress_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub peer_err_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub peer_p50_ms: f32,
    #[wincode(with = "Pod<f32>")]
    pub peer_p95_ms: f32,
    #[wincode(with = "Pod<f32>")]
    pub peer_p99_ms: f32,

    #[wincode(with = "Pod<f32>")]
    pub store_ops_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub store_read_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub store_write_per_s: f32,

    #[wincode(with = "Pod<f32>")]
    pub rpc_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub rpc_err_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub tx_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub tx_err_per_s: f32,

    #[wincode(with = "Pod<f32>")]
    pub blocks_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub replay_per_s: f32,
    pub lag_slots: u64,
    pub tip_slot: u64,
    pub dispatched_slot: u64,

    #[wincode(with = "Pod<f32>")]
    pub cpu_pct: f32,
    pub rss_bytes: u64,
    pub queue_depth: u64,

    /// Bytes per second persisted and fetched, in spool op order
    #[wincode(with = "WincodeVec<Pod<f32>>")]
    pub spool_persisted_per_s: Vec<f32>,

    #[wincode(with = "Pod<f32>")]
    pub decode_per_s: f32,
    #[wincode(with = "Pod<f32>")]
    pub decode_p95_ms: f32,
    #[wincode(with = "Pod<f32>")]
    pub cache_hit_pct: f32,

    /// Challenge totals. On the tick rather than the board because a round now
    /// takes seconds, and on the board they arrive already stale.
    #[serde(default)]
    pub challenge: ChallengeRounds,
}

/// One peer call that moved payload bytes, from the serving node's view.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct AtlasTransfer {
    /// The remote node's address.
    pub peer: String,
    /// Which api call moved the bytes.
    pub op: String,
    /// True when this node sent the bytes, false when it received them.
    pub sent: bool,
    /// Payload bytes moved.
    pub bytes: u64,
}

/// One anonymous client request. The address is only shared with authorized
/// observers, which resolve it to a city and discard it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct AtlasIp {
    /// The caller's address.
    pub ip: String,
    /// True for uploads, false for fetches.
    pub write: bool,
}

/// One recently stored object, coarsened for display.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct AtlasObject {
    pub label: String,
    pub size: u64,
    pub kind: String,
}

/// Everything that happened since a caller's cursor, plus the new cursor.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct AtlasRecent {
    /// Pass back as the after query parameter to resume from here.
    pub seq: u64,
    /// Peer transfers since the cursor.
    pub transfers: Vec<AtlasTransfer>,
    /// Anonymous callers since the cursor.
    pub ips: Vec<AtlasIp>,
    /// Stored objects since the cursor.
    pub objects: Vec<AtlasObject>,
}

/// How reachable a committee member is from the node serving this board.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[serde(rename_all = "lowercase")]
pub enum LinkStatus {
    Local,
    Up,
    Down,
    #[default]
    Unknown,
}

/// Which process produced a board: a storage node or a read gateway.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[serde(rename_all = "lowercase")]
pub enum BoardKind {
    #[default]
    Node,
    Gateway,
}

/// Where a node's figures came from: its full observe board, its always-on
/// public stats endpoint, or nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[serde(rename_all = "lowercase")]
pub enum StatsSource {
    #[default]
    None,
    Observe,
    Public,
}

/// Per-node liveness stats shown in the network table.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NodeStats {
    #[serde(default)] pub version: String,
    #[serde(default)] pub owned_spools: u64,
    #[serde(default)] pub tracks_stored: u64,
    #[serde(default)] pub slices_stored: u64,
    #[serde(default)] pub slice_payload_bytes: u64,
    #[serde(default)] pub store_disk_bytes: u64,
    #[serde(default)] pub store_data_bytes: u64,
    #[serde(default)] pub free_disk_bytes: u64,
    #[serde(default)] pub current_epoch: u64,
    #[serde(default)] pub ingest_state: String,
    #[serde(default)] pub ingest_lag_slots: u64,
    #[serde(default)] pub reclaim_pending: bool,
    #[serde(default)] pub blocks_processed: u64,
    #[serde(default)] pub bootstrap_ready: bool,
    #[serde(default)] pub bootstrap_behind_slots: u64,
    #[serde(default)] pub fee_payer_lamports: Option<u64>,
    #[serde(default)] pub sync_bytes: u64,
    #[serde(default)] pub repair_bytes: u64,
    #[serde(default)] pub recover_bytes: u64,
    #[serde(default)] pub upload_bytes: u64,
}

/// One committee member, as seen on-chain and optionally enriched with liveness
/// stats by the serving node.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NetworkSpool {
    pub spool: u64,
    pub owner: Option<String>,
    pub owner_index: Option<usize>,
}

/// The committee and spool ownership for the current epoch, derived from one
/// node's on-chain state.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Network {
    #[serde(default)]
    pub generated_at: u64,
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

/// Filling a newly assigned spool.
pub const SPOOL_OP_SYNC: &str = "sync";

/// Refetching slices a spool is missing.
pub const SPOOL_OP_REPAIR: &str = "repair";

/// Rebuilding slices from the rest of their group.
pub const SPOOL_OP_RECOVER: &str = "recover";

/// Bytes that came in over the wire.
pub const SPOOL_STAGE_FETCHED: &str = "fetched";

/// Bytes that reached the store.
pub const SPOOL_STAGE_PERSISTED: &str = "persisted";

/// All spool pipeline operation labels.
pub const SPOOL_OPS: &[&str] = &[SPOOL_OP_SYNC, SPOOL_OP_REPAIR, SPOOL_OP_RECOVER];

/// All spool pipeline stage labels.
pub const SPOOL_STAGES: &[&str] = &[SPOOL_STAGE_FETCHED, SPOOL_STAGE_PERSISTED];

/// Epoch phase names, in phase-index order.
pub const EPOCH_PHASES: &[&str] = &["Unknown", "Sync", "Snapshot", "Active", "Closing", "Completed"];

/// All HTTP status class labels.
pub const STATUS_CLASSES: &[&str] = &["1xx", "2xx", "3xx", "4xx", "5xx"];

/// The phase name for a phase index.
pub fn phase_name(index: u8) -> &'static str {
    EPOCH_PHASES.get(index as usize).copied().unwrap_or("Unknown")
}

/// One labeled counter value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Labeled {
    pub label: String,
    pub value: u64,
}

/// Node identity and lifecycle.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct NodeInfo {
    pub address: String,
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
}

/// The current committee epoch and its membership.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct IngestInfo {
    pub tip_slot: u64,
    pub dispatched_slot: u64,
    pub lag_slots: u64,
    pub state: String,
    pub at_tip: bool,
}

/// Bootstrap catch-up progress, the same signal the health endpoint reports.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StorageVolume {
    pub name: String,
    pub used_bytes: u64,
    #[serde(default)]
    pub free_bytes: u64,
}

/// Cheap on-disk footprint figures.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StorageInfo {
    pub disk_used_bytes: u64,
    pub disk_free_bytes: u64,
    pub owned_spools: u64,
    #[serde(default)]
    pub volumes: Vec<StorageVolume>,
    #[serde(default)]
    pub data_bytes: u64,
}

/// Process resource usage.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ResourceInfo {
    pub rss_bytes: u64,
    pub virtual_bytes: u64,
    #[serde(default)]
    #[wincode(with = "Pod<f64>")]
    pub cpu_seconds: f64,
    #[serde(default)]
    pub open_fds: u64,
    #[serde(default)]
    pub queues: Vec<Labeled>,
}

/// Approximate counts of stored entities.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StorageContents {
    pub tapes: u64,
    pub tracks: u64,
    pub objects: u64,
    pub slices: u64,
}

/// Store-engine I/O totals.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StoreIo {
    pub ops: Vec<Labeled>,
    pub total_ops: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub errors: u64,
    #[wincode(with = "Pod<f64>")]
    pub get_avg_ms: f64,
    #[wincode(with = "Pod<f64>")]
    pub put_avg_ms: f64,
}

/// Cumulative counters since process start that feed the rate tiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ThroughputTotals {
    pub blocks_processed: u64,
    pub replay_events: u64,
    pub repair_escalations: u64,
    #[serde(default)]
    pub bytes_uploaded: u64,
}

/// Object decode breakdowns, plus the decode-duration histogram the dashboard
/// turns into windowed quantiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct DecodeStats {
    pub results: Vec<Labeled>,
    pub slices: Vec<Labeled>,
    #[serde(default)]
    pub latency_buckets: Vec<Bucket>,
    #[serde(default)]
    pub latency_total: u64,
}

/// Slice-cache breakdowns (cumulative).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CacheStats {
    pub results: Vec<Labeled>,
    pub evicted: u64,
}

/// Spool pipeline bytes by op and stage (cumulative).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SpoolStat {
    pub op: String,
    pub stage: String,
    pub bytes: u64,
}

/// One row of the challenge record: what this node has seen from one peer.
///
/// Rounds across, peers down. `recent` is oldest-first, one entry per round this
/// peer was judged in, so a void round leaves no mark against anyone and a peer
/// only just seen reads as a short row rather than a wall of misses.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChallengeRow {
    /// Peer address, base58.
    pub node: String,
    /// The spool this record is about.
    ///
    /// One row per spool rather than per peer, which is the grid the paper
    /// draws: rounds across, spools down. A peer holding several appears once
    /// per spool, since each owes its own answer every round.
    #[serde(default)]
    pub spool: u64,
    /// Rounds this peer was challenged in.
    pub opportunities: u64,
    /// Rounds it answered with a valid proof.
    pub successes: u64,
    /// Misses since its last success.
    pub consecutive_misses: u64,
    /// Share of opportunities answered, in basis points.
    pub success_rate_bps: u64,
    /// Whether this node's local rule has fired on the peer.
    pub rule_fired: bool,
    /// Whether this node currently has the peer queued for eviction.
    #[serde(default)]
    pub queued: bool,
    /// The recent strip, oldest first, true for a success.
    pub recent: Vec<bool>,
    /// Which round each entry in `recent` belongs to, same order and length.
    ///
    /// Rows cover different rounds: peers join at different times and are asked
    /// at different rates, so a strip cannot be placed by its length. Without
    /// these a reader can only right-align, which puts a row's cells under
    /// another row's round numbers and slides settled history as rows grow.
    #[serde(default)]
    pub rounds: Vec<RoundId>,
}

/// The challenge record this node keeps, one row per peer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChallengeGrid {
    /// Rounds the strip can hold, so a reader can size the grid.
    pub recent_capacity: u64,
    /// Rounds a peer has to be judged on before the rate arm applies.
    #[serde(default)]
    pub min_opportunities: u64,
    /// Success rate the rate arm fires below, in basis points.
    #[serde(default)]
    pub rate_floor_bps: u64,
    /// Consecutive misses the fast arm fires at.
    #[serde(default)]
    pub max_consecutive_misses: u64,
    /// Milliseconds from the entropy block to its certificate having gossiped,
    /// which is the width a timeline draws one round in. The slots a round
    /// spends searching for that block are behind it, so they are not counted.
    #[serde(default)]
    pub round_span_ms: u64,
    /// Milliseconds after the block by which a proof must arrive.
    #[serde(default)]
    pub proof_deadline_ms: u64,
    /// Milliseconds after the block by which attestations must be signed.
    #[serde(default)]
    pub attest_deadline_ms: u64,
    /// Milliseconds between one round opening and the next, so a reader can say
    /// how long the quiet between them still has to run.
    #[serde(default)]
    pub round_cadence_ms: u64,
    /// Votes a spool's round needs before it certifies.
    #[serde(default)]
    pub quorum: u64,
    /// Measured slot time. Every millisecond field above is this times a slot count.
    #[serde(default)]
    pub slot_ms: u64,
    /// Slots a round searches for its entropy block.
    #[serde(default)]
    pub span_slots: u64,
    /// Slots from a round opening to its certificate having gossiped.
    #[serde(default)]
    pub round_width_slots: u64,
    /// Slots between one round opening and the next.
    #[serde(default)]
    pub cadence_slots: u64,
    /// Slots a round waits on its entropy block before it is void.
    #[serde(default)]
    pub settle_deadline_slots: u64,
    /// Slots the sample set is cut behind a round. Covers the spread between
    /// owners' frontiers, which is wall-clock, so fast slots shrink it.
    #[serde(default)]
    pub sample_lookback_slots: u64,
    /// One row per peer, worst first so an outlier is the top row.
    pub rows: Vec<ChallengeRow>,
    /// One row per owner, judged by this node's own rule.
    #[serde(default)]
    pub owners: Vec<ChallengeOwner>,
    /// Which rounds the strip's columns stand for, right-aligned with them.
    ///
    /// Every member of a group is judged in the same rounds, so one axis labels
    /// every row. Shorter than the widest strip when the round store has been
    /// swept behind it, in which case the oldest columns go unlabelled.
    #[serde(default)]
    pub axis: Vec<RoundId>,
}

/// One owner's spools judged together, as the node judged them.
///
/// The rule lives in the node, so a reader draws this rather than deriving its
/// own. `rate` is pooled across the spools and is for display: a node failing
/// one spool of five still shows four fifths here while `verdict` has failed it.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChallengeOwner {
    /// Peer address, base58.
    pub node: String,
    /// Spools this node keeps a record for.
    pub spools: u64,
    /// Rounds judged, summed over them.
    pub opportunities: u64,
    /// Rounds answered, summed the same way.
    pub successes: u64,
    /// Answered over judged, pooled, in basis points.
    pub rate_bps: u64,
    /// The longest run any one spool is on.
    pub worst_run: u64,
    /// What the node's own rule makes of it.
    pub verdict: OwnerVerdict,
    /// Whether this node has it queued for eviction.
    pub queued: bool,
}

/// What a node's rule makes of one owner.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum OwnerVerdict {
    /// Nothing has fired.
    #[default]
    Healthy,
    /// Too few rounds on every spool to judge.
    Unproven,
    /// A spool has stopped answering. A probe can clear it.
    RunFailed,
    /// A spool's lifetime rate is through the floor. A probe cannot.
    RateFailed,
}

impl OwnerVerdict {
    /// Whether the rule has fired, either arm.
    pub fn fired(self) -> bool {
        matches!(self, OwnerVerdict::RunFailed | OwnerVerdict::RateFailed)
    }
}

/// One round's place in the timeline.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RoundId {
    pub epoch: u64,
    pub round: u64,
}

/// Lifetime challenge round counters for this node.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct ChallengeRounds {
    /// Rounds this node opened and answered for its own spool.
    pub opened: u64,
    /// Spool outcomes settled as certified.
    pub settled_certified: u64,
    /// Spool outcomes settled as local misses.
    pub settled_missed: u64,
    /// Incoming answers refused at the door.
    pub answers_refused: u64,
    /// Rounds whose entropy block never finalized, charged to nobody.
    #[serde(default)]
    pub voided: u64,
    /// Rounds dropped because their candidate block lost.
    #[serde(default)]
    pub discarded: u64,
    /// This node's own rounds that the group certified.
    #[serde(default)]
    pub own_certified: u64,
    /// This node's own rounds that gathered no certificate, which is what its
    /// group-mates each recorded as a miss against it.
    #[serde(default)]
    pub own_missed: u64,
}

/// One round as this node watched it happen.
///
/// The grid says which rounds a spool answered. This says when the evidence
/// arrived inside one of them, which is what a timeline draws. Every stamp is an
/// arrival at this node: no peer's send time is observable from one vantage, so
/// none is reported.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RoundTrace {
    pub epoch: u64,
    pub round: u64,
    /// The spool group, since groups open their rounds independently.
    pub group: u64,
    /// The slot the entropy block was produced in.
    pub anchor_slot: u64,
    /// Entropy blockhash, base58.
    pub block: String,
    /// When the round opened here, unix milliseconds.
    pub opened_at: u64,
    pub close: TraceClose,
    /// What each spool did, folded to the shapes a timeline draws.
    ///
    /// Every round carries these; only the newest carries the individual marks
    /// behind them. Twenty votes fold to one window and a count, which is what
    /// gets drawn anyway, and it is a twentieth of the bytes.
    #[serde(default)]
    pub shapes: Vec<SpoolShape>,
    /// The individual messages, for the rounds recent enough to trace in detail.
    #[serde(default)]
    pub marks: Vec<TraceMark>,
    /// Addresses the marks index into, base58. Sent whole on every push, since
    /// a group's worth of them is smaller than one mark carrying its own.
    #[serde(default)]
    pub nodes: Vec<String>,
    /// Index `marks` starts at, so a live round sends only what is new.
    ///
    /// Zero carries the whole list and replaces what a reader holds; anything
    /// higher appends. A round fills with hundreds of marks, and resending all
    /// of them every push made a round cost its own length squared.
    #[serde(default)]
    pub mark_base: u32,
    /// How each spool's round ended, decided when the round settles.
    ///
    /// Separate from the marks because a verdict is not a moment: settlement
    /// runs when the next round opens, a whole cadence later, so placing it on
    /// this round's clock would put it well past the round's own end.
    #[serde(default)]
    pub outcomes: Vec<SpoolOutcome>,
}

/// One spool's round, folded.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SpoolShape {
    pub spool: u64,
    /// When a peer's proof reached this node, milliseconds after the round opened.
    #[serde(default)]
    pub proof_in: Option<u64>,
    #[serde(default)]
    pub proof_out: Option<u64>,
    #[serde(default)]
    pub refused: Option<u64>,
    /// The voting window: first signature to last.
    #[serde(default)]
    pub vote_from: Option<u64>,
    #[serde(default)]
    pub vote_to: u64,
    #[serde(default)]
    pub votes: u32,
    #[serde(default)]
    pub vote_out: Option<u64>,
    #[serde(default)]
    pub cert: Option<u64>,
}

/// What one spool's round settled to.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SpoolOutcome {
    pub spool: u64,
    /// Index into the trace's `nodes`, or `u32::MAX` when unresolved.
    pub node: u32,
    pub certified: bool,
}

/// How a round ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum TraceClose {
    /// Still collecting evidence.
    #[default]
    Open,
    /// Settled against every spool in the group.
    Settled,
    /// Charged to nobody: the entropy block never finalized.
    Unfinalized,
    /// Charged to nobody: the group had nothing to be asked about.
    Nothing,
}

/// One thing that happened to one spool inside a round.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct TraceMark {
    pub spool: u64,
    /// Index into the trace's `nodes`, or `u32::MAX` when unresolved.
    ///
    /// A round carries hundreds of marks and only a group's worth of distinct
    /// addresses, so the address itself is held once beside them.
    pub node: u32,
    pub kind: MarkKind,
    /// Milliseconds after the round opened, so a mark is placed without the
    /// reader having to reconcile two clocks.
    pub at_ms: u64,
}

/// What a mark records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum MarkKind {
    /// This node built and broadcast its own answer.
    AnswerOut,
    /// A peer's answer arrived and verified.
    #[default]
    AnswerIn,
    /// An answer was turned away at the door.
    AnswerRefused,
    /// This node signed an attestation.
    AttestOut,
    /// A peer's attestation arrived.
    AttestIn,
    /// A certificate assembled for the spool.
    Certified,
}

/// One cumulative histogram bucket.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct Bucket {
    #[wincode(with = "Pod<f64>")]
    pub le_secs: f64,
    pub count: u64,
}

/// HTTP serving stats the dashboard turns into windowed rate, error rate, and
/// latency percentiles.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
    /// Request body bytes: uploads received when serving, bytes sent when a client
    #[serde(default)]
    pub request_bytes: u64,
}

/// Solana RPC and transaction-submission health.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
    /// Quantile in milliseconds over the samples between two cumulative readings
    ///
    /// A window with nothing in it has no percentile to report and reads as zero.
    pub fn quantile_ms(newer: &[Bucket], older: &[Bucket], count: u64, q: f64) -> f32 {
        if count == 0 {
            return 0.0;
        }
        let delta = Self::bucket_delta(newer, older);
        (Self::quantile(&delta, count, q) * 1000.0) as f32
    }

    /// Requests whose status class counts as a serving error
    pub fn error_total(&self) -> u64 {
        self.by_status
            .iter()
            .filter(|l| l.label == "4xx" || l.label == "5xx")
            .map(|l| l.value)
            .sum()
    }

    /// Peer-client responses of 400 and above
    pub fn peer_error_total(&self) -> u64 {
        self.by_status
            .iter()
            .filter(|l| l.label.parse::<u16>().map(|c| c >= 400).unwrap_or(false))
            .map(|l| l.value)
            .sum()
    }

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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
    #[wincode(with = "Pod<f64>")]
    pub serving_p95_ms: f64,
    #[serde(default)]
    #[wincode(with = "Pod<f64>")]
    pub decode_p95_ms: f64,
    #[serde(default)]
    pub max_lag_slots: u64,
    #[serde(default)]
    pub shards_owned: u64,
    #[serde(default)]
    pub synced_groups: u64,
}

/// Minutes of transfer history a board carries.
pub const BANDWIDTH_MINUTES: usize = 60;

/// Bytes moved on each transfer path during one wall-clock minute
///
/// The counters a board carries are cumulative, so a dashboard can only chart
/// what it has watched. This is the node's own reading of the minutes before
/// that, oldest first and contiguous, with the last one still filling.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct BandwidthMinute {
    /// Unix minute the bucket covers
    pub minute: u64,
    pub sync: u64,
    pub repair: u64,
    pub recover: u64,
    pub upload: u64,
}

/// Everything one node reports for its board in a single poll.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
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
    #[serde(default)]
    pub bandwidth: Vec<BandwidthMinute>,
    pub last_epoch: LastEpoch,
    #[serde(default)]
    pub current_epoch: LastEpoch,
    #[serde(default)]
    pub lifetime: LastEpoch,
    #[serde(default)]
    pub challenge: ChallengeGrid,
    #[serde(default)]
    pub challenge_rounds: ChallengeRounds,
    /// Recent rounds in the order they opened, for the live timeline.
    #[serde(default)]
    pub challenge_timeline: Vec<RoundTrace>,
}

impl Board {
    /// Sum of every decode result counter.
    pub fn decode_total(&self) -> u64 {
        self.decode.results.iter().map(|l| l.value).sum()
    }

    /// Sum of the decode result counters that count as failures
    pub fn decode_failures(&self) -> u64 {
        DECODE_FAILURES.iter().map(|f| Self::lookup(&self.decode.results, f)).sum()
    }

    /// Look up a labeled value, defaulting to 0.
    pub fn lookup(series: &[Labeled], label: &str) -> u64 {
        series.iter().find(|l| l.label == label).map(|l| l.value).unwrap_or(0)
    }

    /// Spool pipeline bytes for one operation and stage, defaulting to 0.
    pub fn spool_bytes(&self, op: &str, stage: &str) -> u64 {
        self.spool
            .iter()
            .find(|s| s.op == op && s.stage == stage)
            .map(|s| s.bytes)
            .unwrap_or(0)
    }
}

/// Everything a tick is derived from, as cumulative totals
///
/// Both producers fill this and hand it to `diff`, so the streamed tick and the
/// one derived from two polled boards cannot drift apart.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Counters {
    pub http: HttpStats,
    pub peers: HttpStats,
    pub chain: ChainStats,
    pub store_ops: u64,
    pub store_read: u64,
    pub store_written: u64,
    pub decode_buckets: Vec<Bucket>,
    pub decode_latency_total: u64,
    pub decode_ok: u64,
    pub decode_failed: u64,
    pub cache_hits: u64,
    pub cache_lookups: u64,
    pub blocks: u64,
    pub replay_events: u64,
    pub spool_persisted: Vec<u64>,
    pub spool_fetched: Vec<u64>,
    pub cpu_seconds: f64,
}

/// Gauges read at the instant of a tick rather than differenced
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Gauges {
    pub lag_slots: u64,
    pub tip_slot: u64,
    pub dispatched_slot: u64,
    pub rss_bytes: u64,
    pub queue_depth: u64,
    pub challenge: ChallengeRounds,
}

impl Counters {
    /// The cumulative totals a board already carries
    pub fn from_board(board: &Board) -> Self {
        Self {
            http: board.http.clone(),
            peers: board.peers.clone(),
            chain: board.chain.clone(),
            store_ops: board.store_io.total_ops,
            store_read: board.store_io.bytes_read,
            store_written: board.store_io.bytes_written,
            decode_buckets: board.decode.latency_buckets.clone(),
            decode_latency_total: board.decode.latency_total,
            decode_ok: Board::lookup(&board.decode.results, "ok"),
            decode_failed: board.decode_failures(),
            cache_hits: Board::lookup(&board.cache.results, "hit"),
            cache_lookups: CACHE_RESULTS
                .iter()
                .map(|r| Board::lookup(&board.cache.results, r))
                .sum(),
            blocks: board.throughput.blocks_processed,
            replay_events: board.throughput.replay_events,
            spool_persisted: spool_bytes(board, "persisted"),
            spool_fetched: spool_bytes(board, "fetched"),
            cpu_seconds: board.resources.cpu_seconds,
        }
    }
}

impl Gauges {
    /// The instantaneous figures a board already carries
    pub fn from_board(board: &Board) -> Self {
        Self {
            lag_slots: if board.bootstrap.ready {
                board.ingest.lag_slots
            } else {
                board.bootstrap.behind_slots()
            },
            tip_slot: board.ingest.tip_slot,
            dispatched_slot: board.ingest.dispatched_slot,
            challenge: board.challenge_rounds,
            rss_bytes: board.resources.rss_bytes,
            queue_depth: board.resources.queues.iter().map(|q| q.value).max().unwrap_or(0),
        }
    }
}

/// Spool pipeline bytes for one stage, in spool op order
fn spool_bytes(board: &Board, stage: &str) -> Vec<u64> {
    SPOOL_OPS
        .iter()
        .map(|op| {
            board
                .spool
                .iter()
                .find(|s| s.op == *op && s.stage == stage)
                .map(|s| s.bytes)
                .unwrap_or(0)
        })
        .collect()
}

/// Per-second rate between two cumulative readings
fn rate(now: u64, before: u64, interval: f32) -> f32 {
    now.saturating_sub(before) as f32 / interval
}

/// Per-second rates for each entry of two equal-length readings
fn rates(now: &[u64], before: &[u64], interval: f32) -> Vec<f32> {
    now.iter().zip(before).map(|(n, b)| rate(*n, *b, interval)).collect()
}

/// Difference two readings into the frame a dashboard plots
///
/// The interval is the time between the readings, which the caller knows more
/// precisely than either reading does.
pub fn diff(before: &Counters, now: &Counters, gauges: Gauges, at_ms: u64, interval: f32) -> Tick {
    let interval = interval.max(1e-3);
    let http_count = now.http.total.saturating_sub(before.http.total);
    let peer_count = now.peers.total.saturating_sub(before.peers.total);
    let decode_count = now.decode_latency_total.saturating_sub(before.decode_latency_total);
    let lookups = now.cache_lookups.saturating_sub(before.cache_lookups);

    Tick {
        at_ms,
        interval_secs: interval,

        req_per_s: rate(now.http.total, before.http.total, interval),
        egress_per_s: rate(now.http.response_bytes, before.http.response_bytes, interval),
        ingress_per_s: rate(now.http.request_bytes, before.http.request_bytes, interval),
        err_per_s: rate(now.http.error_total(), before.http.error_total(), interval),
        serving_p50_ms: HttpStats::quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.50),
        serving_p95_ms: HttpStats::quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.95),
        serving_p99_ms: HttpStats::quantile_ms(&now.http.buckets, &before.http.buckets, http_count, 0.99),

        peer_req_per_s: rate(now.peers.total, before.peers.total, interval),
        peer_ingress_per_s: rate(now.peers.response_bytes, before.peers.response_bytes, interval),
        peer_egress_per_s: rate(now.peers.request_bytes, before.peers.request_bytes, interval),
        peer_err_per_s: rate(now.peers.peer_error_total(), before.peers.peer_error_total(), interval),
        peer_p50_ms: HttpStats::quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.50),
        peer_p95_ms: HttpStats::quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.95),
        peer_p99_ms: HttpStats::quantile_ms(&now.peers.buckets, &before.peers.buckets, peer_count, 0.99),

        store_ops_per_s: rate(now.store_ops, before.store_ops, interval),
        store_read_per_s: rate(now.store_read, before.store_read, interval),
        store_write_per_s: rate(now.store_written, before.store_written, interval),

        rpc_per_s: rate(now.chain.rpc_total, before.chain.rpc_total, interval),
        rpc_err_per_s: rate(now.chain.rpc_errors, before.chain.rpc_errors, interval),
        tx_per_s: rate(now.chain.tx_total, before.chain.tx_total, interval),
        tx_err_per_s: rate(now.chain.tx_errors, before.chain.tx_errors, interval),

        blocks_per_s: rate(now.blocks, before.blocks, interval),
        replay_per_s: rate(now.replay_events, before.replay_events, interval),
        lag_slots: gauges.lag_slots,
        tip_slot: gauges.tip_slot,
        dispatched_slot: gauges.dispatched_slot,

        // per-core-second, so the rate is a core fraction
        cpu_pct: ((now.cpu_seconds - before.cpu_seconds) / interval as f64 * 100.0).max(0.0) as f32,
        rss_bytes: gauges.rss_bytes,
        queue_depth: gauges.queue_depth,

        spool_persisted_per_s: rates(&now.spool_persisted, &before.spool_persisted, interval),

        decode_per_s: rate(
            now.decode_ok + now.decode_failed,
            before.decode_ok + before.decode_failed,
            interval,
        ),
        decode_p95_ms: HttpStats::quantile_ms(
            &now.decode_buckets,
            &before.decode_buckets,
            decode_count,
            0.95,
        ),
        cache_hit_pct: if lookups == 0 {
            0.0
        } else {
            now.cache_hits.saturating_sub(before.cache_hits) as f32 / lookups as f32 * 100.0
        },

        challenge: gauges.challenge,
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

    // the backfill thins ticks to its step, and must still cover its span
    #[test]
    fn backfill_shape() {
        assert!(BACKFILL_STEP_MS >= TICK_PERIOD_MS);
        assert_eq!(BACKFILL_SPAN_MS % BACKFILL_STEP_MS, 0);
        let frames = BACKFILL_SPAN_MS / BACKFILL_STEP_MS;
        assert_eq!(frames * BACKFILL_STEP_MS, BACKFILL_SPAN_MS);
    }

    // a renamed tick field would leave the client silently reading zeros
    #[test]
    fn tick_roundtrip() {
        let tick = Tick {
            at_ms: 1_760_000_000_000,
            interval_secs: 0.25,
            req_per_s: 142.5,
            egress_per_s: 8_900_000.0,
            lag_slots: 4,
            tip_slot: 312_456_789,
            spool_persisted_per_s: vec![1.0, 2.0, 3.0],
            ..Default::default()
        };
        let text = serde_json::to_string(&tick).unwrap();
        assert_eq!(serde_json::from_str::<Tick>(&text).unwrap(), tick);
    }

    // rates divide by the interval, so a two-second window halves a per-second figure
    #[test]
    fn diff_rates() {
        let before = Counters {
            http: HttpStats { total: 100, response_bytes: 1_000, ..Default::default() },
            blocks: 10,
            ..Default::default()
        };
        let now = Counters {
            http: HttpStats { total: 140, response_bytes: 9_000, ..Default::default() },
            blocks: 15,
            ..Default::default()
        };
        let tick = diff(&before, &now, Gauges::default(), 0, 2.0);
        assert_eq!(tick.req_per_s, 20.0);
        assert_eq!(tick.egress_per_s, 4_000.0);
        assert_eq!(tick.blocks_per_s, 2.5);
    }

    // a counter that goes backwards across a restart reads as zero, not as a spike
    #[test]
    fn diff_survives_reset() {
        let before = Counters {
            http: HttpStats { total: 900, ..Default::default() },
            ..Default::default()
        };
        let now = Counters { http: HttpStats { total: 5, ..Default::default() }, ..Default::default() };
        assert_eq!(diff(&before, &now, Gauges::default(), 0, 1.0).req_per_s, 0.0);
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

    // the stream encodes with wincode and the dashboard decodes with it, so a
    // type that survives one and not the other blanks the board silently
    #[test]
    fn frames_survive_a_wincode_roundtrip() {
        let tick = Tick {
            at_ms: 1_234,
            interval_secs: 0.25,
            req_per_s: 12.5,
            spool_persisted_per_s: vec![1.0, 2.0, 3.0],
            challenge: ChallengeRounds { opened: 7, own_certified: 3, ..Default::default() },
            ..Default::default()
        };
        let bytes = wincode::serialize(&tick).expect("encode tick");
        let back: Tick = wincode::deserialize(&bytes).expect("decode tick");
        assert_eq!(tick, back);

        let trace = RoundTrace {
            epoch: 3,
            round: 90,
            group: 4,
            block: "Ce2caPm2hKodVn5qWCr7tA29MFXLyFmvAen6mpj4MAgd".into(),
            nodes: vec!["node-a".into(), "node-b".into()],
            marks: vec![TraceMark { spool: 12, node: 1, kind: MarkKind::AttestIn, at_ms: 219 }],
            shapes: vec![SpoolShape { spool: 12, vote_to: 57, votes: 19, ..Default::default() }],
            outcomes: vec![SpoolOutcome { spool: 12, node: 0, certified: true }],
            mark_base: 4,
            ..Default::default()
        };
        let bytes = wincode::serialize(&trace).expect("encode trace");
        let back: RoundTrace = wincode::deserialize(&bytes).expect("decode trace");
        assert_eq!(trace, back);

        let board = Board::default();
        let bytes = wincode::serialize(&board).expect("encode board");
        let back: Board = wincode::deserialize(&bytes).expect("decode board");
        assert_eq!(board, back);
    }
}
