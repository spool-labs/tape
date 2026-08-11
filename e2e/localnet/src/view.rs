use serde::Serialize;
use tape_observe_api::{ChallengeGrid, ChallengeRounds};
use tape_protocol::api::NodeStats;

#[derive(Clone, Serialize, Default)]
pub struct ClusterView {
    pub epoch: u64,
    pub phase: String,
    /// On-chain `EpochPhase` discriminant backing `phase` (see `phase_name`).
    pub phase_index: u8,
    pub phase_weight: Option<u64>,
    pub slot: u64,
    pub live_group_count: u64,
    pub committee_prev_size: usize,
    pub committee_size: usize,
    pub committee_next_size: usize,
    pub total_nodes_registered: u64,
    pub honest_rate_min_bps: Option<u64>,
    pub honest_rate_med_bps: Option<u64>,
}

#[derive(Clone, Serialize, Default)]
pub struct NodeView {
    pub local_id: usize,
    pub node_id: Option<u64>,
    pub authority: String,
    pub node_address: String,
    pub address: Option<String>,
    pub healthy: bool,
    pub stalled: bool,
    pub flapping: bool,
    pub suspended_until: Option<u64>,
    pub metrics_available: bool,
    pub pool_stake: Option<u64>,
    pub stats: Option<NodeStats>,
    pub challenge: Option<ChallengeGrid>,
    pub challenge_rounds: Option<ChallengeRounds>,
}

#[derive(Clone, Serialize, Default)]
pub struct SpoolView {
    pub spool: u64,
    pub owner_node: Option<String>,
    pub owner_local_id: Option<usize>,
}

#[derive(Clone, Serialize, Default)]
pub struct UploadView {
    pub size_bytes: u64,
    pub cert_status: String,
    pub tape_address: String,
    pub track_address: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Clone, Serialize)]
#[derive(Default)]
pub struct LocalnetView {
    pub cluster: ClusterView,
    pub nodes: Vec<NodeView>,
    pub spools: Vec<SpoolView>,
    pub uploads: Vec<UploadView>,
}
