use serde::Serialize;
use tape_protocol::api::NodeStats;

#[derive(Clone, Serialize, Default)]
pub struct ClusterView {
    pub epoch: u64,
    pub phase: String,
    pub phase_weight: Option<u64>,
    pub slot: u64,
    pub live_group_count: u64,
    pub committee_prev_size: usize,
    pub committee_size: usize,
    pub committee_next_size: usize,
    pub total_nodes_registered: u64,
}

#[derive(Clone, Serialize, Default)]
pub struct NodeView {
    pub local_id: usize,
    pub node_id: Option<u64>,
    pub authority: String,
    pub node_address: String,
    pub address: Option<String>,
    pub healthy: bool,
    pub metrics_available: bool,
    pub pool_stake: Option<u64>,
    pub stats: Option<NodeStats>,
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
pub struct TestnetView {
    pub cluster: ClusterView,
    pub nodes: Vec<NodeView>,
    pub spools: Vec<SpoolView>,
    pub uploads: Vec<UploadView>,
}

impl Default for TestnetView {
    fn default() -> Self {
        Self {
            cluster: ClusterView::default(),
            nodes: Vec::new(),
            spools: Vec::new(),
            uploads: Vec::new(),
        }
    }
}
