use serde::Serialize;
use tape_core::erasure::SPOOL_COUNT;
use tape_protocol::api::NodeStats;

#[derive(Clone, Serialize, Default)]
pub struct ClusterView {
    pub epoch: u64,
    pub phase: String,
    pub phase_weight: Option<u64>,
    pub slot: u64,
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
    pub address: Option<String>,
    pub healthy: bool,
    pub metrics_available: bool,
    pub pool_stake: Option<u64>,
    pub stats: Option<NodeStats>,
}

#[derive(Clone, Serialize, Default)]
pub struct SpoolView {
    pub spool: u16,
    pub owner_node_id: Option<u64>,
    pub owner_local_id: Option<usize>,
}

#[derive(Clone, Serialize)]
pub struct ProdnetView {
    pub cluster: ClusterView,
    pub nodes: Vec<NodeView>,
    pub spools: Vec<SpoolView>,
}

impl Default for ProdnetView {
    fn default() -> Self {
        Self {
            cluster: ClusterView::default(),
            nodes: Vec::new(),
            spools: (0..SPOOL_COUNT)
                .map(|spool| SpoolView {
                    spool: spool as u16,
                    owner_node_id: None,
                    owner_local_id: None,
                })
                .collect(),
        }
    }
}
