//! PeerDirectory — maps NodeId to network addresses and spool assignments.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_store::types::NodeInfo;

struct DirectoryInner {
    by_node_id: HashMap<NodeId, NodeInfo>,
    member_to_node: Vec<NodeId>,
    member_to_node_prev: Vec<NodeId>,
    spool_to_node: HashMap<SpoolIndex, NodeId>,
    spool_to_node_prev: HashMap<SpoolIndex, NodeId>,
}

impl DirectoryInner {
    fn empty() -> Self {
        Self {
            by_node_id: HashMap::new(),
            member_to_node: Vec::new(),
            member_to_node_prev: Vec::new(),
            spool_to_node: HashMap::new(),
            spool_to_node_prev: HashMap::new(),
        }
    }

    fn from_committees(committee: &[NodeInfo], committee_prev: &[NodeInfo]) -> Self {
        let mut by_node_id = HashMap::with_capacity(committee.len() + committee_prev.len());
        let mut member_to_node = Vec::with_capacity(committee.len());
        let mut member_to_node_prev = Vec::with_capacity(committee_prev.len());
        let mut spool_to_node = HashMap::new();
        let mut spool_to_node_prev = HashMap::new();

        for info in committee {
            by_node_id.insert(info.node_id, info.clone());
            member_to_node.push(info.node_id);
            for &s in &info.spools {
                spool_to_node.insert(s, info.node_id);
            }
        }

        for info in committee_prev {
            by_node_id.entry(info.node_id).or_insert_with(|| info.clone());
            member_to_node_prev.push(info.node_id);
            for &s in &info.spools {
                spool_to_node_prev.insert(s, info.node_id);
            }
        }

        Self {
            by_node_id,
            member_to_node,
            member_to_node_prev,
            spool_to_node,
            spool_to_node_prev,
        }
    }
}

/// Thread-safe directory mapping NodeId to network addresses and spool assignments.
///
/// Updated once per epoch via `update()`. Lookups are lock-free reads via `ArcSwap`.
pub struct PeerDirectory {
    inner: ArcSwap<DirectoryInner>,
}

impl PeerDirectory {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(DirectoryInner::empty()),
        }
    }

    /// Rebuild all indexes from the current and previous committee.
    pub fn update(&self, committee: &[NodeInfo], committee_prev: &[NodeInfo]) {
        self.inner
            .store(Arc::new(DirectoryInner::from_committees(committee, committee_prev)));
    }

    /// Resolve a node's network address.
    pub fn resolve(&self, node: NodeId) -> Option<NetworkAddress> {
        self.inner.load().by_node_id.get(&node).map(|n| n.network_address)
    }

    /// Get full NodeInfo for a node.
    pub fn node_info(&self, node: NodeId) -> Option<NodeInfo> {
        self.inner.load().by_node_id.get(&node).cloned()
    }

    /// Map a current committee member index to its NodeId.
    pub fn member_to_node_id(&self, index: usize) -> Option<NodeId> {
        self.inner.load().member_to_node.get(index).copied()
    }

    /// Convenience: NodeId -> NetworkAddress.
    pub fn node_id_to_address(&self, id: NodeId) -> Option<NetworkAddress> {
        self.resolve(id)
    }

    /// Current committee as a Vec of NodeInfo.
    pub fn committee(&self) -> Vec<NodeInfo> {
        let guard = self.inner.load();
        guard
            .member_to_node
            .iter()
            .filter_map(|id| guard.by_node_id.get(id).cloned())
            .collect()
    }

    /// Previous committee as a Vec of NodeInfo.
    pub fn committee_prev(&self) -> Vec<NodeInfo> {
        let guard = self.inner.load();
        guard
            .member_to_node_prev
            .iter()
            .filter_map(|id| guard.by_node_id.get(id).cloned())
            .collect()
    }

    /// Which node owns this spool in the current committee?
    pub fn spool_owner(&self, spool: SpoolIndex) -> Option<NodeId> {
        self.inner.load().spool_to_node.get(&spool).copied()
    }

    /// Which node owned this spool in the previous committee?
    pub fn spool_owner_prev(&self, spool: SpoolIndex) -> Option<NodeId> {
        self.inner.load().spool_to_node_prev.get(&spool).copied()
    }

    /// All peers in a spool group (current committee), mapped by spool index to NodeId.
    pub fn group_peers(&self, group: SpoolGroup) -> HashMap<SpoolIndex, NodeId> {
        let guard = self.inner.load();
        guard
            .spool_to_node
            .iter()
            .filter(|(&spool, _)| group.contains(spool))
            .map(|(&spool, &node)| (spool, node))
            .collect()
    }

    /// All peers in a spool group (previous committee), mapped by spool index to NodeId.
    pub fn group_peers_prev(&self, group: SpoolGroup) -> HashMap<SpoolIndex, NodeId> {
        let guard = self.inner.load();
        guard
            .spool_to_node_prev
            .iter()
            .filter(|(&spool, _)| group.contains(spool))
            .map(|(&spool, &node)| (spool, node))
            .collect()
    }

    /// Manually register a node (e.g. after an RPC lookup for a non-committee node).
    pub fn register(&self, node_id: NodeId, info: NodeInfo) {
        let guard = self.inner.load();
        let mut new_inner = DirectoryInner {
            by_node_id: guard.by_node_id.clone(),
            member_to_node: guard.member_to_node.clone(),
            member_to_node_prev: guard.member_to_node_prev.clone(),
            spool_to_node: guard.spool_to_node.clone(),
            spool_to_node_prev: guard.spool_to_node_prev.clone(),
        };
        new_inner.by_node_id.insert(node_id, info);
        self.inner.store(Arc::new(new_inner));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_store::types::Pubkey;

    fn make_node(id: u64, spools: Vec<u16>) -> NodeInfo {
        NodeInfo {
            node_id: NodeId(id),
            node_address: Pubkey::new([id as u8; 32]),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: Pubkey::new([0u8; 32]),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000 + id as u16),
            spools,
        }
    }

    #[test]
    fn resolve_after_update() {
        let dir = PeerDirectory::new();
        assert!(dir.resolve(NodeId(1)).is_none());

        let committee = vec![make_node(1, vec![0, 20]), make_node(2, vec![1, 21])];
        dir.update(&committee, &[]);

        assert!(dir.resolve(NodeId(1)).is_some());
        assert!(dir.resolve(NodeId(2)).is_some());
        assert!(dir.resolve(NodeId(99)).is_none());
    }

    #[test]
    fn spool_owner_lookup() {
        let dir = PeerDirectory::new();
        let current = vec![make_node(1, vec![0, 20]), make_node(2, vec![1, 21])];
        let prev = vec![make_node(3, vec![0, 1])];
        dir.update(&current, &prev);

        assert_eq!(dir.spool_owner(0), Some(NodeId(1)));
        assert_eq!(dir.spool_owner(1), Some(NodeId(2)));
        assert_eq!(dir.spool_owner(999), None);

        assert_eq!(dir.spool_owner_prev(0), Some(NodeId(3)));
        assert_eq!(dir.spool_owner_prev(1), Some(NodeId(3)));
    }

    #[test]
    fn register_non_committee() {
        let dir = PeerDirectory::new();
        dir.update(&[], &[]);
        assert!(dir.resolve(NodeId(99)).is_none());

        dir.register(NodeId(99), make_node(99, vec![]));
        assert!(dir.resolve(NodeId(99)).is_some());
    }

    #[test]
    fn member_index_lookup() {
        let dir = PeerDirectory::new();
        let committee = vec![make_node(10, vec![0]), make_node(20, vec![1])];
        dir.update(&committee, &[]);

        assert_eq!(dir.member_to_node_id(0), Some(NodeId(10)));
        assert_eq!(dir.member_to_node_id(1), Some(NodeId(20)));
        assert_eq!(dir.member_to_node_id(2), None);
    }
}
