//! TrustedPeers — address book of known, trusted peer nodes.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;

use crate::types::PeerNode;

/// Thread-safe address book of trusted peers.
///
/// Peers are added one at a time as they become known/trusted,
/// and removed when they should no longer be contacted.
/// All reads are lock-free via `ArcSwap`.
pub struct TrustedPeers {
    inner: ArcSwap<HashMap<NodeId, PeerNode>>,
}

impl TrustedPeers {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Insert or update a peer. Overwrites any existing entry for the same NodeId.
    pub fn add(&self, peer: PeerNode) {
        let guard = self.inner.load();
        let mut map = (**guard).clone();
        map.insert(peer.node_id, peer);
        self.inner.store(Arc::new(map));
    }

    /// Remove a peer by NodeId.
    pub fn remove(&self, node_id: NodeId) {
        let guard = self.inner.load();
        if !guard.contains_key(&node_id) {
            return;
        }
        let mut map = (**guard).clone();
        map.remove(&node_id);
        self.inner.store(Arc::new(map));
    }

    /// Remove all peers.
    pub fn clear(&self) {
        self.inner.store(Arc::new(HashMap::new()));
    }

    /// Resolve a node's network address.
    pub fn resolve(&self, node: NodeId) -> Option<NetworkAddress> {
        self.inner.load().get(&node).map(|p| p.network_address)
    }

    /// Get a full PeerNode by NodeId.
    pub fn get(&self, node: NodeId) -> Option<PeerNode> {
        self.inner.load().get(&node).cloned()
    }

    /// Check if a node is in the trusted set.
    pub fn contains(&self, node: NodeId) -> bool {
        self.inner.load().contains_key(&node)
    }

    /// Return all trusted peers.
    pub fn all(&self) -> Vec<PeerNode> {
        self.inner.load().values().cloned().collect()
    }

    /// Number of trusted peers.
    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_crypto::Pubkey;

    fn make_peer(id: u64, port: u16) -> PeerNode {
        PeerNode {
            node_id: NodeId(id),
            authority: Pubkey::new_unique(),
            state_address: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: Pubkey::new_unique(),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
        }
    }

    #[test]
    fn add_and_resolve() {
        let peers = TrustedPeers::new();
        assert!(peers.resolve(NodeId(1)).is_none());

        peers.add(make_peer(1, 8001));
        assert!(peers.resolve(NodeId(1)).is_some());
        assert_eq!(peers.len(), 1);
    }

    #[test]
    fn add_overwrites() {
        let peers = TrustedPeers::new();
        peers.add(make_peer(1, 8001));
        peers.add(make_peer(1, 9001));
        assert_eq!(peers.len(), 1);
        let addr = peers.resolve(NodeId(1)).unwrap();
        assert_eq!(
            addr,
            NetworkAddress::new_ipv4([127, 0, 0, 1], 9001)
        );
    }

    #[test]
    fn remove_peer() {
        let peers = TrustedPeers::new();
        peers.add(make_peer(1, 8001));
        peers.add(make_peer(2, 8002));
        assert_eq!(peers.len(), 2);

        peers.remove(NodeId(1));
        assert_eq!(peers.len(), 1);
        assert!(peers.resolve(NodeId(1)).is_none());
        assert!(peers.resolve(NodeId(2)).is_some());
    }

    #[test]
    fn remove_nonexistent() {
        let peers = TrustedPeers::new();
        peers.remove(NodeId(99));
        assert_eq!(peers.len(), 0);
    }

    #[test]
    fn clear_all() {
        let peers = TrustedPeers::new();
        peers.add(make_peer(1, 8001));
        peers.add(make_peer(2, 8002));
        peers.clear();
        assert!(peers.is_empty());
    }

    #[test]
    fn contains_and_get() {
        let peers = TrustedPeers::new();
        peers.add(make_peer(5, 8005));
        assert!(peers.contains(NodeId(5)));
        assert!(!peers.contains(NodeId(6)));

        let node = peers.get(NodeId(5)).unwrap();
        assert_eq!(node.node_id, NodeId(5));
    }
}
