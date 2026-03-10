//! PeerManager — peer lifecycle, health tracking, and routing.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;
use tape_core::types::network::NetworkAddress;
use tape_protocol::{ProtocolState, SharedState};

use crate::PeerNode;

#[derive(Debug, Clone)]
pub enum PeerStatus {
    Healthy,
    Down { failures: u32, last_failure: Instant },
    Hostile,
}

#[derive(Debug, thiserror::Error)]
pub enum PeerManagerError {
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),

    #[error("node {0:?} not found on-chain")]
    NodeNotFound(NodeId),

    #[error("required peers unresolved: {0:?}")]
    UnresolvedPeers(Vec<NodeId>),
}

pub struct PeerManager {
    peers: ArcSwap<HashMap<NodeId, PeerNode>>,
    state: SharedState,
    status: DashMap<NodeId, PeerStatus>,
}

impl PeerManager {
    pub fn new(state: SharedState) -> Self {
        Self {
            peers: ArcSwap::from_pointee(HashMap::new()),
            state,
            status: DashMap::new(),
        }
    }

    fn committee_ids(state: &ProtocolState) -> Vec<NodeId> {
        let mut seen = HashSet::new();
        state.committee
            .iter()
            .chain(state.committee_prev.iter())
            .chain(state.committee_next.iter())
            .filter_map(|member| seen.insert(member.id).then_some(member.id))
            .collect()
    }

    fn required_ids(state: &ProtocolState) -> HashSet<NodeId> {
        state.committee.iter().map(|member| member.id).collect()
    }

    async fn resolve_peer_map<R: Rpc>(
        &self,
        rpc: &RpcClient<R>,
        state: &ProtocolState,
    ) -> Result<HashMap<NodeId, PeerNode>, PeerManagerError> {
        let mut peers = HashMap::new();
        let required = Self::required_ids(state);
        let mut unresolved_required = Vec::new();

        for node_id in Self::committee_ids(state) {
            match self.resolve_peer_inner(rpc, node_id).await {
                Ok(peer) => {
                    peers.insert(node_id, peer);
                }
                Err(err) => {
                    if required.contains(&node_id) {
                        unresolved_required.push(node_id);
                    } else {
                        tracing::warn!(node = node_id.0, "best-effort peer resolve failed: {err}");
                    }
                }
            }
        }

        if !unresolved_required.is_empty() {
            return Err(PeerManagerError::UnresolvedPeers(unresolved_required));
        }

        Ok(peers)
    }

    /// Resolve all committee peers from the current shared state.
    ///
    /// Reads state from `SharedState` (caller must update it first),
    /// resolves network addresses for all committee members, and stores
    /// the peer map.
    pub async fn resolve_peers<R: Rpc>(&self, rpc: &RpcClient<R>) -> Result<(), PeerManagerError> {
        let state = self.state.load();
        let peers = self.resolve_peer_map(rpc, &state).await?;
        self.peers.store(Arc::new(peers));
        Ok(())
    }

    /// Resolve a single peer's current network address from on-chain data.
    pub async fn resolve_peer<R: Rpc>(&self, rpc: &RpcClient<R>, node_id: NodeId) -> Result<PeerNode, PeerManagerError> {
        let peer_node = self.resolve_peer_inner(rpc, node_id).await?;
        self.add_peer(peer_node.clone());
        Ok(peer_node)
    }

    async fn resolve_peer_inner<R: Rpc>(&self, rpc: &RpcClient<R>, node_id: NodeId) -> Result<PeerNode, PeerManagerError> {
        let (pda, node) = rpc
            .get_node_by_id(node_id)
            .await
            .map_err(|_| PeerManagerError::NodeNotFound(node_id))?;

        Ok(PeerNode {
            node_id,
            authority: node.authority,
            state_address: pda,
            bls_pubkey: node.metadata.bls_pubkey,
            tls_pubkey: node.metadata.network_tls,
            network_address: node.metadata.network_address,
        })
    }

    // Peer lookup

    /// Resolve a node's network address.
    pub fn resolve(&self, node_id: NodeId) -> Option<NetworkAddress> {
        self.peers.load().get(&node_id).map(|p| p.network_address)
    }

    /// Get a full PeerNode by NodeId.
    pub fn get(&self, node_id: NodeId) -> Option<PeerNode> {
        self.peers.load().get(&node_id).cloned()
    }

    /// Check if a node is in the trusted set.
    pub fn contains(&self, node_id: NodeId) -> bool {
        self.peers.load().contains_key(&node_id)
    }

    /// Return all trusted peers.
    pub fn all(&self) -> Vec<PeerNode> {
        self.peers.load().values().cloned().collect()
    }

    /// Insert or update a peer.
    pub fn add_peer(&self, peer: PeerNode) {
        let guard = self.peers.load();
        let mut map = (**guard).clone();
        map.insert(peer.node_id, peer);
        self.peers.store(Arc::new(map));
    }

    // Health tracking

    pub fn report_failure(&self, node_id: NodeId) {
        self.status
            .entry(node_id)
            .and_modify(|s| {
                if let PeerStatus::Down { failures, last_failure } = s {
                    *failures += 1;
                    *last_failure = Instant::now();
                } else if matches!(s, PeerStatus::Healthy) {
                    *s = PeerStatus::Down {
                        failures: 1,
                        last_failure: Instant::now(),
                    };
                }
            })
            .or_insert(PeerStatus::Down {
                failures: 1,
                last_failure: Instant::now(),
            });
    }

    pub fn report_hostile(&self, node_id: NodeId) {
        self.status.insert(node_id, PeerStatus::Hostile);
    }

    pub fn report_success(&self, node_id: NodeId) {
        self.status.remove(&node_id);
    }

    pub fn is_healthy(&self, node_id: NodeId) -> bool {
        match self.status.get(&node_id) {
            None => true,
            Some(ref s) => match **s {
                PeerStatus::Healthy => true,
                PeerStatus::Hostile => false,
                PeerStatus::Down { failures, last_failure } => {
                    let cooldown_secs = 1u64 << failures.min(6);
                    last_failure.elapsed().as_secs() >= cooldown_secs
                }
            },
        }
    }

    // Routing

    /// Find a healthy peer that owns the given spool in the current committee.
    pub fn healthy_peer_for_spool(&self, spool: SpoolIndex) -> Option<NodeId> {
        let state = self.state.load();
        let owner = state.spool_owner(spool)?;
        if self.is_healthy(owner) {
            Some(owner)
        } else {
            None
        }
    }

    /// Return all healthy peers in a spool group.
    pub fn healthy_peers_for_group(&self, group: SpoolGroup) -> Vec<(SpoolIndex, NodeId)> {
        let state = self.state.load();
        state
            .group_peers(group)
            .into_iter()
            .filter(|(_, node_id)| self.is_healthy(*node_id))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_crypto::Pubkey;
    use tape_protocol::new_shared_state;

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
        let pm = PeerManager::new(new_shared_state(ProtocolState::default()));
        assert!(pm.resolve(NodeId(1)).is_none());

        pm.add_peer(make_peer(1, 8001));
        assert!(pm.resolve(NodeId(1)).is_some());
    }

    #[test]
    fn add_overwrites() {
        let pm = PeerManager::new(new_shared_state(ProtocolState::default()));
        pm.add_peer(make_peer(1, 8001));
        pm.add_peer(make_peer(1, 9001));
        let addr = pm.resolve(NodeId(1)).unwrap();
        assert_eq!(
            addr,
            NetworkAddress::new_ipv4([127, 0, 0, 1], 9001)
        );
    }

    #[test]
    fn contains_and_get() {
        let pm = PeerManager::new(new_shared_state(ProtocolState::default()));
        pm.add_peer(make_peer(5, 8005));
        assert!(pm.contains(NodeId(5)));
        assert!(!pm.contains(NodeId(6)));

        let node = pm.get(NodeId(5)).unwrap();
        assert_eq!(node.node_id, NodeId(5));
    }
}
