use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_core::types::network::NetworkAddress;
use tape_core::types::tls::NetworkTlsPubkey;
use tape_crypto::Address;
use tape_protocol::{ProtocolState, fetch::fetch_state};

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

    #[error("node {0} not found in protocol peer set")]
    NodeNotFound(Address),

    #[error("required peers unresolved: {0:?}")]
    UnresolvedPeers(Vec<Address>),
}

pub struct PeerManager {
    peers: ArcSwap<HashMap<Address, PeerNode>>,
    status: DashMap<Address, PeerStatus>,
}

impl Default for PeerManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerManager {
    pub fn new() -> Self {
        Self {
            peers: ArcSwap::from_pointee(HashMap::new()),
            status: DashMap::new(),
        }
    }

    /// Resolve all current, previous, and next committee peers from protocol state.
    ///
    /// Current-committee peers are required. Previous and next committee peers
    /// are best effort because those sets are useful for repair, handoff, and
    /// fanout, but should not block serving the current epoch.
    pub fn resolve_peers(&self, state: &ProtocolState) -> Result<(), PeerManagerError> {
        let peers = self.resolve_peer_map(state)?;
        self.peers.store(Arc::new(peers));
        Ok(())
    }

    /// Fetch current protocol state and resolve the committee peer map.
    pub async fn bootstrap<R: Rpc>(
        &self,
        rpc: &RpcClient<R>,
    ) -> Result<ProtocolState, PeerManagerError> {
        let state = fetch_state(rpc).await?;
        self.resolve_peers(&state)?;
        Ok(state)
    }

    /// Resolve one peer from already-fetched protocol state and insert it into the cache.
    pub fn resolve_peer(
        &self,
        state: &ProtocolState,
        node: Address,
    ) -> Result<PeerNode, PeerManagerError> {
        let peer = Self::peer_directory(state)
            .remove(&node)
            .ok_or(PeerManagerError::NodeNotFound(node))?;
        self.add_peer(peer.clone());
        Ok(peer)
    }

    // Peer lookup

    /// Resolve a node's network address.
    pub fn resolve(&self, node: Address) -> Option<NetworkAddress> {
        self.peers.load().get(&node).map(|p| p.network_address)
    }

    /// Get a full PeerNode by node account address.
    pub fn get(&self, node: Address) -> Option<PeerNode> {
        self.peers.load().get(&node).cloned()
    }

    /// Reverse lookup: find the node account whose on-chain `network_tls`
    /// matches the given Ed25519 pubkey. Used by peer-auth middleware to map an
    /// mTLS client cert's SPKI back to a known committee member.
    pub fn node_for_tls_pubkey(&self, tls_pubkey: NetworkTlsPubkey) -> Option<Address> {
        self.peers
            .load()
            .values()
            .find(|peer| peer.tls_pubkey == tls_pubkey)
            .map(|peer| peer.node)
    }

    /// Check if a node is in the trusted set.
    pub fn contains(&self, node: Address) -> bool {
        self.peers.load().contains_key(&node)
    }

    /// Return all trusted peers.
    pub fn all(&self) -> Vec<PeerNode> {
        self.peers.load().values().cloned().collect()
    }

    /// Insert or update a peer.
    pub fn add_peer(&self, peer: PeerNode) {
        let guard = self.peers.load();
        let mut map = (**guard).clone();
        map.insert(peer.node, peer);
        self.peers.store(Arc::new(map));
    }

    // Health tracking

    pub fn report_failure(&self, node: Address) {
        self.status
            .entry(node)
            .and_modify(|s| {
                if let PeerStatus::Down {
                    failures,
                    last_failure,
                } = s
                {
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

    pub fn report_hostile(&self, node: Address) {
        self.status.insert(node, PeerStatus::Hostile);
    }

    pub fn report_success(&self, node: Address) {
        self.status.remove(&node);
    }

    pub fn is_healthy(&self, node: Address) -> bool {
        match self.status.get(&node) {
            None => true,
            Some(ref s) => match **s {
                PeerStatus::Healthy => true,
                PeerStatus::Hostile => false,
                PeerStatus::Down {
                    failures,
                    last_failure,
                } => {
                    tracing::warn!(
                        node = %node,
                        failures,
                        "peer is down with {failures} consecutive failures"
                    );

                    let cooldown_secs = 1u64 << failures.min(6);
                    last_failure.elapsed().as_secs() >= cooldown_secs
                }
            },
        }
    }

    // Routing

    /// Find a healthy peer that owns the given spool in the current committee.
    pub fn healthy_peer_for_spool(
        &self,
        state: &ProtocolState,
        spool: SpoolIndex,
    ) -> Option<Address> {
        let owner = state.spool_owner(spool)?;
        if self.is_healthy(owner) {
            Some(owner)
        } else {
            None
        }
    }

    /// Return all healthy peers in a spool group.
    pub fn healthy_peers_for_group(
        &self,
        state: &ProtocolState,
        group: GroupIndex,
    ) -> Vec<(SpoolIndex, Address)> {
        state
            .group_peers(group)
            .into_iter()
            .filter(|(_, node)| self.is_healthy(*node))
            .collect()
    }

    fn committee_nodes(state: &ProtocolState) -> Vec<Address> {
        let mut seen = HashSet::new();
        let current = state.current.committee.iter();
        let previous = state
            .previous
            .as_ref()
            .into_iter()
            .flat_map(|bundle| bundle.committee.iter());
        let next = state
            .next_committee
            .as_ref()
            .into_iter()
            .flat_map(|committee| committee.iter());

        current
            .chain(previous)
            .chain(next)
            .map(|member| member.node)
            .filter(|node| *node != Address::default())
            .filter(|node| seen.insert(*node))
            .collect()
    }

    fn required_nodes(state: &ProtocolState) -> HashSet<Address> {
        state
            .current
            .committee
            .iter()
            .map(|member| member.node)
            .filter(|node| *node != Address::default())
            .collect()
    }

    fn peer_directory(state: &ProtocolState) -> HashMap<Address, PeerNode> {
        state
            .peers
            .iter()
            .filter_map(|peer| PeerNode::from_peer(*peer))
            .map(|peer| (peer.node, peer))
            .collect()
    }

    fn resolve_peer_map(
        &self,
        state: &ProtocolState,
    ) -> Result<HashMap<Address, PeerNode>, PeerManagerError> {
        let directory = Self::peer_directory(state);
        let required = Self::required_nodes(state);
        let mut peers = HashMap::new();
        let mut unresolved_required = Vec::new();

        for node in Self::committee_nodes(state) {
            match directory.get(&node) {
                Some(peer) => {
                    peers.insert(node, peer.clone());
                }
                None if required.contains(&node) => {
                    unresolved_required.push(node);
                }
                None => {
                    tracing::warn!(
                        node = %node,
                        "best-effort peer resolve failed: node missing from protocol peer set"
                    );
                }
            }
        }

        if !unresolved_required.is_empty() {
            return Err(PeerManagerError::UnresolvedPeers(unresolved_required));
        }

        Ok(peers)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use bytemuck::Zeroable;
    use tape_api::state::Group;
    use tape_core::bls::BlsPubkey;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::{Member, Peer, Spool};
    use tape_core::types::EpochNumber;
    use tape_core::types::coin::TAPE;
    use tape_crypto::Hash;
    use tape_protocol::EpochBundle;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    fn make_peer(node: Address, port: u16) -> PeerNode {
        PeerNode {
            node,
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: NetworkTlsPubkey::new_unique(),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
            preferences: Zeroable::zeroed(),
        }
    }

    fn peer_entry(node: Address, port: u16) -> Peer {
        Peer {
            node,
            bls_pubkey: BlsPubkey::zeroed(),
            network_tls: NetworkTlsPubkey::new_unique(),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], port),
            preferences: Zeroable::zeroed(),
        }
    }

    fn member(node: Address) -> Member {
        Member::new(node, TAPE(1))
    }

    fn group(epoch: EpochNumber, owner: Address) -> Group {
        let mut group = Group {
            epoch,
            id: GroupIndex(0),
            ..Group::zeroed()
        };
        for i in 0..GROUP_SIZE {
            group.spools[i] = Spool {
                node: owner,
                ..Spool::zeroed()
            };
        }
        group
    }

    fn state_with_peer_set(peers: Vec<Peer>, committee: Vec<Member>) -> ProtocolState {
        let epoch = EpochNumber(3);
        ProtocolState {
            peers,
            current: EpochBundle {
                epoch: tape_api::state::Epoch {
                    id: epoch,
                    nonce: Hash::from([7; 32]),
                    ..Zeroable::zeroed()
                },
                committee,
                groups: vec![group(epoch, address(1))],
            },
            ..ProtocolState::default()
        }
    }

    #[test]
    fn add_and_resolve() {
        let pm = PeerManager::new();
        let node = address(1);
        assert!(pm.resolve(node).is_none());

        pm.add_peer(make_peer(node, 8001));
        assert!(pm.resolve(node).is_some());
    }

    #[test]
    fn add_overwrites() {
        let pm = PeerManager::new();
        let node = address(1);
        pm.add_peer(make_peer(node, 8001));
        pm.add_peer(make_peer(node, 9001));
        let addr = pm.resolve(node).unwrap();
        assert_eq!(addr, NetworkAddress::new_ipv4([127, 0, 0, 1], 9001));
    }

    #[test]
    fn contains_and_get() {
        let pm = PeerManager::new();
        let node = address(5);
        pm.add_peer(make_peer(node, 8005));
        assert!(pm.contains(node));
        assert!(!pm.contains(address(6)));

        let peer = pm.get(node).unwrap();
        assert_eq!(peer.node, node);
    }

    #[test]
    fn node_for_tls_pubkey_returns_address() {
        let pm = PeerManager::new();
        let node = address(8);
        let peer = make_peer(node, 8008);
        let tls = peer.tls_pubkey;
        pm.add_peer(peer);

        assert_eq!(pm.node_for_tls_pubkey(tls), Some(node));
    }

    #[test]
    fn resolve_peers_uses_protocol_peer_set() {
        let pm = PeerManager::new();
        let node = address(1);
        let state = state_with_peer_set(vec![peer_entry(node, 8001)], vec![member(node)]);

        pm.resolve_peers(&state).unwrap();

        let peer = pm.get(node).unwrap();
        assert_eq!(peer.node, node);
        assert_eq!(
            peer.network_address,
            NetworkAddress::new_ipv4([127, 0, 0, 1], 8001)
        );
    }

    #[test]
    fn resolve_peers_requires_current_committee_peers() {
        let pm = PeerManager::new();
        let node = address(1);
        let state = state_with_peer_set(Vec::new(), vec![member(node)]);

        let err = pm.resolve_peers(&state).unwrap_err();
        match err {
            PeerManagerError::UnresolvedPeers(nodes) => assert_eq!(nodes, vec![node]),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn healthy_peer_for_spool_returns_owner_address() {
        let pm = PeerManager::new();
        let node = address(1);
        let state = state_with_peer_set(vec![peer_entry(node, 8001)], vec![member(node)]);
        pm.resolve_peers(&state).unwrap();

        assert_eq!(
            pm.healthy_peer_for_spool(&state, SpoolIndex(0)),
            Some(node)
        );
    }
}
