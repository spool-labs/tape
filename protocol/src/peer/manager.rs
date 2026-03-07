//! PeerManager — peer lifecycle, health tracking, and routing.

use std::sync::Arc;
use std::time::Instant;

use dashmap::DashMap;
use rpc::{Rpc, RpcError};
use rpc_client::RpcClient;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::NodeId;

use crate::api::Api;
use crate::peer::{PeerNode, TrustedPeers};
use crate::state::{fetch_state, ProtocolState, StateHandle};

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
}

pub struct PeerManager<R: Rpc, A: Api> {
    rpc: Arc<RpcClient<R>>,
    api: Arc<A>,
    peers: Arc<TrustedPeers>,
    state: StateHandle,
    status: DashMap<NodeId, PeerStatus>,
}

impl<R: Rpc, A: Api> PeerManager<R, A> {
    pub fn new(rpc: Arc<RpcClient<R>>, api: Arc<A>, peers: Arc<TrustedPeers>) -> Self {
        Self {
            rpc,
            api,
            peers,
            state: StateHandle::new(ProtocolState::default()),
            status: DashMap::new(),
        }
    }

    pub fn with_state(
        rpc: Arc<RpcClient<R>>,
        api: Arc<A>,
        peers: Arc<TrustedPeers>,
        state: ProtocolState,
    ) -> Self {
        Self {
            rpc,
            api,
            peers,
            state: StateHandle::new(state),
            status: DashMap::new(),
        }
    }

    pub fn state(&self) -> arc_swap::Guard<Arc<ProtocolState>> {
        self.state.load()
    }

    pub fn state_handle(&self) -> &StateHandle {
        &self.state
    }

    pub fn peers(&self) -> &Arc<TrustedPeers> {
        &self.peers
    }

    pub fn api(&self) -> &Arc<A> {
        &self.api
    }

    /// Cold start: fetch protocol state and resolve all committee members.
    pub async fn bootstrap(&self) -> Result<(), PeerManagerError> {
        let state = fetch_state(&*self.rpc).await?;

        let all_members = state
            .committee
            .iter()
            .chain(state.committee_prev.iter());

        for member in all_members {
            if !self.peers.contains(member.id) {
                if let Ok(peer_node) = self.resolve_peer_inner(member.id).await {
                    self.peers.add(peer_node);
                }
            }
        }

        self.state.store(state);
        Ok(())
    }

    /// Incremental update: fetch new state, resolve only unknown peers.
    pub async fn refresh(&self) -> Result<(), PeerManagerError> {
        let state = fetch_state(&*self.rpc).await?;

        let all_members = state
            .committee
            .iter()
            .chain(state.committee_prev.iter());

        for member in all_members {
            if !self.peers.contains(member.id) {
                if let Ok(peer_node) = self.resolve_peer_inner(member.id).await {
                    self.peers.add(peer_node);
                }
            }
        }

        self.state.store(state);
        Ok(())
    }

    /// Resolve a single peer's current network address from on-chain data.
    pub async fn resolve_peer(&self, node_id: NodeId) -> Result<PeerNode, PeerManagerError> {
        let peer_node = self.resolve_peer_inner(node_id).await?;
        self.peers.add(peer_node.clone());
        Ok(peer_node)
    }

    async fn resolve_peer_inner(&self, node_id: NodeId) -> Result<PeerNode, PeerManagerError> {
        let (pda, node) = self
            .rpc
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

    pub fn reset_status(&self, node_id: NodeId) {
        self.status.remove(&node_id);
    }

    pub fn reset_all_status(&self) {
        self.status.clear();
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

    /// Add a peer to the trusted set.
    pub fn add_peer(&self, peer: PeerNode) {
        self.peers.add(peer);
    }
}
