//! Network — cached, stateful view of the tapedrive cluster.
//!
//! Combines `ProtocolState` (on-chain committees, spools, epoch) with
//! `TrustedPeers` (address book) and knows how to keep them in sync.

use std::sync::Arc;

use tape_peer::{PeerClient, PeerNode, TrustedPeers};
use rpc_client::{ProtocolState, Rpc, RpcClient, RpcError, StateCache};
use tape_core::types::NodeId;

/// Connected view of the tapedrive cluster.
///
/// Owns the protocol state cache and provides access to the trusted peer list
/// via the `PeerClient` implementation. Knows how to bootstrap from scratch and
/// incrementally refresh on epoch transitions.
pub struct Network<R: Rpc, P: PeerClient> {
    rpc: Arc<RpcClient<R>>,
    peer_client: Arc<P>,
    state: StateCache,
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),

    #[error("node {0:?} not found on-chain")]
    NodeNotFound(NodeId),
}

impl<R: Rpc, P: PeerClient> Network<R, P> {
    /// Create a new Network.
    ///
    /// Starts with empty protocol state. Call `bootstrap()` to fetch
    /// the current on-chain state and resolve committee addresses.
    pub fn new(rpc: Arc<RpcClient<R>>, peer_client: Arc<P>) -> Self {
        Self {
            rpc,
            peer_client,
            state: StateCache::new(ProtocolState::default()),
        }
    }

    /// Create a new Network seeded with a known protocol state.
    ///
    /// Use this when you already have state (e.g. from a previous session
    /// or from a node that already bootstrapped).
    pub fn with_state(rpc: Arc<RpcClient<R>>, peer_client: Arc<P>, state: ProtocolState) -> Self {
        Self {
            rpc,
            peer_client,
            state: StateCache::new(state),
        }
    }

    /// Access the current protocol state (lock-free).
    pub fn state(&self) -> arc_swap::Guard<Arc<ProtocolState>> {
        self.state.load()
    }

    /// Access the trusted peers (address book).
    pub fn peers(&self) -> &TrustedPeers {
        self.peer_client.peers()
    }

    /// Access the underlying state cache.
    pub fn state_cache(&self) -> &StateCache {
        &self.state
    }

    /// Access the peer client (for making requests).
    pub fn peer_client(&self) -> &Arc<P> {
        &self.peer_client
    }

    /// Access the RPC client.
    pub fn rpc(&self) -> &Arc<RpcClient<R>> {
        &self.rpc
    }

    /// Cold start: fetch protocol state and resolve all committee members.
    ///
    /// Fetches the current on-chain state, then resolves network addresses
    /// for every member in both current and previous committees.
    pub async fn bootstrap(&self) -> Result<(), NetworkError> {
        let state = self.rpc.fetch_state().await?;

        let all_members = state
            .committee
            .iter()
            .chain(state.committee_prev.iter());

        for member in all_members {
            if !self.peers().contains(member.id) {
                if let Ok(peer_node) = self.resolve_peer_inner(member.id).await {
                    self.peers().add(peer_node);
                }
            }
        }

        self.state.store(state);
        Ok(())
    }

    /// Incremental update: fetch new state, resolve only unknown peers.
    ///
    /// Call this on epoch transitions. Only makes RPC calls for committee
    /// members not already in the trusted peer list.
    pub async fn refresh(&self) -> Result<(), NetworkError> {
        let state = self.rpc.fetch_state().await?;

        let all_members = state
            .committee
            .iter()
            .chain(state.committee_prev.iter());

        for member in all_members {
            if !self.peers().contains(member.id) {
                if let Ok(peer_node) = self.resolve_peer_inner(member.id).await {
                    self.peers().add(peer_node);
                }
            }
        }

        self.state.store(state);
        Ok(())
    }

    /// Resolve a single peer's current network address from on-chain data.
    ///
    /// Useful when a peer is unreachable and its address may have changed.
    /// Updates the trusted peer list with the fresh address.
    pub async fn resolve_peer(&self, node_id: NodeId) -> Result<PeerNode, NetworkError> {
        let peer_node = self.resolve_peer_inner(node_id).await?;
        self.peers().add(peer_node.clone());
        Ok(peer_node)
    }

    async fn resolve_peer_inner(&self, node_id: NodeId) -> Result<PeerNode, NetworkError> {
        let (pda, node) = self
            .rpc
            .get_node_by_id(node_id)
            .await
            .map_err(|_| NetworkError::NodeNotFound(node_id))?;

        Ok(PeerNode {
            node_id,
            authority: node.authority,
            state_address: pda,
            bls_pubkey: node.metadata.bls_pubkey,
            tls_pubkey: node.metadata.network_tls,
            network_address: node.metadata.network_address,
        })
    }
}
