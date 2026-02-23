//! Node context — central shared state for the storage node.
//!
//! `NodeContext` holds all shared dependencies that runtime components need.
//! Every component receives `Arc<NodeContext>` instead of individual dependencies.

use std::sync::Arc;

use rpc::Rpc;
use rpc_client::RpcClient;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use store::Store;
use tape_api::program::tapedrive::node_pda;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::NodeId;
use tape_crypto::Pubkey;
use tape_store::ops::MetaOps;
use tape_store::TapeStore;

use crate::state::ChainStateHandle;
use crate::core::expand_path;
use super::config::NodeConfig;
use super::stats::RuntimeStats;

/// Error type for context initialization.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("failed to load keypair: {0}")]
    Keypair(String),

    #[error("failed to load BLS keypair: {0}")]
    BlsKeypair(String),

    #[error("failed to initialize RPC client: {0}")]
    RpcClient(String),

    #[error("failed to open storage: {0}")]
    Storage(String),

    #[error("failed to fetch on-chain state: {0}")]
    ChainState(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Central context holding all shared node state.
///
/// Generic over storage backend `S` and RPC implementation `R`.
pub struct NodeContext<S: Store, R: Rpc> {
    /// Node configuration.
    pub config: Arc<NodeConfig>,
    /// This node's authority keypair.
    pub keypair: Arc<Keypair>,
    /// BLS private key for committee signing.
    pub bls_keypair: Arc<BlsPrivateKey>,
    /// Typed storage layer.
    pub store: Arc<TapeStore<S>>,
    /// Runtime statistics (atomic counters).
    pub stats: RuntimeStats,
    /// RPC client for on-chain operations.
    pub rpc: Arc<RpcClient<R>>,
    /// In-memory chain state (epoch, phase, committee, spools).
    pub chain_state: ChainStateHandle,
    /// Onchain unique id for this node after registration
    node_id: NodeId,
    /// PDA-derived node account address (cached from authority keypair).
    node_address: Pubkey,
}

impl<S: Store, R: Rpc> NodeContext<S, R> {
    /// Construct context without startup on-chain node-id resolution.
    ///
    /// Intended for tests/local fixtures. Runtime startup should use
    /// `NodeContextBuilder::build()`.
    pub fn new(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        store: TapeStore<S>,
        rpc: RpcClient<R>,
    ) -> Arc<Self> {
        Self::from_parts(config, keypair, bls_keypair, store, rpc, NodeId(0))
    }
    
    /// This node's PDA-derived on-chain account address. Use this to compare
    /// against `NodeInfo.node_address` in committee lookups.
    pub fn node_address(&self) -> Pubkey {
        self.node_address
    }

    fn from_parts(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        store: TapeStore<S>,
        rpc: RpcClient<R>,
        node_id: NodeId,
    ) -> Arc<Self> {
        let (node_address, _) = node_pda(keypair.pubkey());
        Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
            bls_keypair: Arc::new(bls_keypair),
            store: Arc::new(store),
            stats: RuntimeStats::default(),
            rpc: Arc::new(rpc),
            chain_state: ChainStateHandle::new(),
            node_id,
            node_address,
        })
    }

    /// Get this node's public key (authority).
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Globally unique node id for this node (derived onchain after register)
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

}

pub struct NodeContextBuilder<S: Store, R: Rpc> {
    config: NodeConfig,
    keypair: Keypair,
    store: TapeStore<S>,
    rpc: RpcClient<R>,
}

impl<S: Store, R: Rpc> NodeContextBuilder<S, R> {
    pub fn new(
        config: NodeConfig,
        keypair: Keypair,
        store: TapeStore<S>,
        rpc: RpcClient<R>,
    ) -> Self {
        Self {
            config,
            keypair,
            store,
            rpc,
        }
    }

    fn load_bls_keypair(config: &NodeConfig) -> Result<BlsPrivateKey, ContextError> {
        let path = expand_path(config.bls_keypair.to_string_lossy().as_ref());
        let bytes = std::fs::read(&path)
            .map_err(|e| ContextError::BlsKeypair(format!("read {}: {e}", path.display())))?;
        if bytes.len() != std::mem::size_of::<BlsPrivateKey>() {
            return Err(ContextError::BlsKeypair(format!(
                "wrong size: {} bytes (expected {}) at {}",
                bytes.len(),
                std::mem::size_of::<BlsPrivateKey>(),
                path.display()
            )));
        }
        Ok(*bytemuck::from_bytes::<BlsPrivateKey>(&bytes))
    }

    pub async fn resolve_node_id(
        rpc: &RpcClient<R>,
        keypair: &Keypair,
    ) -> Result<NodeId, ContextError> {
        let authority = keypair.pubkey();
        let node = rpc
            .get_node(&authority)
            .await
            .map_err(|e| ContextError::ChainState(format!("get_node({authority}): {e}")))?;
        Ok(node.id)
    }

    pub async fn build(self) -> Result<Arc<NodeContext<S, R>>, ContextError> {
        let node_id = Self::resolve_node_id(&self.rpc, &self.keypair).await?;
        self.store
            .set_node_id(node_id)
            .map_err(|e| ContextError::Storage(format!("set_node_id: {e}")))?;
        let bls_keypair = Self::load_bls_keypair(&self.config)?;

        Ok(NodeContext::from_parts(
            self.config,
            self.keypair,
            bls_keypair,
            self.store,
            self.rpc,
            node_id,
        ))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn context_builder_compiles() {}
}
