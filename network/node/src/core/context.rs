//! Node context - central shared state for the storage node.

use std::path::Path;
use std::sync::Arc;

use rpc_client::{RpcClient, RpcConfig};
use rpc_solana::SolanaRpc;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use store::Store;
use store_rocks::RocksStore;
use tape_core::bls::BlsPrivateKey;
use tape_crypto::Pubkey;
use tape_metrics::MetricsRegistry;

use super::config::NodeConfig;
use super::utils::{load_bls_keypair, load_keypair, KeypairError};
use tape_store::ops::MetaOps;

use crate::control_plane::ControlPlane;
use crate::features::storage::{StorageError, StorageService};
use crate::metrics::NodeMetrics;

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
    Storage(#[from] StorageError),

    #[error("failed to fetch on-chain state: {0}")]
    ChainState(String),

    #[error("node registration failed: {0}")]
    Registration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<KeypairError> for ContextError {
    fn from(err: KeypairError) -> Self {
        match err {
            KeypairError::Keypair(msg) => ContextError::Keypair(msg),
            KeypairError::BlsKeypair(msg) => ContextError::BlsKeypair(msg),
        }
    }
}

/// Central context holding all shared node state.
///
/// Generic over the storage backend `S`. Use [`NodeContext::from_config`] for
/// production (RocksStore) or [`NodeContext::new`] for testing with custom stores.
pub struct NodeContext<S: Store = RocksStore> {
    /// Node configuration.
    pub config: Arc<NodeConfig>,
    /// This node's authority keypair.
    pub keypair: Arc<Keypair>,
    /// BLS private key for committee signing.
    pub bls_keypair: Arc<BlsPrivateKey>,
    /// Tape RPC client for chain interactions.
    pub rpc: Arc<RpcClient<SolanaRpc>>,
    /// Slice storage service.
    pub storage: Arc<StorageService<S>>,
    /// In-memory cache of on-chain control plane state.
    pub control_plane: Arc<ControlPlane>,
    /// Prometheus metrics.
    pub metrics: Arc<NodeMetrics>,
}

impl NodeContext<RocksStore> {
    /// Construct context from config with RocksDB storage.
    ///
    /// This handles:
    /// 1. Loading the Solana keypair
    /// 2. Loading the BLS keypair
    /// 3. Creating the RPC client
    /// 4. Opening RocksDB storage
    /// 5. Fetching initial on-chain state (System, Epoch, Node)
    /// 6. Verifying node account exists
    /// 7. Initializing the ControlPlane cache
    /// 8. Initializing metrics
    pub async fn from_config(config: NodeConfig, rpc_url: &str) -> Result<Arc<Self>, ContextError> {
        // 1. Load keypair
        let keypair = load_keypair(&config.node_keypair)?;
        let authority = keypair.pubkey();
        tracing::info!(authority = %authority, "Loaded node keypair");

        // 2. Load BLS keypair
        let bls_keypair = load_bls_keypair(&config.bls_keypair)?;
        tracing::info!("Loaded BLS keypair");

        // 3. Create RPC client
        let rpc_config = RpcConfig {
            endpoints: vec![rpc_url.to_string()],
            ..Default::default()
        };
        let rpc = RpcClient::new(rpc_config)
            .map_err(|e| ContextError::RpcClient(e.to_string()))?;

        // 4. Open storage
        let storage = StorageService::open(Path::new(&config.storage_path))?;

        // 5. Fetch initial on-chain state
        let system = rpc
            .get_system()
            .await
            .map_err(|e| ContextError::ChainState(format!("Failed to fetch system: {}", e)))?;

        let epoch = rpc
            .get_epoch()
            .await
            .map_err(|e| ContextError::ChainState(format!("Failed to fetch epoch: {}", e)))?;

        // 6. Get or register node
        let node = match rpc.get_node(&authority).await {
            Ok(node) => {
                tracing::info!(node_id = node.id.as_u64(), "Found existing node account");
                node
            }
            Err(e) => {
                return Err(ContextError::ChainState(format!(
                    "Node account not found: {}. Register with `tape node register` first.", e
                )));
            }
        };

        // 7. Initialize control plane cache (load persisted NodeStatus)
        let node_status = storage
            .store
            .get_node_status()
            .unwrap_or(None)
            .unwrap_or_default();
        let control_plane = ControlPlane::new(system, epoch, node, node_status);

        // 8. Initialize metrics registry and node metrics
        let registry = MetricsRegistry::init();
        let metrics = NodeMetrics::with_registry(registry.prometheus_registry());

        Ok(Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
            bls_keypair: Arc::new(bls_keypair),
            rpc: Arc::new(rpc),
            storage: Arc::new(storage),
            control_plane: Arc::new(control_plane),
            metrics: Arc::new(metrics),
        }))
    }
}

impl<S: Store> NodeContext<S> {
    /// Construct context with a custom storage backend.
    ///
    /// Use this for testing with in-memory stores or other backends.
    pub fn new(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        rpc: RpcClient<SolanaRpc>,
        storage: StorageService<S>,
        control_plane: ControlPlane,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
            bls_keypair: Arc::new(bls_keypair),
            rpc: Arc::new(rpc),
            storage: Arc::new(storage),
            control_plane: Arc::new(control_plane),
            metrics: Arc::new(NodeMetrics::new()),
        })
    }

    /// Get this node's public key (authority).
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Check if this node is in the current committee.
    pub fn is_in_committee(&self) -> bool {
        self.control_plane.is_in_committee()
    }

    /// Get this node's assigned spools.
    pub fn our_spools(&self) -> Vec<tape_core::spooler::SpoolIndex> {
        self.control_plane.get_our_spools()
    }
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
