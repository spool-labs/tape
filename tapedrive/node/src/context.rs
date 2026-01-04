//! Node context - central shared state for the storage node.

use std::path::Path;
use std::sync::Arc;

use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use store::Store;
use store_rocks::RocksStore;
use tape_client::{RpcConfig, TapeClient};
use tape_crypto::Pubkey;

use crate::config::NodeConfig;
use crate::control_plane::ControlPlane;
use crate::metrics::NodeMetrics;
use crate::StorageService;

/// Error type for context initialization.
#[derive(Debug, thiserror::Error)]
pub enum ContextError {
    #[error("failed to load keypair: {0}")]
    Keypair(String),

    #[error("failed to initialize RPC client: {0}")]
    RpcClient(String),

    #[error("failed to open storage: {0}")]
    Storage(#[from] crate::StorageError),

    #[error("failed to fetch on-chain state: {0}")]
    ChainState(String),

    #[error("node registration failed: {0}")]
    Registration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
    /// Tape RPC client for chain interactions.
    pub rpc: Arc<TapeClient>,
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
    /// 2. Creating the RPC client
    /// 3. Opening RocksDB storage
    /// 4. Fetching initial on-chain state (System, Epoch, Node)
    /// 5. Auto-registering node if account doesn't exist
    /// 6. Initializing the ControlPlane cache
    /// 7. Initializing metrics
    pub async fn from_config(config: NodeConfig) -> Result<Arc<Self>, ContextError> {
        // 1. Load keypair
        let keypair = load_keypair(&config.solana_keypair_path)?;
        let authority = keypair.pubkey();
        tracing::info!(authority = %authority, "Loaded node keypair");

        // 2. Create RPC client
        let rpc_config = RpcConfig {
            endpoints: vec![config.solana_rpc_url.clone()],
            ..Default::default()
        };
        let rpc = TapeClient::new(rpc_config)
            .map_err(|e| ContextError::RpcClient(e.to_string()))?;

        // 3. Open storage
        let storage = StorageService::open(Path::new(&config.storage_path))?;

        // 4. Fetch initial on-chain state
        let system = rpc
            .get_system()
            .await
            .map_err(|e| ContextError::ChainState(format!("Failed to fetch system: {}", e)))?;

        let epoch = rpc
            .get_epoch()
            .await
            .map_err(|e| ContextError::ChainState(format!("Failed to fetch epoch: {}", e)))?;

        // 5. Get or register node
        let node = match rpc.get_node(&authority).await {
            Ok(node) => {
                tracing::info!(node_id = node.id.as_u64(), "Found existing node account");
                node
            }
            Err(e) => {
                // TODO: Implement auto-registration if enabled in config
                // For now, just error out
                return Err(ContextError::ChainState(format!(
                    "Node account not found and auto-registration not implemented: {}",
                    e
                )));
            }
        };

        // 6. Initialize control plane cache
        let control_plane = ControlPlane::new(system, epoch, node, authority);

        // 7. Initialize metrics
        let metrics = NodeMetrics::new();

        Ok(Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
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
        rpc: TapeClient,
        storage: StorageService<S>,
        control_plane: ControlPlane,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
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

/// Load a Solana keypair from a JSON file.
fn load_keypair(path: &str) -> Result<Keypair, ContextError> {
    let keypair_bytes = std::fs::read(path)
        .map_err(|e| ContextError::Keypair(format!("Failed to read keypair file: {}", e)))?;

    let keypair_json: Vec<u8> = serde_json::from_slice(&keypair_bytes)
        .map_err(|e| ContextError::Keypair(format!("Failed to parse keypair JSON: {}", e)))?;

    Keypair::from_bytes(&keypair_json)
        .map_err(|e| ContextError::Keypair(format!("Invalid keypair bytes: {}", e)))
}

#[cfg(test)]
mod tests {
    // Tests would require mocking the RPC client
}
