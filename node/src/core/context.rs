//! Node context — central shared state for the storage node.
//!
//! `NodeContext` holds all shared dependencies that runtime components need.
//! Every component receives `Arc<NodeContext>` instead of individual dependencies.

use std::sync::Arc;

use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use store::Store;
use tape_core::bls::BlsPrivateKey;
use tape_crypto::Pubkey;
use tape_store::TapeStore;

use super::config::NodeConfig;
use super::utils::current_timestamp;

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
/// Generic over storage backend `S`. Use [`NodeContext::new`] with a concrete
/// store type (e.g. `RocksStore` for production, `MemoryStore` for tests).
pub struct NodeContext<S: Store> {
    /// Node configuration.
    pub config: Arc<NodeConfig>,
    /// This node's authority keypair.
    pub keypair: Arc<Keypair>,
    /// BLS private key for committee signing.
    pub bls_keypair: Arc<BlsPrivateKey>,
    /// Typed storage layer.
    pub store: Arc<TapeStore<S>>,
    /// Time source used by FSM/epoch logic.
    pub now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl<S: Store> NodeContext<S> {
    /// Construct context with a custom storage backend.
    pub fn new(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        store: TapeStore<S>,
    ) -> Arc<Self> {
        Self::new_with_clock(config, keypair, bls_keypair, store, Arc::new(current_timestamp))
    }

    /// Construct context with a custom time source (for deterministic tests).
    pub fn new_with_clock(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        store: TapeStore<S>,
        now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
            bls_keypair: Arc::new(bls_keypair),
            store: Arc::new(store),
            now_fn,
        })
    }

    /// Get this node's public key (authority).
    pub fn pubkey(&self) -> Pubkey {
        self.keypair.pubkey()
    }

    /// Current timestamp for FSM and epoch decisions.
    pub fn now(&self) -> i64 {
        (self.now_fn)()
    }
}
