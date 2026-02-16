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
use tape_store::ops::{CommitteeOps, MetaOps};
use tape_store::TapeStore;

use super::config::NodeConfig;
use super::stats::RuntimeStats;
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
    /// Runtime statistics (atomic counters).
    pub stats: RuntimeStats,
    /// Time source used by FSM/epoch logic.
    pub now_fn: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// RPC client for on-chain operations (only available with `rpc` feature).
    #[cfg(feature = "rpc")]
    pub rpc: Option<Arc<rpc_client::RpcClient<rpc_solana::SolanaRpc>>>,
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
            stats: RuntimeStats::default(),
            now_fn,
            #[cfg(feature = "rpc")]
            rpc: None,
        })
    }

    /// Construct context with an RPC client for on-chain operations.
    #[cfg(feature = "rpc")]
    pub fn new_with_rpc(
        config: NodeConfig,
        keypair: Keypair,
        bls_keypair: BlsPrivateKey,
        store: TapeStore<S>,
        rpc: rpc_client::RpcClient<rpc_solana::SolanaRpc>,
    ) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            keypair: Arc::new(keypair),
            bls_keypair: Arc::new(bls_keypair),
            store: Arc::new(store),
            stats: RuntimeStats::default(),
            now_fn: Arc::new(current_timestamp),
            rpc: Some(Arc::new(rpc)),
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

    /// Look up our (node_id, member_index) in the current committee.
    /// Returns (0, 0) if committee not loaded or node not found.
    pub fn committee_identity(&self) -> (u64, u8) {
        let epoch = match self.store.get_current_epoch() {
            Ok(Some(e)) => e,
            _ => return (0, 0),
        };
        let committee = match self.store.get_committee(epoch) {
            Ok(Some(c)) => c,
            _ => return (0, 0),
        };
        let our_bls = match self.bls_keypair.public_key() {
            Ok(pk) => pk,
            Err(_) => return (0, 0),
        };
        let member_index = committee
            .iter()
            .position(|m| m.bls_pubkey == our_bls)
            .unwrap_or(0) as u8;
        let node_id = self
            .store
            .get_node_id()
            .ok()
            .flatten()
            .map(|id| id.0)
            .unwrap_or(0);
        (node_id, member_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::types::network::NetworkAddress;
    use tape_core::types::{EpochNumber, NodeId};
    use tape_store::ops::CommitteeOps;
    use tape_store::types::NodeInfo;
    use tape_store::MemoryStore;

    use crate::core::config::RecoveryConfig;
    use crate::core::{NodeApiConfig, NodeConfig, TlsConfig};

    fn test_config() -> NodeConfig {
        NodeConfig {
            version: 1,
            name: "test-node".to_string(),
            tls_keypair: PathBuf::from("/dev/null"),
            bls_keypair: PathBuf::from("/dev/null"),
            node_keypair: String::new(),
            bind_address: "127.0.0.1:0".parse().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 0,
            tls: TlsConfig::default(),
            storage_path: "/tmp".to_string(),
            poll_interval_ms: None,
            sync_concurrency: None,
            sync_batch_size: None,
            commission: None,
            recovery: RecoveryConfig::default(),
            node_api: NodeApiConfig::default(),
        }
    }

    #[test]
    fn committee_identity_lookup() {
        let bls_key = BlsPrivateKey::from_random();
        let our_bls = bls_key.public_key().unwrap();

        let store = tape_store::TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(
            test_config(),
            solana_sdk::signature::Keypair::new(),
            bls_key,
            store,
        );

        // Set epoch and committee with our key at position 2
        let epoch = EpochNumber(5);
        ctx.store.set_current_epoch(epoch).unwrap();

        let committee = vec![
            NodeInfo {
                node_address: tape_store::types::Pubkey::new([1u8; 32]),
                bls_pubkey: BlsPubkey::zeroed(),
                tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
                network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
                spools: vec![],
            },
            NodeInfo {
                node_address: tape_store::types::Pubkey::new([2u8; 32]),
                bls_pubkey: BlsPubkey::zeroed(),
                tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
                network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8001),
                spools: vec![],
            },
            NodeInfo {
                node_address: tape_store::types::Pubkey::new([3u8; 32]),
                bls_pubkey: our_bls,
                tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
                network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8002),
                spools: vec![],
            },
        ];
        ctx.store.put_committee(epoch, committee).unwrap();

        let (node_id, member_index) = ctx.committee_identity();
        assert_eq!(member_index, 2);
        assert_eq!(node_id, 0); // no node_id stored yet
    }

    #[test]
    fn committee_identity_missing() {
        let bls_key = BlsPrivateKey::from_random();
        let store = tape_store::TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(
            test_config(),
            solana_sdk::signature::Keypair::new(),
            bls_key,
            store,
        );

        // No epoch, no committee
        let (node_id, member_index) = ctx.committee_identity();
        assert_eq!(node_id, 0);
        assert_eq!(member_index, 0);
    }

    #[test]
    fn committee_identity_with_node_id() {
        let bls_key = BlsPrivateKey::from_random();
        let our_bls = bls_key.public_key().unwrap();

        let store = tape_store::TapeStore::new(MemoryStore::new());
        let ctx = NodeContext::new(
            test_config(),
            solana_sdk::signature::Keypair::new(),
            bls_key,
            store,
        );

        let epoch = EpochNumber(1);
        ctx.store.set_current_epoch(epoch).unwrap();
        ctx.store.set_node_id(NodeId(42)).unwrap();

        let committee = vec![NodeInfo {
            node_address: tape_store::types::Pubkey::new([1u8; 32]),
            bls_pubkey: our_bls,
            tls_pubkey: tape_store::types::Pubkey::new([0u8; 32]),
            network_address: NetworkAddress::new_ipv4([127, 0, 0, 1], 8000),
            spools: vec![],
        }];
        ctx.store.put_committee(epoch, committee).unwrap();

        let (node_id, member_index) = ctx.committee_identity();
        assert_eq!(node_id, 42);
        assert_eq!(member_index, 0);
    }
}
