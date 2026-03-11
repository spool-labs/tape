//! Core module — shared utilities and runtime primitives for the storage node.
//!
//! This module centralizes code that is used across multiple components:
//! - `bootstrap`: Config-derived startup I/O helpers
//! - `config`: Node/runtime configuration structs and helpers
//! - `context`: Shared node context and builder
//! - `peer_call`: Shared peer retry/report helper
//! - `stats`: Runtime statistics counters

mod bootstrap;
mod config;
mod context;
mod peer_call;
mod stats;

pub use bootstrap::{
    BootstrapError, load_bls_keypair_from_config, load_node_keypair, open_primary_store,
};
pub use config::{
    ConfigError, IngressLimitsConfig, NodeApiConfig, NodeConfig, RecoveryConfig, TlsConfig,
    TransportSecurityConfig, default_config_path,
};
pub use context::{ContextError, NodeContext, NodeContextBuilder};
pub use peer_call::call_peer;

#[cfg(test)]
pub mod test_utils {
    //! Shared test helpers — config and context factories.

    use std::path::PathBuf;
    use std::sync::Arc;

    use peer_memory::MemoryApi;
    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_core::bls::BlsPrivateKey;
    use peer_manager::PeerManager;
    use tape_store::{MemoryStore, TapeStore};

    use super::{NodeApiConfig, NodeConfig, NodeContext, RecoveryConfig, TlsConfig};

    pub fn test_config() -> NodeConfig {
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

    pub fn test_context() -> Arc<NodeContext<MemoryStore, MemoryApi, LiteSvmRpc>> {
        let peer_manager = Arc::new(PeerManager::new());
        let api = Arc::new(MemoryApi::noop());
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(
            test_config(),
            Keypair::new(),
            BlsPrivateKey::from_random(),
            store,
            RpcClient::from_rpc(LiteSvmRpc::new()),
            peer_manager,
            api,
        )
    }
}
