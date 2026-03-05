//! Core module — shared utilities and runtime primitives for the storage node.
//!
//! This module centralizes code that is used across multiple components:
//! - `retry`: Shared retry infrastructure with exponential backoff
//! - `helpers`: Shared helper functions (path expansion, epoch helpers, etc)
//! - `committee`: Helpers for committee membership and membership lookups
//! - `config`: Node/runtime configuration structs and helpers
//! - `context`: Shared node context and builder
//! - `peer`: Peer cooldown and peer service primitives
//! - `stats`: Runtime statistics counters

pub mod retry;
pub mod helpers;
pub mod committee;
pub mod config;
pub mod context;
pub mod peer;
pub mod stats;
pub mod throttle;

pub use retry::{Backoff, BackoffConfig, retry_with_backoff, compute_delay};
pub use config::{
    ConfigError, IngressLimitsConfig, NodeApiConfig, NodeConfig, RecoveryConfig, TlsConfig,
    TransportSecurityConfig, default_config_content, default_config_path,
};
pub use context::{ContextError, NodeContext, NodeContextBuilder};
pub use helpers::{expand_path, has_missing_slices, validate_slice_entry};
pub use peer::{PeerHandle, PeerService, PeerServiceError};
pub use stats::RuntimeStats;
pub use throttle::RefreshThrottle;

#[cfg(test)]
pub mod test_utils {
    //! Shared test helpers — config and context factories.

    use std::path::PathBuf;
    use std::sync::Arc;

    use rpc_client::RpcClient;
    use rpc_litesvm::LiteSvmRpc;
    use solana_sdk::signature::Keypair;
    use tape_core::bls::BlsPrivateKey;
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

    pub fn test_context() -> Arc<NodeContext<MemoryStore, LiteSvmRpc>> {
        let store = TapeStore::new(MemoryStore::new());
        NodeContext::new(
            test_config(),
            Keypair::new(),
            BlsPrivateKey::from_random(),
            store,
            RpcClient::from_rpc(LiteSvmRpc::new()),
        )
    }
}
