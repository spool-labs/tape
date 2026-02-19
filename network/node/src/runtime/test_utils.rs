//! Shared test helpers — config and context factories.

use std::path::PathBuf;
use std::sync::Arc;

use rpc_client::RpcClient;
use rpc_litesvm::LiteSvmRpc;
use solana_sdk::signature::Keypair;
use tape_core::bls::BlsPrivateKey;
use tape_store::{MemoryStore, TapeStore};

use crate::runtime::RecoveryConfig;
use crate::runtime::{NodeApiConfig, NodeConfig, TlsConfig};
use crate::runtime::NodeContext;

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
