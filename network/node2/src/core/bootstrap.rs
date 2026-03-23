use std::sync::Arc;

use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use store_rocks::RocksStore;
use tape_store::TapeStore;

use crate::config::node::NodeConfig;
use crate::context::{AppContext, NodeContextBuilder};
use crate::core::error::NodeError;

pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<RocksStore>, NodeError> {
    TapeStore::open_primary_with_compaction_rate_limit(
        &config.store.path,
        config.store.compaction_mb_per_sec,
    )
    .map_err(|error| {
        NodeError::Store(format!(
            "failed to open storage at {}: {error}",
            config.store.path.display()
        ))
    })
}

pub fn build_rpc_client(config: &NodeConfig) -> Result<RpcClient<SolanaRpc>, NodeError> {
    let rpc_config = RpcConfig {
        endpoints: vec![config.solana.rpc.clone()],
        ..RpcConfig::default()
    };

    RpcClient::new(rpc_config).map_err(NodeError::from)
}

pub async fn build_context(config: &NodeConfig) -> Result<AppContext, NodeError> {
    let keypair = config.load_node_keypair()?;
    let bls_keypair = config.load_bls_keypair()?;
    let store = open_primary_store(config)?;
    let rpc = build_rpc_client(config)?;

    let peer_manager = Arc::new(PeerManager::new());
    let api = Arc::new(HttpApi::with_default_timeouts(peer_manager.clone()));

    NodeContextBuilder::new(
        config.clone(),
        keypair,
        bls_keypair,
        store,
        rpc,
        peer_manager,
        api,
    )
    .build()
    .await
}
