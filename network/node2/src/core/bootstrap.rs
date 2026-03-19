use std::sync::Arc;

use peer_http::HttpApi;
use peer_manager::PeerManager;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use store_rocks::RocksStore;
use tape_store::TapeStore;

use crate::config::{AppConfig, NodeConfig, load_bls_keypair_from_config, load_node_keypair};
use crate::context::{AppContext, NodeContextBuilder};
use crate::core::error::NodeError;

pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<RocksStore>, NodeError> {
    TapeStore::open_primary(&config.storage_path).map_err(|error| {
        NodeError::Store(format!(
            "failed to open storage at {}: {error}",
            config.storage_path
        ))
    })
}

pub fn build_rpc_client(config: &NodeConfig) -> Result<RpcClient<SolanaRpc>, NodeError> {
    let rpc_config = RpcConfig {
        endpoints: vec![config.rpc_url.clone()],
        ..RpcConfig::default()
    };

    RpcClient::new(rpc_config).map_err(NodeError::from)
}

pub async fn build_context(config: &AppConfig) -> Result<AppContext, NodeError> {
    let keypair = load_node_keypair(&config.node)?;
    let bls_keypair = load_bls_keypair_from_config(&config.node)?;
    let store = open_primary_store(&config.node)?;
    let rpc = build_rpc_client(&config.node)?;

    let peer_manager = Arc::new(PeerManager::new());
    let api = Arc::new(HttpApi::with_default_timeouts(peer_manager.clone()));

    NodeContextBuilder::new(
        config.node.clone(),
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
