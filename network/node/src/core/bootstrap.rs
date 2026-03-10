use solana_sdk::signature::Keypair;
use store_rocks::RocksStore;
use tape_core::bls::BlsPrivateKey;
use tape_sdk::{load_bls_keypair, load_solana_keypair};
use tape_store::TapeStore;

use super::config::NodeConfig;

#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    #[error("failed to load node keypair from {path}: {message}")]
    NodeKeypair { path: String, message: String },

    #[error("failed to load BLS keypair from {path}: {message}")]
    BlsKeypair { path: String, message: String },

    #[error("failed to open storage at {path}: {message}")]
    Storage { path: String, message: String },
}

pub fn load_node_keypair(config: &NodeConfig) -> Result<Keypair, BootstrapError> {
    let path = config.node_keypair.as_str();
    load_solana_keypair(config.node_keypair.as_ref()).map_err(|e| BootstrapError::NodeKeypair {
        path: path.to_string(),
        message: e.to_string(),
    })
}

pub fn load_bls_keypair_from_config(config: &NodeConfig) -> Result<BlsPrivateKey, BootstrapError> {
    let path = &config.bls_keypair;
    load_bls_keypair(path).map_err(|e| BootstrapError::BlsKeypair {
        path: path.display().to_string(),
        message: e.to_string(),
    })
}

pub fn open_primary_store(config: &NodeConfig) -> Result<TapeStore<RocksStore>, BootstrapError> {
    let path = &config.storage_path;
    TapeStore::open_primary(path).map_err(|e| BootstrapError::Storage {
        path: path.clone(),
        message: e.to_string(),
    })
}
