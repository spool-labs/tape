use std::path::{Path, PathBuf};

use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use tape_crypto::ed25519::Keypair;
use tape_sdk::keys::helpers::load_ed25519_keypair;

use crate::error::{Error, Result};

/// Shared state for admin operations: RPC client, payer keypair, and paths.
pub struct Context {
    pub rpc: RpcClient<SolanaRpc>,
    pub payer: Keypair,
    pub payer_path: PathBuf,
    pub rpc_url: String,
}

impl Context {
    pub fn new(rpc_url: impl Into<String>, payer_path: &Path) -> Result<Self> {
        let rpc_url = rpc_url.into();
        let config = RpcConfig {
            endpoints: vec![rpc_url.clone()],
            ..Default::default()
        };
        let rpc = RpcClient::new(config)
            .map_err(|e| Error::Other(format!("create rpc client: {e}")))?;
        let payer = load_ed25519_keypair(payer_path)
            .map_err(|e| Error::Keypair(e.to_string()))?;
        Ok(Self {
            rpc,
            payer,
            payer_path: payer_path.to_path_buf(),
            rpc_url,
        })
    }
}
