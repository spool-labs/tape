use std::path::{Path, PathBuf};

use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use tape_cli_common::config::{self, CliConfig};
use tape_cli_common::{OutputFormat, cluster};
use tape_crypto::ed25519::Keypair;
use tape_sdk::keys::helpers::load_ed25519_keypair;

use crate::error::{Error, Result};

/// Shared state for admin operations: RPC client, payer keypair, and paths.
pub struct Context {
    pub rpc: RpcClient<SolanaRpc>,
    pub payer: Keypair,
    pub payer_path: PathBuf,
    pub rpc_url: String,
    pub output: OutputFormat,
}

impl Context {
    /// Build a context directly from explicit inputs — used by callers that
    /// embed tape-admin as a library (e.g. `tape-network` during bootstrap).
    /// Defaults output to text since library callers print directly.
    pub fn new(rpc_url: impl Into<String>, payer_path: &Path) -> Result<Self> {
        Self::build(rpc_url.into(), payer_path.to_path_buf(), OutputFormat::Text)
    }

    /// Build a context from the global CLI args, falling back to env vars
    /// and `~/.tape/cli-config.yaml` the same way `tape` does. Shared by
    /// every `tape-admin` subcommand.
    pub fn from_global_args(
        cli_rpc_url: Option<String>,
        cli_keypair: Option<PathBuf>,
        cli_config_path: Option<PathBuf>,
        output: OutputFormat,
    ) -> Result<Self> {
        let config_path = cli_config_path.unwrap_or_else(config::default_config_path);
        let cfg = CliConfig::load(&config_path)?;

        let rpc_url = cli_rpc_url
            .or_else(|| cfg.rpc_url.clone())
            .unwrap_or_else(|| cluster::DEVNET.to_string());
        let rpc_url = cluster::resolve(&rpc_url);

        let keypair_path = cli_keypair
            .or_else(|| cfg.keypair.clone())
            .unwrap_or_else(config::default_solana_keypair);
        let keypair_path = config::expand(&keypair_path);

        Self::build(rpc_url, keypair_path, output)
    }

    fn build(rpc_url: String, payer_path: PathBuf, output: OutputFormat) -> Result<Self> {
        let rpc_config = RpcConfig {
            endpoints: vec![rpc_url.clone()],
            ..Default::default()
        };
        let rpc = RpcClient::new(rpc_config)
            .map_err(|e| Error::Other(format!("create rpc client: {e}")))?;
        let payer =
            load_ed25519_keypair(&payer_path).map_err(|e| Error::Keypair(e.to_string()))?;
        Ok(Self {
            rpc,
            payer,
            payer_path,
            rpc_url,
            output,
        })
    }
}
