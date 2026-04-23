//! Shared runtime state for every command: config, RPC client, payer
//! keypair, active cassette, chosen output format. Commands take `&Context`
//! and rely on it for everything environmental.

use std::path::{Path, PathBuf};

use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use tape_crypto::ed25519::Keypair;
use tape_sdk::keys::helpers::{load_ed25519_keypair, load_solana_keypair};
use tape_sdk::tapedrive::Tapedrive;
use peer_http::HttpApi;

use crate::cluster;
use crate::config::{self, CliConfig};
use crate::error::{Error, Result};
use crate::output::OutputFormat;

/// Defaults and runtime state every command shares. Built once in `main`.
pub struct Context {
    pub rpc_url: String,
    pub payer: Keypair,
    pub payer_path: PathBuf,
    pub config_path: PathBuf,
    pub config: CliConfig,
    pub active_cassette: Option<PathBuf>,
    pub output: OutputFormat,
}

impl Context {
    /// Resolve all global flags into a concrete runtime context.
    ///
    /// Precedence for each setting (highest wins):
    /// 1. command-line flag
    /// 2. environment variable
    /// 3. config file (`~/.tape/cli-config.yaml`)
    /// 4. hard-coded default
    pub fn load(
        cli_rpc_url: Option<String>,
        cli_keypair: Option<PathBuf>,
        cli_config_path: Option<PathBuf>,
        output: OutputFormat,
    ) -> Result<Self> {
        let config_path = cli_config_path.unwrap_or_else(config::default_config_path);
        let cfg = CliConfig::load(&config_path)?;

        let rpc_url = cli_rpc_url
            .or_else(|| std::env::var("TAPE_RPC_URL").ok())
            .or_else(|| cfg.rpc_url.clone())
            .unwrap_or_else(|| cluster::DEVNET.to_string());
        let rpc_url = cluster::resolve(&rpc_url);

        let keypair_path = cli_keypair
            .or_else(|| std::env::var("TAPE_KEYPAIR").ok().map(PathBuf::from))
            .or_else(|| cfg.keypair.clone())
            .unwrap_or_else(default_solana_keypair);
        let keypair_path = config::expand(&keypair_path);

        let payer =
            load_ed25519_keypair(&keypair_path).map_err(|e| Error::Keypair(e.to_string()))?;

        let active_cassette = cfg.active_cassette.clone().map(|p| config::expand(&p));

        Ok(Self {
            rpc_url,
            payer,
            payer_path: keypair_path,
            config_path,
            config: cfg,
            active_cassette,
            output,
        })
    }

    /// Build a bare RPC client. Used by commands that talk directly to the
    /// chain without needing the higher-level SDK (e.g. `tape balance`).
    pub fn rpc_client(&self) -> Result<RpcClient<SolanaRpc>> {
        RpcClient::new(RpcConfig {
            endpoints: vec![self.rpc_url.clone()],
            ..Default::default()
        })
        .map_err(|e| Error::Other(format!("rpc client: {e}")))
    }

    /// Build a Tapedrive SDK client. Used by `read`/`write`.
    pub fn sdk(&self) -> Result<Tapedrive<SolanaRpc, HttpApi>> {
        let rpc = SolanaRpc::new(RpcConfig {
            endpoints: vec![self.rpc_url.clone()],
            ..Default::default()
        })
        .map_err(|e| Error::Other(format!("solana rpc: {e}")))?;
        // Tapedrive takes the payer keypair by value; clone so the Context
        // can stay intact for later commands in the same process.
        let payer = clone_keypair(&self.payer)?;
        Ok(Tapedrive::new(rpc, payer))
    }

    /// Resolve which cassette to operate on, preferring the CLI flag,
    /// falling back to the `use`'d default. Errors with a friendly message
    /// when neither is set.
    pub fn require_cassette(&self, flag: Option<&Path>) -> Result<PathBuf> {
        if let Some(path) = flag {
            return Ok(config::expand(path));
        }
        self.active_cassette
            .clone()
            .ok_or(Error::NoActiveCassette)
    }

    /// Persist the current in-memory config back to disk.
    pub fn save_config(&self) -> Result<()> {
        self.config.save(&self.config_path)
    }
}

fn default_solana_keypair() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".config/solana/id.json"))
        .unwrap_or_else(|| PathBuf::from("~/.config/solana/id.json"))
}

/// `tape_crypto::ed25519::Keypair` doesn't implement `Clone`; round-trip
/// through the Solana-compatible byte array to duplicate it.
fn clone_keypair(kp: &Keypair) -> Result<Keypair> {
    let bytes = kp.to_keypair_bytes();
    let solana_kp = solana_sdk::signature::Keypair::try_from(&bytes[..])
        .map_err(|e| Error::Keypair(format!("reconstruct: {e}")))?;
    Keypair::from_solana_keypair(&solana_kp).map_err(|e| Error::Keypair(e.to_string()))
}

// Required by load_solana_keypair in some call paths; keep a shim so
// callers that only have a `Path` can resolve a Solana-native Keypair too.
#[allow(dead_code)]
pub(crate) fn load_payer_as_solana(path: &Path) -> Result<solana_sdk::signature::Keypair> {
    load_solana_keypair(path).map_err(|e| Error::Keypair(e.to_string()))
}
