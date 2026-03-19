use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use solana_sdk::signature::Keypair;
use tape_core::bls::BlsPrivateKey;
use tape_core::types::SlotNumber;
use tape_retry::RetryConfig;
use tape_sdk::{load_bls_keypair, load_solana_keypair};

use crate::core::error::NodeError;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: usize,
    pub max_blocking_threads: usize,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub node_keypair: String,
    pub bls_keypair: PathBuf,
    pub rpc_url: String,
    pub storage_path: String,
    pub start_slot: SlotNumber,
}

impl NodeConfig {
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, NodeError> {
        let path = expand_path(path.as_ref());
        let contents = fs::read_to_string(&path).map_err(|error| {
            NodeError::Config(format!("failed to read config {}: {error}", path.display()))
        })?;
        Self::from_yaml_str(&contents)
    }

    pub fn from_yaml_str(yaml: &str) -> Result<Self, NodeError> {
        let raw: RawNodeConfig = serde_yaml::from_str(yaml)
            .map_err(|error| NodeError::Config(format!("failed to parse YAML config: {error}")))?;

        let config = Self {
            node_keypair: expand_path(&raw.node_keypair).display().to_string(),
            bls_keypair: expand_path(&raw.bls_keypair),
            rpc_url: raw.rpc_url,
            storage_path: expand_path(&raw.storage_path).display().to_string(),
            start_slot: raw.start_slot,
        };

        if config.rpc_url.trim().is_empty() {
            return Err(NodeError::Config("rpc_url is required".to_string()));
        }

        if config.storage_path.trim().is_empty() {
            return Err(NodeError::Config("storage_path is required".to_string()));
        }

        Ok(config)
    }
}

#[derive(Debug, Clone)]
pub struct HttpConfig {
    pub bind_addr: SocketAddr,
    pub concurrency_limit: usize,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct ChannelConfig {
    pub parsed_block_capacity: usize,
    pub replay_batch_capacity: usize,
}

#[derive(Debug, Clone)]
pub struct BlockIngestorConfig {
    pub start_slot: SlotNumber,
    pub fetch_retry: RetryConfig,
}

#[derive(Debug, Clone)]
pub struct EpochManagerConfig {
}

#[derive(Debug, Clone)]
pub struct EpochLifecycleConfig {
    /// Retry config for Solana transaction submissions.
    pub tx_retry: RetryConfig,
    /// Interval for lifecycle tasks.
    pub interval: Duration,
}

impl Default for EpochLifecycleConfig {
    fn default() -> Self {
        Self {
            tx_retry: RetryConfig::infinite(),
            interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SpoolManagerConfig {
    pub max_parallel_spools: usize,
    pub sync_batch_size: usize,
    pub scan_batch_size: usize,
    pub repair_batch_size: usize,
    pub recover_batch_size: usize,
    pub locked_spool_retention_epochs: u64,
    pub peer_retry: RetryConfig,
}

impl Default for SpoolManagerConfig {
    fn default() -> Self {
        Self {
            max_parallel_spools: 4,
            sync_batch_size: 100,
            scan_batch_size: 100,
            repair_batch_size: 10,
            recover_batch_size: 10,
            locked_spool_retention_epochs: 4,
            peer_retry: RetryConfig::ten(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub max_snapshot_items: usize,
}

#[derive(Debug, Clone)]
pub struct ReplayConfig;

#[derive(Debug, Clone)]
pub struct StateConfig;

#[derive(Debug, Clone)]
pub struct GcConfig {
    pub enabled: bool,
    pub scan_interval: Duration,
    pub track_batch_size: usize,
    pub slice_batch_size: usize,
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub node: NodeConfig,
    pub http: HttpConfig,
    pub channels: ChannelConfig,
    pub block: BlockIngestorConfig,
    pub epoch: EpochManagerConfig,
    pub epoch_lifecycle: EpochLifecycleConfig,
    pub spool: SpoolManagerConfig,
    pub snapshot: SnapshotConfig,
    pub replay: ReplayConfig,
    pub state: StateConfig,
    pub gc: GcConfig,
}

impl AppConfig {
    pub fn production(node: NodeConfig) -> Result<Self, NodeError> {
        let available_threads = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(4);

        let worker_threads = available_threads.max(4);

        Ok(Self {
            runtime: RuntimeConfig {
                worker_threads,
                max_blocking_threads: worker_threads.saturating_mul(4),
            },
            node: node.clone(),
            http: HttpConfig {
                bind_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3000),
                concurrency_limit: 2048,
                request_timeout: Duration::from_secs(5),
            },
            channels: ChannelConfig {
                parsed_block_capacity: 256,
                replay_batch_capacity: 256,
            },
            block: BlockIngestorConfig {
                start_slot: node.start_slot,
                fetch_retry: RetryConfig::infinite(),
            },
            epoch: EpochManagerConfig {
            },
            epoch_lifecycle: EpochLifecycleConfig::default(),
            spool: SpoolManagerConfig {
                max_parallel_spools: worker_threads.clamp(4, 64),
                sync_batch_size: 100,
                scan_batch_size: 100,
                repair_batch_size: 10,
                recover_batch_size: 10,
                locked_spool_retention_epochs: 4,
                peer_retry: RetryConfig::ten(),
            },
            snapshot: SnapshotConfig {
                max_snapshot_items: 10_000,
            },
            replay: ReplayConfig,
            state: StateConfig,
            gc: GcConfig {
                enabled: true,
                scan_interval: Duration::from_secs(60),
                track_batch_size: 256,
                slice_batch_size: 256,
            },
        })
    }
}

pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

pub fn load_node_keypair(config: &NodeConfig) -> Result<Keypair, NodeError> {
    load_solana_keypair(Path::new(&config.node_keypair)).map_err(|error| {
        NodeError::Keypair(format!(
            "failed to load node keypair from {}: {error}",
            config.node_keypair
        ))
    })
}

pub fn load_bls_keypair_from_config(config: &NodeConfig) -> Result<BlsPrivateKey, NodeError> {
    load_bls_keypair(&config.bls_keypair).map_err(|error| {
        NodeError::Keypair(format!(
            "failed to load BLS keypair from {}: {error}",
            config.bls_keypair.display()
        ))
    })
}

#[derive(Debug, Deserialize)]
struct RawNodeConfig {
    #[serde(default = "default_node_keypair")]
    node_keypair: String,
    bls_keypair: PathBuf,
    rpc_url: String,
    storage_path: PathBuf,
    #[serde(default = "default_start_slot")]
    start_slot: SlotNumber,
}

fn default_node_keypair() -> String {
    "~/.config/solana/id.json".to_string()
}

fn default_start_slot() -> SlotNumber {
    SlotNumber(1)
}

fn expand_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    let raw = path.to_string_lossy();

    if raw == "~" {
        return dirs::home_dir().unwrap_or_else(|| path.to_path_buf());
    }

    if let Some(suffix) = raw.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(suffix);
        }
    }

    path.to_path_buf()
}
