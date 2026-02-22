//! Node configuration.

use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tape_core::types::BasisPoints;

use crate::core::expand_path;

/// Recovery subsystem configuration parameters.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum concurrent track sync tasks.
    pub max_concurrent_track_syncs: usize,
    /// Maximum concurrent slice downloads across all tracks.
    pub max_concurrent_slice_syncs: usize,
    /// Maximum queued recovery tasks before backpressure.
    pub recovery_track_concurrency: usize,
    /// Maximum concurrent spool sync operations.
    pub spool_sync_concurrency: usize,
    /// Timeout for individual repair requests to helpers.
    pub repair_request_timeout: Duration,
    /// Timeout for individual slice download requests.
    pub slice_request_timeout: Duration,
    /// Timeout for metadata requests to peers.
    pub metadata_request_timeout: Duration,
    /// Total timeout before spool sync falls back to direct recovery.
    pub spool_sync_recovery_timeout: Duration,
    /// Maximum time to defer live uploads during recovery.
    pub max_total_defer: Duration,
    /// Delay between track sync retry attempts.
    pub track_sync_retry_delay: Duration,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_concurrent_track_syncs: 100,
            max_concurrent_slice_syncs: 2000,
            recovery_track_concurrency: 1000,
            spool_sync_concurrency: 10,
            repair_request_timeout: Duration::from_secs(45),
            slice_request_timeout: Duration::from_secs(45),
            metadata_request_timeout: Duration::from_secs(5),
            spool_sync_recovery_timeout: Duration::from_secs(12 * 3600),
            max_total_defer: Duration::from_secs(120),
            track_sync_retry_delay: Duration::from_secs(30),
        }
    }
}

/// Error type for configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("failed to parse YAML config: {0}")]
    ParseYaml(#[from] serde_yaml::Error),

    #[error("invalid bind address: {0}")]
    InvalidBindAddress(String),
}

/// Configuration for a storage node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Config file version.
    pub version: u32,

    /// Node name for identification.
    pub name: String,

    /// Path to Ed25519 TLS keypair file (for HTTPS certificates).
    pub tls_keypair: PathBuf,

    /// Path to BLS keypair file (for committee signing).
    pub bls_keypair: PathBuf,

    /// Path to node keypair file (JSON format, used for signing on-chain transactions).
    pub node_keypair: String,

    /// Address to bind the server to.
    pub bind_address: SocketAddr,

    /// Public hostname for this node.
    pub public_host: String,

    /// Public port for this node.
    pub public_port: u16,

    /// TLS configuration.
    pub tls: TlsConfig,

    /// Path to storage directory.
    pub storage_path: String,

    /// Block polling interval in milliseconds (default: 400ms).
    pub poll_interval_ms: Option<u64>,

    /// Number of concurrent spool sync operations (default: 4).
    pub sync_concurrency: Option<usize>,

    /// Batch size for sync requests (default: 1000).
    pub sync_batch_size: Option<usize>,

    /// Commission rate in basis points (0-10000). Used during registration.
    pub commission: Option<BasisPoints>,

    /// Recovery subsystem configuration.
    pub recovery: RecoveryConfig,

    /// Node API transport security and ingress limits.
    pub node_api: NodeApiConfig,
}

impl NodeConfig {
    /// Load configuration from a YAML file.
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        Self::from_yaml_str(&contents)
    }

    /// Load configuration from a YAML string.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let raw: RawNodeConfig = serde_yaml::from_str(yaml)?;
        raw.try_into()
    }

    /// Override the bind address.
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_address = addr;
        self
    }

    /// Get the network address as host:port string.
    pub fn network_address(&self) -> String {
        format!("{}:{}", self.public_host, self.public_port)
    }
}

/// Default node config file path (~/.tape/node.yaml).
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

/// Default node config content for initialization.
pub fn default_config_content() -> &'static str {
    r#"# Tapedrive Storage Node Configuration
version: 1

# Display name for this node
name: "my-node"

# Commission rate in basis points (0-10000, where 10000 = 100%)
commission: 500

# Keypairs (auto-generated by `tape node init`)
tls_keypair: ~/.tape/keys/tls.json
bls_keypair: ~/.tape/keys/bls.json
node_keypair: ~/.config/solana/id.json

# Local address to bind the server
bind_address: "0.0.0.0:8080"

# Public address other nodes use to reach this node
public_host: "127.0.0.1"
public_port: 8080

# TLS certificate (self-signed for development, provide paths for production)
tls:
  generate_self_signed: true
  # certificate_path: /path/to/cert.pem
  # key_path: /path/to/key.pem

# Directory for slice data storage
storage_path: ~/.tape/data

# Performance tuning (optional)
# poll_interval_ms: 400
# sync_concurrency: 4
# sync_batch_size: 1000

# Node API security and ingress limits (optional)
# node_api:
#   transport_security:
#     # Grace window for prior peer TLS keys during rotation smoothing
#     pin_ttl_secs: 90
#     # Max accepted keys per peer (current + prior)
#     pin_keys_max: 2
#     # Require TLS peer identity on protected routes.
#     peer_id_enforce: true
#   ingress_limits:
    # Enable public unauthenticated upload routes.
    # Set false to require only internal authenticated ingest.
    # public_ingest: true
#     # Per-endpoint body limits (bytes)
#     slice_body_max: 10485760
#     metadata_body_max: 1048576
#     sync_body_max: 1048576
#     repair_body_max: 1048576
#     inconsistency_body_max: 1048576
#     # Concurrency throttles for expensive routes (None = disabled)
#     sync_spool_limit: 64
#     repair_limit: 128
#     inconsistency_limit: 32
    # Optional public upload concurrency throttles (None = disabled)
    # public_slice_limit: 256
    # public_metadata_limit: 128
"#
}

/// Raw configuration as loaded from YAML (before validation).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RawNodeConfig {
    /// Config file version.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Node name for identification.
    pub name: String,

    /// Path to Ed25519 TLS keypair file (for HTTPS certificates).
    pub tls_keypair: PathBuf,

    /// Path to BLS keypair file (for committee signing).
    pub bls_keypair: PathBuf,

    /// Path to node keypair file (JSON format, for signing on-chain transactions).
    #[serde(default = "solana_key_default")]
    pub node_keypair: String,

    /// Address to bind the server to (as string for parsing).
    pub bind_address: String,

    /// Public hostname for this node.
    pub public_host: String,

    /// Public port for this node.
    pub public_port: u16,

    /// TLS configuration.
    #[serde(default)]
    pub tls: TlsConfig,

    /// Path to storage directory.
    pub storage_path: String,

    /// Block polling interval in milliseconds.
    #[serde(default)]
    pub poll_interval_ms: Option<u64>,

    /// Number of concurrent spool sync operations.
    #[serde(default)]
    pub sync_concurrency: Option<usize>,

    /// Batch size for sync requests.
    #[serde(default)]
    pub sync_batch_size: Option<usize>,

    /// Commission rate in basis points (0-10000). Used during registration.
    #[serde(default)]
    pub commission: Option<u64>,

    /// Node API transport security and ingress limits.
    ///
    /// `alias = "api_hardening"` preserves compatibility with earlier drafts.
    #[serde(default, alias = "api_hardening")]
    pub node_api: NodeApiConfig,
}

fn default_version() -> u32 {
    1
}

fn solana_key_default() -> String {
    "~/.config/solana/id.json".to_string()
}

impl TryFrom<RawNodeConfig> for NodeConfig {
    type Error = ConfigError;

    fn try_from(raw: RawNodeConfig) -> Result<Self, Self::Error> {
        let bind_address = raw
            .bind_address
            .parse()
            .map_err(|_| ConfigError::InvalidBindAddress(raw.bind_address.clone()))?;

        Ok(Self {
            version: raw.version,
            name: raw.name,
            tls_keypair: expand_path(raw.tls_keypair.to_string_lossy().as_ref()),
            bls_keypair: expand_path(raw.bls_keypair.to_string_lossy().as_ref()),
            node_keypair: expand_path(&raw.node_keypair).to_string_lossy().to_string(),
            bind_address,
            public_host: raw.public_host,
            public_port: raw.public_port,
            tls: raw.tls,
            storage_path: expand_path(&raw.storage_path).to_string_lossy().to_string(),
            poll_interval_ms: raw.poll_interval_ms,
            sync_concurrency: raw.sync_concurrency,
            sync_batch_size: raw.sync_batch_size,
            commission: raw.commission.map(BasisPoints),
            recovery: RecoveryConfig::default(),
            node_api: raw.node_api,
        })
    }
}

/// Node API configuration root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeApiConfig {
    /// Transport security and pinning controls.
    #[serde(default)]
    pub transport_security: TransportSecurityConfig,

    /// Request sizing and endpoint concurrency controls.
    #[serde(default)]
    pub ingress_limits: IngressLimitsConfig,
}

impl Default for NodeApiConfig {
    fn default() -> Self {
        Self {
            transport_security: TransportSecurityConfig::default(),
            ingress_limits: IngressLimitsConfig::default(),
        }
    }
}

/// Runtime controls for mTLS/pinning behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportSecurityConfig {
    /// Grace period (seconds) for prior TLS peer keys during key rotation.
    #[serde(default = "pin_ttl_default")]
    pub pin_ttl_secs: u64,

    /// Maximum accepted keys per peer in grace cache.
    #[serde(default = "pin_keys_default")]
    pub pin_keys_max: usize,

    /// Require peer TLS identity on protected routes.
    #[serde(default = "peer_id_default")]
    pub peer_id_enforce: bool,
}

impl Default for TransportSecurityConfig {
    fn default() -> Self {
        Self {
            pin_ttl_secs: pin_ttl_default(),
            pin_keys_max: pin_keys_default(),
            peer_id_enforce: peer_id_default(),
        }
    }
}

/// Runtime controls for API ingress body sizes and endpoint concurrency limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressLimitsConfig {
    /// Enable public unauthenticated ingest routes (/v1/tracks/* PUT).
    #[serde(default = "default_public_ingest")]
    pub public_ingest: bool,

    /// Maximum request body size for PUT slice.
    #[serde(default = "slice_body_default")]
    pub slice_body_max: usize,

    /// Maximum request body size for PUT metadata.
    #[serde(default = "metadata_body_default")]
    pub metadata_body_max: usize,

    /// Maximum request body size for sync spool requests.
    #[serde(default = "sync_body_default")]
    pub sync_body_max: usize,

    /// Maximum request body size for repair requests.
    #[serde(default = "repair_body_default")]
    pub repair_body_max: usize,

    /// Maximum request body size for inconsistency proof requests.
    #[serde(default = "inconsistency_body_default")]
    pub inconsistency_body_max: usize,

    /// Optional cap on concurrently handled sync_spool requests.
    #[serde(default = "sync_limit_default")]
    pub sync_spool_limit: Option<usize>,

    /// Optional cap on concurrently handled repair requests.
    #[serde(default = "repair_limit_default")]
    pub repair_limit: Option<usize>,

    /// Optional cap on concurrently handled inconsistency requests.
    #[serde(default = "inconsistency_limit_default")]
    pub inconsistency_limit: Option<usize>,

    /// Optional cap on concurrently handled public PUT slice requests.
    #[serde(default = "public_slice_default")]
    pub public_slice_limit: Option<usize>,

    /// Optional cap on concurrently handled public PUT metadata requests.
    #[serde(default = "public_metadata_default")]
    pub public_metadata_limit: Option<usize>,
}

fn pin_ttl_default() -> u64 {
    90
}

fn pin_keys_default() -> usize {
    2
}

fn peer_id_default() -> bool {
    true
}

fn slice_body_default() -> usize {
    10 * 1024 * 1024
}

fn default_public_ingest() -> bool {
    true
}

fn metadata_body_default() -> usize {
    1024 * 1024
}

fn sync_body_default() -> usize {
    1024 * 1024
}

fn repair_body_default() -> usize {
    1024 * 1024
}

fn inconsistency_body_default() -> usize {
    1024 * 1024
}

fn sync_limit_default() -> Option<usize> {
    Some(64)
}

fn repair_limit_default() -> Option<usize> {
    Some(128)
}

fn inconsistency_limit_default() -> Option<usize> {
    Some(32)
}

fn public_slice_default() -> Option<usize> {
    None
}

fn public_metadata_default() -> Option<usize> {
    None
}

impl Default for IngressLimitsConfig {
    fn default() -> Self {
        Self {
            public_ingest: default_public_ingest(),
            slice_body_max: slice_body_default(),
            metadata_body_max: metadata_body_default(),
            sync_body_max: sync_body_default(),
            repair_body_max: repair_body_default(),
            inconsistency_body_max: inconsistency_body_default(),
            sync_spool_limit: sync_limit_default(),
            repair_limit: repair_limit_default(),
            inconsistency_limit: inconsistency_limit_default(),
            public_slice_limit: public_slice_default(),
            public_metadata_limit: public_metadata_default(),
        }
    }
}

/// TLS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to TLS certificate file.
    #[serde(default)]
    pub certificate_path: Option<PathBuf>,

    /// Path to TLS key file.
    #[serde(default)]
    pub key_path: Option<PathBuf>,

    /// Whether to generate a self-signed certificate.
    #[serde(default = "self_signed_default")]
    pub generate_self_signed: bool,
}

fn self_signed_default() -> bool {
    true
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            certificate_path: None,
            key_path: None,
            generate_self_signed: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE_CONFIG: &str = r#"
name: "tape-node-1"
tls_keypair: "/etc/tape/tls.key"
bls_keypair: "/etc/tape/bls.key"
bind_address: "0.0.0.0:8080"
public_host: "node1.tapedrive.io"
public_port: 443
tls:
  generate_self_signed: true
storage_path: "/var/lib/tape/data"
"#;

    #[test]
    fn test_parse_yaml() {
        let config = NodeConfig::from_yaml_str(EXAMPLE_CONFIG).unwrap();

        assert_eq!(config.name, "tape-node-1");
        assert_eq!(config.tls_keypair, PathBuf::from("/etc/tape/tls.key"));
        assert_eq!(config.bls_keypair, PathBuf::from("/etc/tape/bls.key"));
        assert_eq!(config.bind_address.to_string(), "0.0.0.0:8080");
        assert_eq!(config.public_host, "node1.tapedrive.io");
        assert_eq!(config.public_port, 443);
        assert!(config.tls.generate_self_signed);
        assert_eq!(config.storage_path, "/var/lib/tape/data");
    }

    #[test]
    fn test_override_bind() {
        let config = NodeConfig::from_yaml_str(EXAMPLE_CONFIG).unwrap();
        let new_addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let config = config.with_bind_address(new_addr);

        assert_eq!(config.bind_address.to_string(), "127.0.0.1:9090");
    }

    #[test]
    fn test_invalid_bind() {
        let yaml = r#"
name: "test"
tls_keypair: "/test"
bls_keypair: "/test"
bind_address: "not_an_address"
public_host: "test"
public_port: 443
storage_path: "/test"
"#;
        let result = NodeConfig::from_yaml_str(yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ConfigError::InvalidBindAddress(_)));
    }

    #[test]
    fn test_default_config_path() {
        let path = default_config_path();
        assert!(path.to_string_lossy().contains("node.yaml"));
    }
}
