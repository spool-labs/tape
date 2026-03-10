use serde::Deserialize;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use tape_core::types::BasisPoints;

use super::api::NodeApiConfig;
use super::expand_path;
use super::recovery::{RawRecoveryConfig, RecoveryConfig};
use super::tls::TlsConfig;

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
        let path = expand_path(path.as_ref().to_string_lossy().as_ref());
        let contents = fs::read_to_string(path)?;
        Self::from_yaml_str(&contents)
    }

    /// Load configuration from a YAML string.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let raw: RawNodeConfig = serde_yaml::from_str(yaml)?;
        raw.try_into()
    }
}

/// Default node config file path (~/.tape/node.yaml).
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

/// Raw configuration as loaded from YAML (before validation).
#[derive(Debug, Clone, Deserialize)]
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

    /// Recovery subsystem settings.
    #[serde(default)]
    pub recovery: RawRecoveryConfig,

    /// Node API transport security and ingress limits.
    ///
    /// `alias = "api_hardening"` preserves compatibility with earlier drafts.
    #[serde(default, alias = "api_hardening")]
    pub node_api: NodeApiConfig,
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
            recovery: raw.recovery.build(),
            node_api: raw.node_api,
        })
    }
}

fn default_version() -> u32 {
    1
}

fn solana_key_default() -> String {
    "~/.config/solana/id.json".to_string()
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
    fn test_parse_yaml_expands_storage_path() {
        let config = NodeConfig::from_yaml_str(
            r#"
name: "test"
tls_keypair: "~/.tape/tls.key"
bls_keypair: "~/.tape/bls.key"
bind_address: "0.0.0.0:8080"
public_host: "test"
public_port: 443
storage_path: "~/tape/data"
"#,
        )
        .unwrap();

        assert!(!config.storage_path.starts_with('~'));
        assert!(!config.tls_keypair.to_string_lossy().starts_with('~'));
    }

    #[test]
    fn test_parse_yaml_recovery_override() {
        let config = NodeConfig::from_yaml_str(
            r#"
name: "test"
tls_keypair: "/test"
bls_keypair: "/test"
bind_address: "0.0.0.0:8080"
public_host: "test"
public_port: 443
storage_path: "/test"
recovery:
  spool_sync_concurrency: 42
  repair_request_timeout: 12
"#,
        )
        .unwrap();

        assert_eq!(config.recovery.spool_sync_concurrency, 42);
        assert_eq!(config.recovery.repair_request_timeout.as_secs(), 12);
        assert_eq!(
            config.recovery.max_concurrent_track_syncs,
            RecoveryConfig::default().max_concurrent_track_syncs
        );
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
