use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use tape_core::bls::BlsPrivateKey;
use tape_api::consts::NAME_LENGTH;
use tape_api::genesis::GenesisConfig;
use tape_core::types::BasisPoints;
use tape_crypto::ed25519::Keypair;
use tape_sdk::keys::helpers::{ensure_ed25519_keypair, load_bls_keypair, load_ed25519_keypair};

use crate::core::error::NodeError;
use super::{
    gateway::GatewayConfig,
    helpers::{deserialize_pathbuf, expand_path},
    http::{HttpConfig, NetworkConfig},
    https::HttpsConfig,
    logs::LoggingConfig,
    metrics::MetricsConfig,
    recovery::RecoveryConfig,
    solana::SolanaConfig,
    store::StoreConfig,
};

/// Error type for configuration loading and validation.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    ReadFile(#[from] std::io::Error),

    #[error("failed to parse YAML config: {0}")]
    ParseYaml(#[from] serde_yaml::Error),

    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Root node configuration loaded from YAML.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NodeConfig {
    /// Node identity and key material.
    #[serde(default)]
    pub node: IdentityConfig,

    /// Solana RPC and block-ingest settings.
    #[serde(default)]
    pub solana: SolanaConfig,

    /// Node advertisement settings for peers and orchestration.
    #[serde(default)]
    pub network: NetworkConfig,

    /// HTTP (plaintext) listener and ingress controls.
    #[serde(default)]
    pub http: HttpConfig,

    /// HTTPS (pinned + mTLS) listener and TLS key material.
    #[serde(default)]
    pub https: HttpsConfig,

    /// Local RocksDB storage settings.
    #[serde(default)]
    pub store: StoreConfig,

    /// Recovery worker and batch sizing controls.
    #[serde(default)]
    pub recovery: RecoveryConfig,

    /// Logging configuration.
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Metrics configuration.
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Gateway-only settings. Ignored by storage-node runtime.
    #[serde(default)]
    pub gateway: GatewayConfig,

    /// Genesis configuration.
    #[serde(default)]
    pub genesis_preset: GenesisPreset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum GenesisPreset {
    /// Production profile (1-week epochs). Safe default.
    #[default]
    Mainnet,
    /// Deployed fleet on Solana devnet (1-hour epochs).
    Devnet,
    /// Local-validator e2e profile (100s epochs).
    Localnet,
    /// In-memory simnet profile (20s epochs).
    Simnet,
}

impl GenesisPreset {
    pub fn config(self) -> GenesisConfig {
        match self {
            GenesisPreset::Mainnet => GenesisConfig::mainnet(),
            GenesisPreset::Devnet => GenesisConfig::devnet(),
            GenesisPreset::Localnet => GenesisConfig::localnet(),
            GenesisPreset::Simnet => GenesisConfig::simnet(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node: IdentityConfig::default(),
            solana: SolanaConfig::default(),
            network: NetworkConfig::default(),
            http: HttpConfig::default(),
            https: HttpsConfig::default(),
            store: StoreConfig::default(),
            recovery: RecoveryConfig::default(),
            logging: LoggingConfig::default(),
            metrics: MetricsConfig::default(),
            gateway: GatewayConfig::default(),
            genesis_preset: GenesisPreset::default(),
        }
    }
}

impl NodeConfig {
    /// Load configuration from a YAML file.
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = expand_path(path.as_ref());
        let contents = fs::read_to_string(&path)?;
        Self::from_yaml_str(&contents)
    }

    /// Load configuration from a YAML string.
    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let config: Self = serde_yaml::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate semantic constraints after parsing.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.node.name.trim().is_empty() {
            return Err(ConfigError::Invalid("node.name is required".into()));
        }

        if self.node.name.as_bytes().len() > NAME_LENGTH {
            return Err(ConfigError::Invalid(format!(
                "node.name exceeds {} bytes",
                NAME_LENGTH
            )));
        }

        if self.solana.rpc.is_empty() || self.solana.rpc.iter().any(|url| url.trim().is_empty()) {
            return Err(ConfigError::Invalid(
                "solana.rpc must list at least one endpoint".into(),
            ));
        }

        if !self.node.commission.is_valid() {
            return Err(ConfigError::Invalid(format!(
                "node.commission must be <= {}",
                BasisPoints::MAX
            )));
        }

        if self.gateway.cache.eviction_batch == 0 {
            return Err(ConfigError::Invalid(
                "gateway.cache.eviction_batch must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.object_read_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.object_read_per_sec must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.object_read_burst == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.object_read_burst must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.object_read_bytes_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.object_read_bytes_per_sec must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.object_read_byte_burst == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.object_read_byte_burst must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.over_budget_penalty_secs == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.over_budget_penalty_secs must be greater than zero".into(),
            ));
        }

        if self.gateway.metering.stale_entry_secs == 0 {
            return Err(ConfigError::Invalid(
                "gateway.metering.stale_entry_secs must be greater than zero".into(),
            ));
        }

        if self.http.admission.anonymous_write_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.anonymous_write_per_sec must be greater than zero".into(),
            ));
        }

        if self.http.admission.anonymous_write_burst == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.anonymous_write_burst must be greater than zero".into(),
            ));
        }

        if self.http.admission.anonymous_read_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.anonymous_read_per_sec must be greater than zero".into(),
            ));
        }

        if self.http.admission.anonymous_read_burst == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.anonymous_read_burst must be greater than zero".into(),
            ));
        }

        if self.http.admission.probe_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.probe_per_sec must be greater than zero".into(),
            ));
        }

        if self.http.admission.probe_burst == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.probe_burst must be greater than zero".into(),
            ));
        }

        if self.http.admission.trusted_metered_per_sec == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.trusted_metered_per_sec must be greater than zero".into(),
            ));
        }

        if self.http.admission.trusted_metered_burst == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.trusted_metered_burst must be greater than zero".into(),
            ));
        }

        if self.http.admission.over_budget_penalty_secs == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.over_budget_penalty_secs must be greater than zero".into(),
            ));
        }

        if self.http.admission.stale_entry_secs == 0 {
            return Err(ConfigError::Invalid(
                "http.admission.stale_entry_secs must be greater than zero".into(),
            ));
        }

        if let Some(host) = &self.network.host {
            if host.trim().is_empty() {
                return Err(ConfigError::Invalid(
                    "network.host must not be empty when provided".into(),
                ));
            }
        }

        Ok(())
    }

    /// Load the node authority keypair referenced by the config.
    pub fn load_node_keypair(&self) -> Result<Keypair, NodeError> {
        load_ed25519_keypair(&self.node.node_keypair).map_err(|error| {
            NodeError::Keypair(format!(
                "failed to load node keypair from {}: {error}",
                self.node.node_keypair.display()
            ))
        })
    }

    /// Load the BLS committee signing keypair referenced by the config.
    pub fn load_bls_keypair(&self) -> Result<BlsPrivateKey, NodeError> {
        load_bls_keypair(&self.node.bls_keypair).map_err(|error| {
            NodeError::Keypair(format!(
                "failed to load BLS keypair from {}: {error}",
                self.node.bls_keypair.display()
            ))
        })
    }

    /// Load the node's Ed25519 TLS keypair, generating and persisting a fresh
    /// one if `https.identity_keypair` does not yet exist.
    pub fn load_or_generate_tls_keypair(&self) -> Result<Keypair, NodeError> {
        ensure_ed25519_keypair(&self.https.identity_keypair).map_err(|error| {
            NodeError::Keypair(format!(
                "failed to load TLS keypair from {}: {error}",
                self.https.identity_keypair.display()
            ))
        })
    }
}

/// Operator-facing node identity settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct IdentityConfig {
    /// Node name used in logs and future registration flows.
    #[serde(default = "default_node_name")]
    pub name: String,

    /// Path to the Solana authority keypair.
    #[serde(
        default = "default_node_keypair_path",
        deserialize_with = "deserialize_pathbuf"
    )]
    pub node_keypair: PathBuf,

    /// Path to the BLS committee signing keypair.
    #[serde(
        default = "default_bls_keypair_path",
        deserialize_with = "deserialize_pathbuf"
    )]
    pub bls_keypair: PathBuf,

    /// Commission rate to use for self-registration.
    #[serde(default = "default_commission")]
    pub commission: BasisPoints,
}

impl Default for IdentityConfig {
    fn default() -> Self {
        Self {
            name: default_node_name(),
            node_keypair: default_node_keypair_path(),
            bls_keypair: default_bls_keypair_path(),
            commission: default_commission(),
        }
    }
}

/// Default node config file path (`~/.tape/node.yaml`).
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

fn default_node_name() -> String {
    "tape-node".to_string()
}

fn default_node_keypair_path() -> PathBuf {
    expand_path("~/.config/solana/id.json")
}

fn default_bls_keypair_path() -> PathBuf {
    expand_path("~/.tape/bls.key")
}

fn default_commission() -> BasisPoints {
    BasisPoints::zero()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tape_core::types::{BasisPoints, SlotNumber};

    use super::{NodeConfig, default_config_path};
    use crate::config::logs::LoggingFormat;

    const EXAMPLE_CONFIG: &str = r#"
node:
  name: "tape-node-1"
  node_keypair: "/etc/tape/node.json"
  bls_keypair: "/etc/tape/bls.key"
  commission: 0
solana:
  rpc:
    - "http://127.0.0.1:8899"
  start_slot: 12
network:
  host: "10.0.0.1"
  port: 3430
http:
  listen: "0.0.0.0:3420"
  timeout_secs: 7
  concurrency: 1024
  slice_max_bytes: 2097152
  peer_max_bytes: 524288
  admission:
    anonymous_write_per_sec: 2
    anonymous_write_burst: 3
    anonymous_read_per_sec: 11
    anonymous_read_burst: 22
    probe_per_sec: 33
    probe_burst: 44
    trusted_metered_per_sec: 200
    trusted_metered_burst: 400
    over_budget_penalty_secs: 9
    stale_entry_secs: 90
https:
  listen: "0.0.0.0:3430"
  identity_keypair: "/etc/tape/tls.json"
  auto_update: false
store:
  path: "/var/lib/tape/data"
  compaction_mb_per_sec: 80
  gc:
    enabled: true
    interval_secs: 30
    track_batch: 64
    slice_batch: 32
    reclaim_min_deleted_slices: 40
gateway:
  cache:
    max_bytes: 1073741824
    eviction_batch: 16
    reclaim_after_deleted_slices: 64
  metering:
    object_read_per_sec: 12
    object_read_burst: 24
    object_read_bytes_per_sec: 1048576
    object_read_byte_burst: 2097152
    over_budget_penalty_secs: 8
    stale_entry_secs: 120
recovery:
  max_workers: 42
  sync_batch: 99
  scan_batch: 77
  repair_batch: 8
  recover_batch: 6
logging:
  filter: "debug"
  format: "json"
metrics:
  enabled: false
"#;

    #[test]
    fn parses_example_config() {
        let config = NodeConfig::from_yaml_str(EXAMPLE_CONFIG).unwrap();

        assert_eq!(config.node.name, "tape-node-1");
        assert_eq!(config.node.node_keypair, PathBuf::from("/etc/tape/node.json"));
        assert_eq!(config.node.bls_keypair, PathBuf::from("/etc/tape/bls.key"));
        assert_eq!(config.node.commission, BasisPoints(0));
        assert_eq!(config.solana.rpc, vec!["http://127.0.0.1:8899"]);
        assert_eq!(config.solana.start_slot, Some(SlotNumber(12)));
        assert_eq!(config.network.host.as_deref(), Some("10.0.0.1"));
        assert_eq!(config.network.port, 3430);
        assert_eq!(config.http.listen.to_string(), "0.0.0.0:3420");
        assert_eq!(config.http.timeout_secs, 7);
        assert_eq!(config.http.concurrency, 1024);
        assert_eq!(config.http.slice_max_bytes, 2 * 1024 * 1024);
        assert_eq!(config.http.peer_max_bytes, 512 * 1024);
        assert_eq!(config.http.admission.anonymous_write_per_sec, 2);
        assert_eq!(config.http.admission.anonymous_write_burst, 3);
        assert_eq!(config.http.admission.anonymous_read_per_sec, 11);
        assert_eq!(config.http.admission.anonymous_read_burst, 22);
        assert_eq!(config.http.admission.probe_per_sec, 33);
        assert_eq!(config.http.admission.probe_burst, 44);
        assert_eq!(config.http.admission.trusted_metered_per_sec, 200);
        assert_eq!(config.http.admission.trusted_metered_burst, 400);
        assert_eq!(config.http.admission.over_budget_penalty_secs, 9);
        assert_eq!(config.http.admission.stale_entry_secs, 90);
        assert_eq!(config.https.listen.to_string(), "0.0.0.0:3430");
        assert_eq!(config.https.identity_keypair, PathBuf::from("/etc/tape/tls.json"));
        assert!(!config.https.auto_update);
        assert_eq!(config.store.path, PathBuf::from("/var/lib/tape/data"));
        assert_eq!(config.store.compaction_mb_per_sec, 80);
        assert!(config.store.gc.enabled);
        assert_eq!(config.store.gc.interval_secs, 30);
        assert_eq!(config.store.gc.track_batch, 64);
        assert_eq!(config.store.gc.slice_batch, 32);
        assert_eq!(config.store.gc.reclaim_min_deleted_slices, 40);
        assert_eq!(config.gateway.cache.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.gateway.cache.eviction_batch, 16);
        assert_eq!(config.gateway.cache.reclaim_after_deleted_slices, 64);
        assert_eq!(config.gateway.metering.object_read_per_sec, 12);
        assert_eq!(config.gateway.metering.object_read_burst, 24);
        assert_eq!(config.gateway.metering.object_read_bytes_per_sec, 1024 * 1024);
        assert_eq!(config.gateway.metering.object_read_byte_burst, 2 * 1024 * 1024);
        assert_eq!(config.gateway.metering.over_budget_penalty_secs, 8);
        assert_eq!(config.gateway.metering.stale_entry_secs, 120);
        assert_eq!(config.recovery.max_workers, 42);
        assert_eq!(config.recovery.sync_batch, 99);
        assert_eq!(config.recovery.scan_batch, 77);
        assert_eq!(config.recovery.repair_batch, 8);
        assert_eq!(config.recovery.recover_batch, 6);
        assert_eq!(config.logging.filter, "debug");
        assert_eq!(config.logging.format, LoggingFormat::Json);
        assert!(!config.metrics.enabled);
    }

    #[test]
    fn expands_paths_in_example_shape() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
  node_keypair: "~/.config/solana/id.json"
  bls_keypair: "~/.tape/bls.key"
network:
  host: "test"
store:
  path: "~/tape/data"
https:
  identity_keypair: "~/.tape/tls.json"
"#,
        )
        .unwrap();

        assert!(!config.node.node_keypair.to_string_lossy().starts_with('~'));
        assert!(!config.node.bls_keypair.to_string_lossy().starts_with('~'));
        assert!(!config.store.path.to_string_lossy().starts_with('~'));
        assert!(!config.https.identity_keypair.to_string_lossy().starts_with('~'));
    }

    #[test]
    fn rejects_invalid_listen_address() {
        let result = NodeConfig::from_yaml_str(
            r#"
http:
  listen: "not-an-address"
"#,
        );

        assert!(result.is_err());
    }

    // an empty endpoint list leaves the ingestor with nowhere to read from
    #[test]
    fn rejects_empty_rpc_list() {
        let result = NodeConfig::from_yaml_str(
            r#"
solana:
  rpc: []
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn parses_optional_start_slot() {
        let config = NodeConfig::from_yaml_str(
            r#"
solana:
  start_slot: 42
"#,
        )
        .unwrap();

        assert_eq!(config.solana.start_slot, Some(SlotNumber(42)));
    }

    #[test]
    fn allows_omitted_deferred_sections() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
"#,
        )
        .unwrap();

        assert_eq!(config.network.host, None);
        assert_eq!(config.network.port, 3430);
        assert!(config.metrics.enabled);
    }

    #[test]
    fn rejects_invalid_commission() {
        let result = NodeConfig::from_yaml_str(
            r#"
node:
  commission: 12000
"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn parses_valid_commission() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
  commission: 2500
"#,
        )
        .unwrap();

        assert_eq!(config.node.commission, BasisPoints(2500));
    }

    #[test]
    fn parses_recovery_override() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
network:
  host: "test"
recovery:
  max_workers: 42
  repair_batch: 12
"#,
        )
        .unwrap();

        assert_eq!(config.recovery.max_workers, 42);
        assert_eq!(config.recovery.repair_batch, 12);
        assert_eq!(config.recovery.sync_batch, 100);
    }

    #[test]
    fn default_config_path_points_to_node_yaml() {
        let path = default_config_path();
        assert!(path.to_string_lossy().contains("node.yaml"));
    }

    #[test]
    fn rejects_overlong_name() {
        let long = "a".repeat(33);
        let yaml = format!(
            r#"
node:
  name: "{long}"
"#
        );
        let result = NodeConfig::from_yaml_str(&yaml);
        assert!(result.is_err());
    }

    #[test]
    fn accepts_max_length_name() {
        let name = "a".repeat(32);
        let yaml = format!(
            r#"
node:
  name: "{name}"
"#
        );
        let config = NodeConfig::from_yaml_str(&yaml).unwrap();
        assert_eq!(config.node.name, name);
    }
}
