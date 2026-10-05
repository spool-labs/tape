use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tape_core::bls::BlsPrivateKey;
use tape_core::challenge::schedule::{Schedule, rounds_in_seconds};
use tape_api::consts::NAME_LENGTH;
use tape_api::genesis::GenesisConfig;
use tape_core::thresholds::{NetworkParams, ThresholdError};
use tape_core::types::{BasisPoints, SlotNumber};
use tape_crypto::ed25519::Keypair;
use tape_sdk::keys::helpers::{ensure_ed25519_keypair, load_bls_keypair, load_ed25519_keypair};

use crate::core::error::NodeError;
use super::challenge::ChallengeConfig;
use super::eviction::EvictionConfig;
use super::{
    gateway::{GatewayConfig, is_valid_origin, is_valid_policy},
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
#[derive(Default)]
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

    /// Whether this node runs the storage challenge.
    #[serde(default)]
    pub challenge: ChallengeConfig,

    /// Whether this node takes part in evicting its peers.
    #[serde(default)]
    pub eviction: EvictionConfig,

    /// Local reel volume settings.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
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
    /// A private cluster on its own profile and trust model, with devnet's
    /// values for everything else.
    Custom,
}

impl GenesisPreset {
    /// Round spacing in slots: the preset's planned spacing at its slot time,
    /// to the whole slot below.
    pub fn challenge_interval_slots(self) -> u64 {
        self.challenge_spacing_ms() / self.slot_ms()
    }

    /// Wall time each preset plans between one round opening and the next.
    pub fn challenge_spacing_ms(self) -> u64 {
        match self {
            Self::Mainnet => 60_000,
            Self::Devnet | Self::Custom => 30_000,
            Self::Localnet => 10_000,
            Self::Simnet => 10_000,
        }
    }

    /// Wall time one slot takes on each network, and the one rate every preset
    /// timing converts at.
    ///
    /// Mainnet plans at 200 ms, where it is heading from 250 today. A slower
    /// live rate only stretches each round, so a span converted here lasts at
    /// least as long as stated. The simnet producer ticks once a wall second,
    /// while its chain clock counts `SLOT_MS` a slot. `SLOT_MS` is the fallback
    /// before ingest measures a live rate, and no cluster preset plans slower.
    pub fn slot_ms(self) -> u64 {
        match self {
            Self::Mainnet => 200,
            Self::Devnet | Self::Custom => 166,
            Self::Localnet => 400,
            Self::Simnet => 1_000,
        }
    }

    /// Seconds a spool that changed hands has to fetch what it was handed.
    ///
    /// Eight rounds at each preset's own cadence, the count every preset ran
    /// with when the grace was stated in rounds. Devnet's figure falls midway
    /// through its eighth round, so a slot time a few percent off keeps the count.
    pub fn challenge_handover_grace_seconds(self) -> u64 {
        match self {
            Self::Mainnet => 480,
            Self::Devnet | Self::Custom => 225,
            Self::Localnet => 80,
            Self::Simnet => 80,
        }
    }

    /// Seconds of blank rounds in a row before the node re-reads protocol state.
    ///
    /// Three rounds at each preset's own cadence, well inside the default
    /// eviction policy's thirty consecutive misses.
    pub fn challenge_realign_after_blank_seconds(self) -> u64 {
        match self {
            Self::Mainnet => 180,
            Self::Devnet | Self::Custom => 75,
            Self::Localnet => 30,
            Self::Simnet => 30,
        }
    }

    /// A tapenet preset's genesis values; `custom` has none of its own.
    pub fn tapenet(self) -> Option<GenesisConfig> {
        match self {
            GenesisPreset::Mainnet => Some(GenesisConfig::mainnet()),
            GenesisPreset::Devnet => Some(GenesisConfig::devnet()),
            GenesisPreset::Localnet => Some(GenesisConfig::localnet()),
            GenesisPreset::Simnet => Some(GenesisConfig::simnet()),
            GenesisPreset::Custom => None,
        }
    }

    /// The preset's genesis values, which set a node's preferences when it
    /// registers. `custom` takes the profile and trust model of the chain the
    /// node is joining, so its committee size preference is that chain's n.
    pub fn config(self, params: &NetworkParams) -> Result<GenesisConfig, ThresholdError> {
        match self.tapenet() {
            Some(config) => Ok(config),
            None => GenesisConfig::try_custom(params.profile, params.trust),
        }
    }
}


impl NodeConfig {
    pub fn challenge_interval_slots(&self) -> u64 {
        self.challenge
            .interval_slots
            .unwrap_or_else(|| self.genesis_preset.challenge_interval_slots())
    }

    /// Rounds a spool that changed hands has to fetch what it was handed.
    pub fn challenge_handover_grace_rounds(&self) -> u64 {
        let seconds = self
            .challenge
            .handover_grace_seconds
            .unwrap_or_else(|| self.genesis_preset.challenge_handover_grace_seconds());
        self.challenge_rounds_in(seconds)
    }

    /// Blank rounds in a row before the node re-reads protocol state.
    pub fn challenge_realign_after_blank_rounds(&self) -> u64 {
        let seconds = self
            .challenge
            .realign_after_blank_seconds
            .unwrap_or_else(|| self.genesis_preset.challenge_realign_after_blank_seconds());
        self.challenge_rounds_in(seconds)
    }

    /// The fewest whole rounds that last a wall-clock span at this network's
    /// cadence.
    fn challenge_rounds_in(&self, seconds: u64) -> u64 {
        rounds_in_seconds(
            seconds,
            self.challenge_interval_slots(),
            self.genesis_preset.slot_ms(),
        )
    }

    /// Returns whether challenge and eviction participation are both enabled.
    pub fn eviction_enabled(&self) -> bool {
        self.challenge.enabled && self.eviction.enabled
    }

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
        Schedule::for_epoch(SlotNumber(0), self.challenge_interval_slots())
        .map_err(|error| ConfigError::Invalid(format!("challenge.interval_slots: {error:?}")))?;

        self.eviction
            .policy
            .validate()
            .map_err(|error| ConfigError::Invalid(format!("eviction.policy.{error}")))?;

        if self.challenge.realign_after_blank_seconds == Some(0) {
            return Err(ConfigError::Invalid(
                "challenge.realign_after_blank_seconds must be greater than zero".into(),
            ));
        }
        if self.challenge.handover_grace_seconds == Some(0) {
            return Err(ConfigError::Invalid(
                "challenge.handover_grace_seconds must be greater than zero".into(),
            ));
        }
        if self.challenge.verify_workers == 0 {
            return Err(ConfigError::Invalid(
                "challenge.verify_workers must be greater than zero".into(),
            ));
        }
        if self.node.name.trim().is_empty() {
            return Err(ConfigError::Invalid("node.name is required".into()));
        }

        if self.node.name.len() > NAME_LENGTH {
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

        for (name, grade) in &self.gateway.metering.grades {
            if grade.read_per_sec == 0 {
                return Err(ConfigError::Invalid(format!(
                    "gateway.metering.grades.{name}.read_per_sec must be greater than zero"
                )));
            }
            if grade.read_burst == 0 {
                return Err(ConfigError::Invalid(format!(
                    "gateway.metering.grades.{name}.read_burst must be greater than zero"
                )));
            }
            if grade.read_bytes_per_sec == 0 {
                return Err(ConfigError::Invalid(format!(
                    "gateway.metering.grades.{name}.read_bytes_per_sec must be greater than zero"
                )));
            }
            if grade.read_byte_burst == 0 {
                return Err(ConfigError::Invalid(format!(
                    "gateway.metering.grades.{name}.read_byte_burst must be greater than zero"
                )));
            }
        }

        let metering = &self.gateway.metering;
        if !metering.grades.contains_key(&metering.anonymous_grade) {
            return Err(ConfigError::Invalid(format!(
                "gateway.metering.anonymous_grade `{}` is not a defined grade",
                metering.anonymous_grade
            )));
        }

        if !metering.grades.contains_key(&metering.default_grade) {
            return Err(ConfigError::Invalid(format!(
                "gateway.metering.default_grade `{}` is not a defined grade",
                metering.default_grade
            )));
        }

        if !metering.grades.contains_key(&metering.site_grade) {
            return Err(ConfigError::Invalid(format!(
                "gateway.metering.site_grade `{}` is not a defined grade",
                metering.site_grade
            )));
        }

        for origin in &self.gateway.site.cors_origins {
            if !is_valid_origin(origin) {
                return Err(ConfigError::Invalid(format!(
                    "gateway.site origin `{origin}` is not a valid origin"
                )));
            }
        }

        if let Some(policy) = self.gateway.site.content_security_policy.as_deref() {
            if !is_valid_policy(policy) {
                return Err(ConfigError::Invalid(
                    "gateway.site.content_security_policy must be non-empty printable ascii"
                        .into(),
                ));
            }
        }

        if self.gateway.site.subdomain_redirect && self.gateway.site.subdomain_suffix.is_none() {
            return Err(ConfigError::Invalid(
                "gateway.site.subdomain_redirect needs gateway.site.subdomain_suffix".into(),
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

        self.gateway.validate_blacklists().map_err(ConfigError::Invalid)?;

        if let Some(admission) = &self.http.admission {
            if admission.anonymous_write_per_sec == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.anonymous_write_per_sec must be greater than zero".into(),
                ));
            }

            if admission.anonymous_write_burst == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.anonymous_write_burst must be greater than zero".into(),
                ));
            }

            if admission.anonymous_read_per_sec == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.anonymous_read_per_sec must be greater than zero".into(),
                ));
            }

            if admission.anonymous_read_burst == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.anonymous_read_burst must be greater than zero".into(),
                ));
            }

            if admission.probe_per_sec == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.probe_per_sec must be greater than zero".into(),
                ));
            }

            if admission.probe_burst == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.probe_burst must be greater than zero".into(),
                ));
            }

            if admission.trusted_metered_per_sec == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.trusted_metered_per_sec must be greater than zero".into(),
                ));
            }

            if admission.trusted_metered_burst == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.trusted_metered_burst must be greater than zero".into(),
                ));
            }

            for tier in &admission.stake_tiers {
                if tier.per_sec == 0 || tier.burst == 0 {
                    return Err(ConfigError::Invalid(
                        "http.admission.stake_tiers rates must be greater than zero".into(),
                    ));
                }
            }

            let ascending = admission
                .stake_tiers
                .windows(2)
                .all(|pair| pair[0].min_stake < pair[1].min_stake);
            if !ascending {
                return Err(ConfigError::Invalid(
                    "http.admission.stake_tiers must ascend by stake".into(),
                ));
            }

            if admission.over_budget_penalty_secs == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.over_budget_penalty_secs must be greater than zero".into(),
                ));
            }

            if admission.stale_entry_secs == 0 {
                return Err(ConfigError::Invalid(
                    "http.admission.stale_entry_secs must be greater than zero".into(),
                ));
            }
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

    use tape_core::challenge::schedule::SLOT_MS;
    use tape_core::types::{BasisPoints, SlotNumber};

    use super::{GenesisPreset, NodeConfig, default_config_path};
    use tape_crypto::address::Address;
    use tape_forwarded::cidr::CidrBlock;
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
challenge:
  enabled: false
eviction:
  enabled: false
store:
  path: "/var/lib/tape/data"
  compaction_mb_per_sec: 80
  sync_bytes: 8388608
  io_backend: posix
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
    grades:
      anonymous:
        read_per_sec: 12
        read_burst: 24
        read_bytes_per_sec: 1048576
        read_byte_burst: 2097152
      standard:
        read_per_sec: 25
        read_burst: 50
        read_bytes_per_sec: 2097152
        read_byte_burst: 4194304
      site:
        read_per_sec: 40
        read_burst: 160
        read_bytes_per_sec: 2097152
        read_byte_burst: 4194304
    anonymous_grade: anonymous
    default_grade: standard
    site_grade: site
    over_budget_penalty_secs: 8
    stale_entry_secs: 120
    trusted_proxies: ["203.0.113.7", "173.245.48.0/20"]
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
        let admission = config.http.admission.as_ref().expect("admission block parses");
        assert_eq!(admission.anonymous_write_per_sec, 2);
        assert_eq!(admission.anonymous_write_burst, 3);
        assert_eq!(admission.anonymous_read_per_sec, 11);
        assert_eq!(admission.anonymous_read_burst, 22);
        assert_eq!(admission.probe_per_sec, 33);
        assert_eq!(admission.probe_burst, 44);
        assert_eq!(admission.trusted_metered_per_sec, 200);
        assert_eq!(admission.trusted_metered_burst, 400);
        assert_eq!(admission.over_budget_penalty_secs, 9);
        assert_eq!(admission.stale_entry_secs, 90);
        assert_eq!(config.https.listen.to_string(), "0.0.0.0:3430");
        assert_eq!(config.https.identity_keypair, PathBuf::from("/etc/tape/tls.json"));
        assert!(!config.https.auto_update);
        assert!(!config.challenge.enabled);
        assert!(!config.eviction.enabled);
        assert_eq!(config.store.path, PathBuf::from("/var/lib/tape/data"));
        assert_eq!(config.store.compaction_mb_per_sec, 80);
        assert_eq!(config.store.sync_bytes, 8 * 1024 * 1024);
        assert_eq!(config.store.io_backend, reel::IoBackend::Posix);
        assert!(config.store.gc.enabled);
        assert_eq!(config.store.gc.interval_secs, 30);
        assert_eq!(config.store.gc.track_batch, 64);
        assert_eq!(config.store.gc.slice_batch, 32);
        assert_eq!(config.store.gc.reclaim_min_deleted_slices, 40);
        assert_eq!(config.gateway.cache.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(config.gateway.cache.eviction_batch, 16);
        assert_eq!(config.gateway.cache.reclaim_after_deleted_slices, 64);
        let anonymous = &config.gateway.metering.grades["anonymous"];
        assert_eq!(anonymous.read_per_sec, 12);
        assert_eq!(anonymous.read_burst, 24);
        assert_eq!(anonymous.read_bytes_per_sec, 1024 * 1024);
        assert_eq!(anonymous.read_byte_burst, 2 * 1024 * 1024);
        let standard = &config.gateway.metering.grades["standard"];
        assert_eq!(standard.read_per_sec, 25);
        assert_eq!(standard.read_burst, 50);
        assert_eq!(standard.read_bytes_per_sec, 2 * 1024 * 1024);
        assert_eq!(standard.read_byte_burst, 4 * 1024 * 1024);
        assert_eq!(config.gateway.metering.anonymous_grade, "anonymous");
        assert_eq!(config.gateway.metering.default_grade, "standard");
        assert_eq!(config.gateway.metering.over_budget_penalty_secs, 8);
        assert_eq!(config.gateway.metering.stale_entry_secs, 120);
        assert_eq!(config.gateway.metering.site_grade, "site");
        assert_eq!(
            config.gateway.metering.trusted_proxies,
            vec![
                "203.0.113.7".parse::<CidrBlock>().unwrap(),
                "173.245.48.0/20".parse::<CidrBlock>().unwrap(),
            ]
        );
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

    // a site policy that could end the header early is refused at load
    #[test]
    fn rejects_policy_control_bytes() {
        let result = NodeConfig::from_yaml_str(
            "gateway:\n  site:\n    content_security_policy: \"default-src 'self'\\nx: y\"\n",
        );

        assert!(result.is_err());
    }

    // retiring the path form needs a subdomain to send sites to
    #[test]
    fn rejects_redirect_without_suffix() {
        let result = NodeConfig::from_yaml_str(
            "gateway:\n  site:\n    subdomain_redirect: true\n",
        );

        assert!(result.is_err());
    }

    // a well-formed site policy loads as written
    #[test]
    fn parses_site_policy() {
        let config = NodeConfig::from_yaml_str(
            "gateway:\n  site:\n    content_security_policy: \"default-src 'self'\"\n",
        )
        .unwrap();

        assert_eq!(
            config.gateway.site.content_security_policy.as_deref(),
            Some("default-src 'self'")
        );
    }

    // blacklist subscriptions load, and a tape subscribed twice is refused
    #[test]
    fn blacklist_subscriptions() {
        let tape = Address::new([7u8; 32]);
        let one = format!("gateway:\n  blacklists:\n    - name: a\n      tape: {tape}\n      actions: [remove]\n");
        let twice = format!("{one}    - name: b\n      tape: {tape}\n      actions: [unlist]\n");

        let config = NodeConfig::from_yaml_str(&one).expect("parse config");

        assert_eq!(config.gateway.blacklists.len(), 1);
        assert!(NodeConfig::from_yaml_str(&twice).is_err());
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

    // the challenge is what produces eviction evidence, so turning it off takes
    // eviction with it whatever the eviction knob says
    #[test]
    fn challenge_off_takes_eviction_with_it() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
network:
  host: "test"
challenge:
  enabled: false
eviction:
  enabled: true
"#,
        )
        .unwrap();

        assert!(config.eviction.enabled);
        assert!(!config.eviction_enabled());
    }

    #[test]
    fn challenge_is_on_unless_turned_off() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
network:
  host: "test"
"#,
        )
        .unwrap();

        assert!(config.challenge.enabled);
    }

    #[test]
    fn eviction_off_by_default() {
        let config = NodeConfig::from_yaml_str(
            r#"
node:
  name: "test"
network:
  host: "test"
"#,
        )
        .unwrap();

        assert!(!config.eviction.enabled);
        assert!(!config.eviction_enabled());
    }

    #[test]
    fn eviction_policy_is_explicit_and_independent_of_genesis_preset() {
        for preset in ["mainnet", "devnet", "localnet", "simnet"] {
            let yaml = format!(
                r#"
genesis_preset: {preset}
eviction:
  policy:
    max_consecutive_misses: 45
    min_opportunities: 90
    minimum_success_rate_bps: 6000
"#
            );
            let config = NodeConfig::from_yaml_str(&yaml).expect("valid eviction policy");

            assert_eq!(config.eviction.policy.max_consecutive_misses, 45);
            assert_eq!(config.eviction.policy.min_opportunities, 90);
            assert_eq!(
                config.eviction.policy.minimum_success_rate_bps,
                BasisPoints(6_000)
            );
        }
    }

    // each preset's grace and realign spans are wall clock, and each becomes
    // the fewest whole rounds that last it at that preset's own cadence
    #[test]
    fn challenge_spans_become_round_counts() {
        let spans = [
            ("mainnet", 300, 200, 8, 3),
            ("devnet", 180, 166, 8, 3),
            ("localnet", 25, 400, 8, 3),
            ("simnet", 10, 1_000, 8, 3),
            ("custom", 180, 166, 8, 3),
        ];

        for (preset, interval, slot_ms, grace, realign) in spans {
            let yaml = format!("genesis_preset: {preset}");
            let config = NodeConfig::from_yaml_str(&yaml).expect("valid preset");

            assert_eq!(config.challenge_interval_slots(), interval);
            assert_eq!(config.genesis_preset.slot_ms(), slot_ms);
            assert_eq!(config.challenge_handover_grace_rounds(), grace, "{preset}");
            assert_eq!(config.challenge_realign_after_blank_rounds(), realign, "{preset}");
        }

        // The tighter grid the fleet ran for a month covers the same spans in
        // many more rounds, which is the whole point of stating them in seconds.
        let yaml = r#"
genesis_preset: devnet
challenge:
  interval_slots: 7
"#;
        let config = NodeConfig::from_yaml_str(yaml).expect("valid interval");
        assert_eq!(config.challenge_handover_grace_rounds(), 194);
        assert_eq!(config.challenge_realign_after_blank_rounds(), 65);

        // A stated span wins over the preset at the same cadence, and a part
        // round counts in full.
        let yaml = r#"
genesis_preset: devnet
challenge:
  handover_grace_seconds: 240
  realign_after_blank_seconds: 90
"#;
        let config = NodeConfig::from_yaml_str(yaml).expect("valid spans");
        assert_eq!(config.challenge_handover_grace_rounds(), 9);
        assert_eq!(config.challenge_realign_after_blank_rounds(), 4);

        for zero in ["handover_grace_seconds", "realign_after_blank_seconds"] {
            let yaml = format!("challenge:\n  {zero}: 0");
            assert!(NodeConfig::from_yaml_str(&yaml).is_err());
        }
    }

    // a custom node registers with devnet's preferences, except the committee
    // size, which is the n of the chain it joins
    #[test]
    fn custom_preset() {
        use tape_api::genesis::GenesisConfig;
        use tape_core::encoding::{ErasureCode, RSParams, TAPENET_PROFILE};
        use tape_core::thresholds::{NetworkParams, TrustModel};

        let narrow = ErasureCode::ReedSolomon(RSParams::new(3, 1));
        let small = NetworkParams::try_new(narrow, TrustModel::Trusted).expect("3 of 4");
        let tapenet = NetworkParams::try_new(TAPENET_PROFILE, TrustModel::Permissionless)
            .expect("tapenet");

        let config = NodeConfig::from_yaml_str("genesis_preset: custom").expect("valid preset");
        assert_eq!(config.genesis_preset, GenesisPreset::Custom);

        let custom = config.genesis_preset.config(&small).expect("custom genesis");
        let devnet = GenesisConfig::devnet();
        assert_eq!(custom.committee_size, 4);
        assert_eq!(custom.profile, small.profile);
        assert_eq!(custom.trust_model, TrustModel::Trusted);
        assert_eq!(custom.epoch_duration, devnet.epoch_duration);
        assert_eq!(custom.storage_price, devnet.storage_price);

        // A tapenet preset keeps its own values whatever chain the node joins.
        assert_eq!(GenesisPreset::Devnet.config(&small), Ok(devnet));
        assert_eq!(GenesisPreset::Mainnet.config(&tapenet), Ok(GenesisConfig::mainnet()));
    }

    // each preset's cadence comes from its own slot time, and the fallback slot
    // time is no faster than any cluster preset plans for
    #[test]
    fn slot_times() {
        let presets = [
            GenesisPreset::Mainnet,
            GenesisPreset::Devnet,
            GenesisPreset::Localnet,
            GenesisPreset::Simnet,
            GenesisPreset::Custom,
        ];

        for preset in presets {
            let slot_ms = preset.slot_ms();
            let planned = preset.challenge_interval_slots() * slot_ms;

            assert!(planned <= preset.challenge_spacing_ms(), "{preset:?}");
            assert!(planned + slot_ms > preset.challenge_spacing_ms(), "{preset:?}");
        }
        for cluster in [GenesisPreset::Mainnet, GenesisPreset::Devnet, GenesisPreset::Localnet] {
            assert!(cluster.slot_ms() <= SLOT_MS, "{cluster:?}");
        }
    }

    #[test]
    fn challenge_interval_uses_named_presets_and_explicit_overrides() {
        let presets = [
            ("mainnet", 300),
            ("devnet", 180),
            ("localnet", 25),
            ("simnet", 10),
            ("custom", 180),
        ];

        for (preset, interval) in presets {
            let yaml = format!("genesis_preset: {preset}");
            let config = NodeConfig::from_yaml_str(&yaml).expect("valid preset");
            assert_eq!(config.challenge_interval_slots(), interval);

            let yaml = format!(
                r#"
genesis_preset: {preset}
challenge:
  interval_slots: 40
"#
            );
            let config = NodeConfig::from_yaml_str(&yaml).expect("valid interval override");
            assert_eq!(config.challenge_interval_slots(), 40);
        }

        for interval in [0, 1, 4] {
            let yaml = format!(
                r#"
challenge:
  interval_slots: {interval}
"#
            );
            assert!(NodeConfig::from_yaml_str(&yaml).is_err());
        }

        let misspelled = r#"
challenge:
  interval_slot: 10
"#;
        assert!(NodeConfig::from_yaml_str(misspelled).is_err());
    }

    #[test]
    fn rejects_invalid_or_misspelled_eviction_policy() {
        let invalid_rules = [
            (0, 60, 5_000),
            (30, 0, 5_000),
            (30, 60, 10_001),
        ];

        for (max_misses, min_opportunities, rate_bps) in invalid_rules {
            let yaml = format!(
                r#"
eviction:
  policy:
    max_consecutive_misses: {max_misses}
    min_opportunities: {min_opportunities}
    minimum_success_rate_bps: {rate_bps}
"#
            );
            assert!(NodeConfig::from_yaml_str(&yaml).is_err(), "accepted {yaml}");
        }

        let misspelled = r#"
eviction:
  policy:
    max_consecutive_miss: 30
    min_opportunities: 60
    minimum_success_rate_bps: 5000
"#;
        assert!(NodeConfig::from_yaml_str(misspelled).is_err());
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
