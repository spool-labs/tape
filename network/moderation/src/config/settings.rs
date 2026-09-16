//! Every setting of the service, loaded from one YAML file

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;
use tape_core::types::coin::{SOL, TAPE};
use tape_core::types::{EpochNumber, StorageUnits, TrackNumber};
use tape_forwarded::cidr::CidrBlock;
use tape_sdk::stream::manifest::MAX_TRACKS_PER_TAPE;

use crate::config::error::ConfigError;
use crate::types::category::Category;
use crate::types::ids::{AttemptCount, RemovalCount, ReportCount};

const KIB: u64 = 1_024;
const MIB: u64 = 1_024 * KIB;
const LAMPORTS_PER_SOL: u64 = 1_000_000_000;

/// The whole config file
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// SQLite records database
    pub database: PathBuf,
    pub api: ApiConfig,
    pub gateway: GatewayConfig,
    pub moderation: ModerationConfig,
    #[serde(default)]
    pub worker: WorkerConfig,
    #[serde(default)]
    pub tape_rule: TapeRuleConfig,
    #[serde(default)]
    pub publisher: PublisherConfig,
    pub chain: ChainConfig,
    pub tapes: TapesConfig,
    #[serde(default)]
    pub funder: FunderConfig,
}

/// The report API
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    pub listen: SocketAddr,
    /// Origins whose browsers may submit reports
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    /// Proxies whose `x-forwarded-for` names the real caller
    #[serde(default)]
    pub trusted_proxies: Vec<CidrBlock>,
    /// Salt for hashing reporter addresses, so records never hold them
    pub reporter_salt_file: PathBuf,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default)]
    pub caller_limit: CallerLimitConfig,
}

/// Reports one caller may submit per window
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CallerLimitConfig {
    pub reports: ReportCount,
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub window: Duration,
    /// Callers tracked at once; the oldest window is dropped beyond this
    pub max_callers: usize,
}

/// The gateway content is read through
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    pub url: String,
    #[serde(
        default = "default_gateway_timeout",
        deserialize_with = "crate::config::duration::deserialize"
    )]
    pub request_timeout: Duration,
}

/// The moderation API
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModerationConfig {
    #[serde(default = "default_moderation_url")]
    pub base_url: String,
    #[serde(default = "default_moderation_model")]
    pub model: String,
    pub api_key_file: PathBuf,
    #[serde(
        default = "default_moderation_timeout",
        deserialize_with = "crate::config::duration::deserialize"
    )]
    pub request_timeout: Duration,
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: usize,
    /// Categories that remove a track when flagged
    #[serde(default = "default_removal_categories")]
    pub removal_categories: Vec<Category>,
}

/// The check worker
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkerConfig {
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub poll_interval: Duration,
    pub max_attempts: AttemptCount,
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub initial_retry_delay: Duration,
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub max_retry_delay: Duration,
    /// Tracks a tape report checks
    pub tape_track_limit: u32,
}

/// When removals on a tape unlist it
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TapeRuleConfig {
    pub triggering_categories: Vec<Category>,
    pub removal_count: RemovalCount,
}

/// Batch publishing
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PublisherConfig {
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub batch_window: Duration,
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub poll_interval: Duration,
}

/// The chain and the payer for writes and upkeep
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainConfig {
    pub rpc_url: String,
    pub payer_key_file: PathBuf,
}

/// Rack and blacklist tapes this service reserves
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TapesConfig {
    /// Where tape keys are persisted, one JSON keypair per tape
    pub key_dir: PathBuf,
    #[serde(default = "default_initial_capacity")]
    pub initial_capacity: StorageUnits,
    #[serde(default = "default_initial_epochs")]
    pub initial_epochs: EpochNumber,
}

/// Tape upkeep and payer alerts
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FunderConfig {
    #[serde(deserialize_with = "crate::config::duration::deserialize")]
    pub interval: Duration,
    pub expiry_horizon: EpochNumber,
    pub expiry_extension: EpochNumber,
    /// Epochs to expiry at or below which a blacklist tape's decisions move to the newest tape
    pub move_horizon: EpochNumber,
    pub capacity_reserve: StorageUnits,
    pub capacity_extension: StorageUnits,
    /// Free track slots at or below which the newest blacklist tape gets a successor and the rack warns
    pub track_headroom: TrackNumber,
    pub sol_warn_below: SOL,
    pub tape_warn_below: TAPE,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = serde_yaml::from_str(&text).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let checks = [
            (self.gateway.url.trim().is_empty(), "gateway.url is empty"),
            (self.chain.rpc_url.trim().is_empty(), "chain.rpc_url is empty"),
            (
                self.moderation.removal_categories.is_empty(),
                "moderation.removal_categories is empty, so nothing could be removed",
            ),
            (self.worker.max_attempts.0 == 0, "worker.max_attempts must be at least 1"),
            (self.worker.tape_track_limit == 0, "worker.tape_track_limit must be at least 1"),
            (self.tape_rule.removal_count.0 == 0, "tape_rule.removal_count must be at least 1"),
            (
                self.funder.track_headroom.0 >= MAX_TRACKS_PER_TAPE,
                "funder.track_headroom must be below a tape's track limit, or every new blacklist tape needs a successor",
            ),
            (
                self.funder.move_horizon.0 == 0,
                "funder.move_horizon must be at least 1, or decisions move only after their tape expires",
            ),
            (
                self.funder.move_horizon >= self.funder.expiry_horizon,
                "funder.move_horizon must be below funder.expiry_horizon, or decisions move before extending is tried",
            ),
            (self.tapes.initial_epochs.0 == 0, "tapes.initial_epochs must be at least 1"),
            (self.tapes.initial_capacity.0 == 0, "tapes.initial_capacity must be above 0"),
        ];
        for (is_invalid, message) in checks {
            if is_invalid {
                return Err(ConfigError::Invalid(message.to_string()));
            }
        }
        Ok(())
    }
}

impl Default for CallerLimitConfig {
    fn default() -> Self {
        Self {
            reports: ReportCount(10),
            window: Duration::from_secs(3_600),
            max_callers: 100_000,
        }
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            max_attempts: AttemptCount(6),
            initial_retry_delay: Duration::from_secs(30),
            max_retry_delay: Duration::from_secs(3_600),
            tape_track_limit: 50,
        }
    }
}

impl Default for TapeRuleConfig {
    fn default() -> Self {
        Self {
            triggering_categories: vec![Category::SexualMinors],
            removal_count: RemovalCount(3),
        }
    }
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            batch_window: Duration::from_secs(10),
            poll_interval: Duration::from_secs(1),
        }
    }
}

impl Default for FunderConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(600),
            expiry_horizon: EpochNumber(8),
            expiry_extension: EpochNumber(26),
            move_horizon: EpochNumber(2),
            capacity_reserve: StorageUnits(64 * KIB),
            capacity_extension: StorageUnits(MIB),
            track_headroom: TrackNumber(1_024),
            sol_warn_below: SOL(LAMPORTS_PER_SOL / 2),
            tape_warn_below: TAPE(10 * TAPE::SCALE),
        }
    }
}

fn default_max_body_bytes() -> usize {
    (4 * KIB) as usize
}

fn default_gateway_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_moderation_url() -> String {
    "https://api.openai.com/v1".to_string()
}

fn default_moderation_model() -> String {
    "omni-moderation-latest".to_string()
}

fn default_moderation_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_max_response_bytes() -> usize {
    MIB as usize
}

fn default_removal_categories() -> Vec<Category> {
    Category::ALL.to_vec()
}

fn default_initial_capacity() -> StorageUnits {
    StorageUnits(MIB)
}

fn default_initial_epochs() -> EpochNumber {
    EpochNumber(52)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = "
database: records.sqlite
api:
  listen: 127.0.0.1:8080
  reporter_salt_file: salt
gateway:
  url: http://127.0.0.1:3000
moderation:
  api_key_file: key
chain:
  rpc_url: http://127.0.0.1:8899
  payer_key_file: payer.json
tapes:
  key_dir: tapes
";

    fn write_config(text: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("config.yaml");
        fs::write(&path, text).expect("write config");
        (dir, path)
    }

    // a minimal file takes every default, including all removal categories
    #[test]
    fn defaults() {
        let (_dir, path) = write_config(MINIMAL);

        let config = Config::load(&path).expect("load");

        assert_eq!(config.moderation.removal_categories, Category::ALL.to_vec());
        assert_eq!(config.tape_rule.triggering_categories, vec![Category::SexualMinors]);
        assert_eq!(config.tape_rule.removal_count, RemovalCount(3));
        assert_eq!(config.publisher.batch_window, Duration::from_secs(10));
    }

    // the example config in the repository loads
    #[test]
    fn example() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.yaml");

        let config = Config::load(&path).expect("load example");

        assert_eq!(config.worker.tape_track_limit, 50);
        assert_eq!(config.funder.track_headroom, TrackNumber(1_024));
        assert_eq!(config.funder.move_horizon, EpochNumber(2));
    }

    // unknown categories, misspelled fields and the old rollover setting are refused
    #[test]
    fn refused() {
        let category = format!("{MINIMAL}tape_rule:\n  triggering_categories: [sexual/adults]\n");
        let field = format!("{MINIMAL}publisher:\n  batch_windw: 5s\n");
        let rollover = format!("{MINIMAL}funder:\n  rollover_track_headroom: 1024\n");

        for text in [category, field, rollover] {
            let (_dir, path) = write_config(&text);
            assert!(matches!(Config::load(&path), Err(ConfigError::Parse { .. })));
        }
    }
}
