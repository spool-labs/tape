use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use serde::Deserialize;

use super::helpers::{deserialize_option_pathbuf, deserialize_socket_addr};

/// Gateway-only runtime settings.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GatewayConfig {
    /// Slice cache settings for the public read gateway.
    #[serde(default)]
    pub cache: GatewayCacheConfig,

    /// Public gateway request/byte metering.
    #[serde(default)]
    pub metering: GatewayMeteringConfig,

    /// S3-compatible gateway listener. Disabled by default.
    #[serde(default)]
    pub s3: S3Config,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            cache: GatewayCacheConfig::default(),
            metering: GatewayMeteringConfig::default(),
            s3: S3Config::default(),
        }
    }
}

/// S3-compatible gateway listener controls.
#[derive(Clone, Deserialize, Eq, PartialEq)]
pub struct S3Config {
    /// Bind and serve the S3-compatible listener. Disabled by default.
    #[serde(default)]
    pub enabled: bool,

    /// Address the S3-compatible listener binds to.
    #[serde(
        default = "default_s3_listen",
        deserialize_with = "deserialize_socket_addr"
    )]
    pub listen: SocketAddr,

    /// Optional Ed25519 delegate keypair used to sign Solana write transactions.
    #[serde(default, deserialize_with = "deserialize_option_pathbuf")]
    pub delegate_key: Option<PathBuf>,

    /// SigV4 access key id that signed requests must present.
    #[serde(default)]
    pub access_key_id: Option<String>,

    /// SigV4 secret access key paired with `access_key_id`, used to derive the
    /// signing key during signature verification.
    #[serde(default)]
    pub secret_access_key: Option<String>,

    /// Write-authorization controls: the default decision, the admin
    /// control-plane surface, and the default budgets.
    #[serde(default)]
    pub write: S3WriteConfig,

    /// Overall maximum object size (bytes) for a streamed PutObject.
    #[serde(default = "default_s3_max_object_bytes")]
    pub max_object_bytes: usize,

    /// Maximum bytes held in memory for a buffered write.
    #[serde(default = "default_s3_max_buffered_bytes")]
    pub max_buffered_bytes: usize,

    /// Public base URL clients reach this gateway at (e.g. `https://s3.example.com`),
    /// used for the `Location` of a completed multipart upload. When unset, a
    /// path-style resource (`/{bucket}/{key}`) is returned.
    #[serde(default)]
    pub public_endpoint: Option<String>,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: default_s3_listen(),
            delegate_key: None,
            access_key_id: None,
            secret_access_key: None,
            write: S3WriteConfig::default(),
            max_object_bytes: default_s3_max_object_bytes(),
            max_buffered_bytes: default_s3_max_buffered_bytes(),
            public_endpoint: None,
        }
    }
}

// Custom Debug so the SigV4 secret never lands in a log line; its presence is
// still visible.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("enabled", &self.enabled)
            .field("listen", &self.listen)
            .field("delegate_key", &self.delegate_key)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &self.secret_access_key.as_ref().map(|_| "<redacted>"))
            .field("write", &self.write)
            .field("max_object_bytes", &self.max_object_bytes)
            .field("max_buffered_bytes", &self.max_buffered_bytes)
            .field("public_endpoint", &self.public_endpoint)
            .finish()
    }
}

fn default_s3_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3450)
}

/// Default overall object ceiling: AWS's single-object size of 5 GiB.
fn default_s3_max_object_bytes() -> usize {
    5 * 1024 * 1024 * 1024
}

/// Default in-memory buffered-write ceiling: 256 MiB.
fn default_s3_max_buffered_bytes() -> usize {
    256 * 1024 * 1024
}

/// S3 write-authorization defaults and control-plane wiring.
#[derive(Clone, Deserialize, Eq, PartialEq)]
pub struct S3WriteConfig {
    /// The default authorization decision for a write that no stored policy rule
    /// explicitly resolves.
    #[serde(default)]
    pub default: WriteDefault,

    /// The admin control-plane surface
    #[serde(default)]
    pub admin: S3AdminConfig,

    /// Default per-principal budgets.
    #[serde(default)]
    pub budgets: S3WriteBudgets,

    /// Server pepper for credential secret hashing
    /// (`HMAC-SHA256(secret, pepper)`).
    #[serde(default)]
    pub pepper: Option<String>,
}

impl Default for S3WriteConfig {
    fn default() -> Self {
        Self {
            default: WriteDefault::default(),
            admin: S3AdminConfig::default(),
            budgets: S3WriteBudgets::default(),
            pepper: None,
        }
    }
}

// Custom Debug so the credential-hashing pepper never lands in a log line.
impl std::fmt::Debug for S3WriteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3WriteConfig")
            .field("default", &self.default)
            .field("admin", &self.admin)
            .field("budgets", &self.budgets)
            .field("pepper", &self.pepper.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The default authorization decision when no stored policy rule resolves a write.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum WriteDefault {
    /// Reject any write not explicitly allowed by a stored policy rule.
    #[default]
    Deny,
    /// Admit any write from an active credential unless a stored rule denies it.
    Allow,
}

/// The admin control-plane listener for the write-authorization subsystem.
#[derive(Clone, Deserialize, Eq, PartialEq)]
pub struct S3AdminConfig {
    /// Address the admin control-plane listener binds to. Defaults to loopback.
    #[serde(
        default = "default_s3_admin_listen",
        deserialize_with = "deserialize_socket_addr"
    )]
    pub listen: SocketAddr,

    /// Operator bearer token that authenticates admin control-plane requests.
    #[serde(default)]
    pub operator_token: Option<String>,
}

impl Default for S3AdminConfig {
    fn default() -> Self {
        Self {
            listen: default_s3_admin_listen(),
            operator_token: None,
        }
    }
}

// Custom Debug so the operator bearer token never lands in a log line.
impl std::fmt::Debug for S3AdminConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3AdminConfig")
            .field("listen", &self.listen)
            .field("operator_token", &self.operator_token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

fn default_s3_admin_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 3451)
}

/// Default per-principal write budgets.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct S3WriteBudgets {
    /// Lamports of SOL fees a principal may spend per rolling day.
    #[serde(default = "default_sol_per_day")]
    pub sol_per_day: u64,

    /// Bytes a principal may write per rolling day.
    #[serde(default = "default_bytes_per_day")]
    pub bytes_per_day: u64,

    /// `PutObject` operations a principal may perform per rolling hour.
    #[serde(default = "default_puts_per_hour")]
    pub puts_per_hour: u32,

    /// Concurrent in-flight multipart uploads a principal may hold open.
    #[serde(default = "default_max_concurrent_multipart")]
    pub max_concurrent_multipart: u32,
}

impl Default for S3WriteBudgets {
    fn default() -> Self {
        Self {
            sol_per_day: default_sol_per_day(),
            bytes_per_day: default_bytes_per_day(),
            puts_per_hour: default_puts_per_hour(),
            max_concurrent_multipart: default_max_concurrent_multipart(),
        }
    }
}

fn default_sol_per_day() -> u64 {
    1_000_000_000
}

fn default_bytes_per_day() -> u64 {
    1024 * 1024 * 1024
}

fn default_puts_per_hour() -> u32 {
    1_000
}

fn default_max_concurrent_multipart() -> u32 {
    16
}

/// Gateway slice-cache controls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GatewayCacheConfig {
    /// Maximum raw slice payload bytes the gateway keeps on disk.
    ///
    /// A value of 0 disables persistent slice caching.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,

    /// Maximum entries deleted in one eviction pass.
    #[serde(default = "default_eviction_batch")]
    pub eviction_batch: usize,

    /// Trigger best-effort backend reclaim after this many evicted slices.
    ///
    /// A value of 0 disables explicit reclaim triggers.
    #[serde(default = "default_reclaim_after_deleted_slices")]
    pub reclaim_after_deleted_slices: usize,
}

impl Default for GatewayCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: default_max_bytes(),
            eviction_batch: default_eviction_batch(),
            reclaim_after_deleted_slices: default_reclaim_after_deleted_slices(),
        }
    }
}

fn default_max_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}

fn default_eviction_batch() -> usize {
    256
}

fn default_reclaim_after_deleted_slices() -> usize {
    1024
}

/// One named bundle of read-metering rates.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct MeteringGrade {
    /// Decoded-object request refill rate.
    pub read_per_sec: u32,

    /// Decoded-object request burst.
    pub read_burst: u32,

    /// Decoded-object byte refill rate.
    pub read_bytes_per_sec: u64,

    /// Decoded-object byte burst.
    pub read_byte_burst: u64,
}

/// Gateway public-route metering controls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct GatewayMeteringConfig {
    /// Named read-rate bundles callers are metered against.
    #[serde(default = "default_grades")]
    pub grades: BTreeMap<String, MeteringGrade>,

    /// Grade charged per resolved caller IP on every read.
    #[serde(default = "default_anonymous_grade")]
    pub anonymous_grade: String,

    /// Grade charged per verified access key when the credential has no
    /// grade assigned.
    #[serde(default = "default_default_grade")]
    pub default_grade: String,

    /// Short block window after a caller exceeds its bucket.
    #[serde(default = "default_over_budget_penalty_secs")]
    pub over_budget_penalty_secs: u64,

    /// Remove idle meter entries after this many seconds.
    #[serde(default = "default_stale_entry_secs")]
    pub stale_entry_secs: u64,

    /// Proxy addresses whose X-Forwarded-For header is trusted when resolving
    /// the caller IP. Empty means the socket peer is always the caller.
    #[serde(default)]
    pub trusted_proxies: Vec<IpAddr>,
}

impl Default for GatewayMeteringConfig {
    fn default() -> Self {
        Self {
            grades: default_grades(),
            anonymous_grade: default_anonymous_grade(),
            default_grade: default_default_grade(),
            over_budget_penalty_secs: default_over_budget_penalty_secs(),
            stale_entry_secs: default_stale_entry_secs(),
            trusted_proxies: Vec::new(),
        }
    }
}

fn default_grades() -> BTreeMap<String, MeteringGrade> {
    BTreeMap::from([
        (
            "anonymous".to_string(),
            MeteringGrade {
                read_per_sec: 10,
                read_burst: 50,
                read_bytes_per_sec: 64 * 1024 * 1024,
                read_byte_burst: 128 * 1024 * 1024,
            },
        ),
        (
            "standard".to_string(),
            MeteringGrade {
                read_per_sec: 20,
                read_burst: 100,
                read_bytes_per_sec: 128 * 1024 * 1024,
                read_byte_burst: 256 * 1024 * 1024,
            },
        ),
    ])
}

fn default_anonymous_grade() -> String {
    "anonymous".to_string()
}

fn default_default_grade() -> String {
    "standard".to_string()
}

fn default_over_budget_penalty_secs() -> u64 {
    5
}

fn default_stale_entry_secs() -> u64 {
    300
}
