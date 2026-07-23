use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Deserializer};
use tape_protocol::api::SLICE_BODY_LIMIT;

use super::cidr::CidrBlock;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NetworkConfig {
    #[serde(default)]
    pub host: Option<String>,

    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            host: None,
            port: default_port(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct HttpConfig {
    /// Address the plaintext HTTP listener binds to.
    #[serde(
        default = "default_http_listen",
        deserialize_with = "deserialize_socket_addr"
    )]
    pub listen: SocketAddr,

    /// Global request timeout in seconds.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Global concurrent request limit.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Maximum request body size for slice upload requests.
    #[serde(default = "default_slice_max_bytes")]
    pub slice_max_bytes: usize,

    /// Maximum request body size for peer protocol POST requests.
    #[serde(default = "default_peer_max_bytes")]
    pub peer_max_bytes: usize,

    /// Cheap node-side admission controls for metered public routes.
    #[serde(default)]
    pub admission: AdmissionConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            listen: default_http_listen(),
            timeout_secs: default_timeout_secs(),
            concurrency: default_concurrency(),
            slice_max_bytes: default_slice_max_bytes(),
            peer_max_bytes: default_peer_max_bytes(),
            admission: AdmissionConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct AdmissionConfig {
    /// Anonymous direct-write request refill rate, per source IP.
    #[serde(default = "default_anonymous_write_per_sec")]
    pub anonymous_write_per_sec: u32,

    /// Anonymous direct-write burst, per source IP.
    #[serde(default = "default_anonymous_write_burst")]
    pub anonymous_write_burst: u32,

    /// Anonymous public-read request refill rate, per source IP.
    #[serde(default = "default_anonymous_read_per_sec")]
    pub anonymous_read_per_sec: u32,

    /// Anonymous public-read burst, per source IP.
    #[serde(default = "default_anonymous_read_burst")]
    pub anonymous_read_burst: u32,

    /// Health/stats probe refill rate, per caller.
    #[serde(default = "default_probe_per_sec")]
    pub probe_per_sec: u32,

    /// Health/stats probe burst, per caller.
    #[serde(default = "default_probe_burst")]
    pub probe_burst: u32,

    /// Authenticated metered-route refill rate, per node identity.
    #[serde(default = "default_trusted_metered_per_sec")]
    pub trusted_metered_per_sec: u32,

    /// Authenticated metered-route burst, per node identity.
    #[serde(default = "default_trusted_metered_burst")]
    pub trusted_metered_burst: u32,

    /// Short block window after a caller exceeds its bucket.
    #[serde(default = "default_over_budget_penalty_secs")]
    pub over_budget_penalty_secs: u64,

    /// Remove idle limiter entries after this many seconds.
    #[serde(default = "default_stale_entry_secs")]
    pub stale_entry_secs: u64,

    /// Stake-tiered rates for registered staked callers that are not
    /// committee members, such as gateways. Sorted ascending by stake; a
    /// caller gets the highest tier its stake reaches, and below the first
    /// tier it is admitted as anonymous.
    #[serde(default = "default_stake_tiers")]
    pub stake_tiers: Vec<StakeTier>,

    /// Proxy addresses or CIDR ranges whose X-Forwarded-For header is trusted
    /// when resolving the caller address. Empty means the socket peer is
    /// always the caller.
    #[serde(default)]
    pub trusted_proxies: Vec<CidrBlock>,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            anonymous_write_per_sec: default_anonymous_write_per_sec(),
            anonymous_write_burst: default_anonymous_write_burst(),
            anonymous_read_per_sec: default_anonymous_read_per_sec(),
            anonymous_read_burst: default_anonymous_read_burst(),
            probe_per_sec: default_probe_per_sec(),
            probe_burst: default_probe_burst(),
            trusted_metered_per_sec: default_trusted_metered_per_sec(),
            trusted_metered_burst: default_trusted_metered_burst(),
            over_budget_penalty_secs: default_over_budget_penalty_secs(),
            stale_entry_secs: default_stale_entry_secs(),
            stake_tiers: default_stake_tiers(),
            trusted_proxies: Vec::new(),
        }
    }
}

/// One stake-tier admission rate: callers staking at least this many whole
/// TAPE refill at this rate with this burst capacity.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StakeTier {
    pub min_stake_tape: u64,
    pub per_sec: u32,
    pub burst: u32,
}

fn default_port() -> u16 {
    default_https_listen().port()
}

fn default_http_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3420)
}

pub fn default_https_listen() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 3430)
}

fn default_timeout_secs() -> u64 {
    60
}

fn default_concurrency() -> usize {
    2048
}

fn default_slice_max_bytes() -> usize {
    SLICE_BODY_LIMIT
}

fn default_peer_max_bytes() -> usize {
    1024 * 1024
}

// A single writer streaming one blob sends a slice put per chunk plus the
// certify sign polls to every committee node from one address, so the
// write bucket must absorb a full stream, not just a lone track.
fn default_anonymous_write_per_sec() -> u32 {
    16
}

fn default_stake_tiers() -> Vec<StakeTier> {
    vec![
        StakeTier { min_stake_tape: 100, per_sec: 32, burst: 128 },
        StakeTier { min_stake_tape: 1_000, per_sec: 128, burst: 512 },
        StakeTier { min_stake_tape: 10_000, per_sec: 512, burst: 2_048 },
    ]
}

fn default_anonymous_write_burst() -> u32 {
    64
}

fn default_anonymous_read_per_sec() -> u32 {
    10
}

fn default_anonymous_read_burst() -> u32 {
    50
}

fn default_probe_per_sec() -> u32 {
    60
}

fn default_probe_burst() -> u32 {
    120
}

fn default_trusted_metered_per_sec() -> u32 {
    128
}

fn default_trusted_metered_burst() -> u32 {
    256
}

fn default_over_budget_penalty_secs() -> u64 {
    5
}

fn default_stale_entry_secs() -> u64 {
    300
}

fn deserialize_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}
