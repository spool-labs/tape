//! Config file + CLI-arg merging. YAML on disk, `${VAR}` substitution
//! borrowed from `tape-network`'s settings loader.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use regex::Regex;
use serde::Deserialize;
use tape_forwarded::cidr::CidrBlock;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Address the proxy binds to (e.g. `0.0.0.0:8899`).
    pub listen: SocketAddr,

    /// Upstream Solana RPC URL. Typically points at a paid provider.
    pub upstream: String,

    /// Extra HTTP headers attached to every upstream request. Used for
    /// providers that authenticate via header (Triton One uses `x-token`)
    /// rather than via URL query string (Helius). Empty map = no extra
    /// headers, suitable for public endpoints.
    #[serde(default)]
    pub upstream_headers: HashMap<String, String>,

    /// Per-method TTL overrides. Keys are canonical JSON-RPC method names
    /// (e.g. `getSlot`). Values are duration strings: `500ms`, `2s`, `5m`.
    #[serde(default, with = "humantime_map")]
    pub ttls: HashMap<String, Duration>,

    /// How long every upstream request holds off after a 429, when the
    /// server doesn't send a `Retry-After` header. Default 10 s.
    #[serde(default = "default_min_429", with = "humantime_serde")]
    pub min_429_delay: Duration,

    /// Upstream requests allowed in flight at once. Caps the burst when
    /// many fills land together, such as nodes catching up after an
    /// upstream outage. Default 32.
    #[serde(default = "default_upstream_max_in_flight")]
    pub upstream_max_in_flight: usize,

    /// Write a structured line to stderr for every transaction submit
    /// (`sendTransaction` etc). Used to identify which callers spam
    /// which submit paths. Default true.
    #[serde(default = "default_true")]
    pub log_submits: bool,

    /// Max number of cache entries kept in memory. 10k is plenty for a
    /// 20-node fleet; bump if you see churn. Applies to the moka cache
    /// for non-block JSON-RPC methods (getSlot, getAccountInfo, etc).
    #[serde(default = "default_capacity")]
    pub max_entries: u64,

    /// API key required as `?api=<key>` on every JSON-RPC request.
    /// Purely a port-scanner filter — not a security boundary.
    /// `/v1/health` and `/v1/stats` are intentionally unauthed. Required
    /// unless `public` is set; a public instance takes no key.
    #[serde(default)]
    pub api_key: Option<String>,

    /// Serve the tape CLI to anyone: gate methods, meter callers, and skip
    /// block ingest. See `docs/public-rpc.md`.
    #[serde(default)]
    pub public: Option<PublicConfig>,

    /// Soft byte ceiling for the slot store (filtered confirmed blocks).
    /// Moka's weigher evicts oldest entries past this. 4 GiB by default.
    #[serde(default = "default_slot_store_max_bytes")]
    pub slot_store_max_bytes: u64,

    /// How many whole epochs of history to fill below the current epoch at
    /// bootstrap. 0 = current epoch only (from its start slot); 1 = also the
    /// previous epoch, etc. Widening this lets a reader that bootstraps from
    /// an older snapshot still find its blocks here instead of hitting the
    /// validator's prune window. Default 1.
    #[serde(default = "default_bootstrap_lookback_epochs")]
    pub bootstrap_lookback_epochs: u64,

    /// Filter target. Any tx referencing one of these programs (via
    /// account_keys, loaded_addresses, or `Program <id> invoke` log
    /// lines) is kept. All other txs are dropped from the stored block.
    /// Defaults to the four tape-network programs.
    #[serde(default = "default_filter_program_ids")]
    pub filter_program_ids: Vec<String>,
}

/// Limits for a public instance.
#[derive(Debug, Clone, Deserialize)]
pub struct PublicConfig {
    /// The proxy trusts `X-Forwarded-For` from these addresses. Loopback
    /// covers nginx on the same host.
    #[serde(default = "default_trusted_proxies")]
    pub trusted_proxies: Vec<CidrBlock>,

    /// Each caller's bucket refills this many requests per second. Default 20.
    #[serde(default = "default_caller_per_second")]
    pub caller_per_second: f64,

    /// Each caller may send this many requests at once. Default 200, about
    /// eight 1 MiB writes.
    #[serde(default = "default_caller_burst")]
    pub caller_burst: f64,

    /// All callers together may send this many requests per second upstream.
    /// The upstream key is shared with the fleet cache, so this keeps public
    /// traffic off the nodes' quota. Default 50.
    #[serde(default = "default_upstream_per_second")]
    pub upstream_per_second: f64,

    /// All callers together may send this many upstream requests at once.
    /// Default 100.
    #[serde(default = "default_upstream_burst")]
    pub upstream_burst: f64,
}

fn default_trusted_proxies() -> Vec<CidrBlock> {
    vec![
        CidrBlock::from(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        CidrBlock::from(IpAddr::V6(Ipv6Addr::LOCALHOST)),
    ]
}

fn default_caller_per_second() -> f64 {
    20.0
}

fn default_caller_burst() -> f64 {
    200.0
}

fn default_upstream_per_second() -> f64 {
    50.0
}

fn default_upstream_burst() -> f64 {
    100.0
}

fn default_min_429() -> Duration {
    Duration::from_secs(10)
}

fn default_upstream_max_in_flight() -> usize {
    32
}

fn default_true() -> bool {
    true
}

fn default_capacity() -> u64 {
    10_000
}

fn default_slot_store_max_bytes() -> u64 {
    4 * 1024 * 1024 * 1024
}

fn default_bootstrap_lookback_epochs() -> u64 {
    1
}

fn default_filter_program_ids() -> Vec<String> {
    vec![
        tape_api::program::tapedrive::ID.to_string(),
        tape_api::program::exchange::ID.to_string(),
        tape_api::program::staking::ID.to_string(),
        tape_api::program::token::ID.to_string(),
    ]
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let substituted = substitute_env(&raw)?;
        let config: Config = serde_yaml::from_str(&substituted)
            .with_context(|| format!("parsing config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.public.is_none() && self.api_key.is_none() {
            bail!("`api_key` is required unless `public` is set");
        }
        Ok(())
    }
}

fn substitute_env(input: &str) -> Result<String> {
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").unwrap();
    let mut out = String::with_capacity(input.len());
    let mut last_end = 0usize;
    for cap in re.captures_iter(input) {
        let whole = cap.get(0).unwrap();
        let name = cap.get(1).unwrap().as_str();
        out.push_str(&input[last_end..whole.start()]);
        let value = std::env::var(name)
            .map_err(|_| anyhow!("environment variable `{name}` is not set"))?;
        out.push_str(&value);
        last_end = whole.end();
    }
    out.push_str(&input[last_end..]);
    Ok(out)
}

/// YAML deserializer for `HashMap<String, Duration>` using the
/// human-readable string format (e.g. `"30s"`).
mod humantime_map {
    use std::collections::HashMap;
    use std::time::Duration;

    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D: Deserializer<'de>>(
        de: D,
    ) -> Result<HashMap<String, Duration>, D::Error> {
        let raw: HashMap<String, String> = HashMap::deserialize(de)?;
        raw.into_iter()
            .map(|(k, v)| {
                humantime::parse_duration(&v)
                    .map(|d| (k, d))
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let yaml = r#"
listen: "0.0.0.0:8899"
upstream: "https://api.devnet.solana.com"
api_key: "deadbeef"
"#;
        let config: Config = serde_yaml::from_str(&substitute_env(yaml).unwrap()).unwrap();
        assert_eq!(config.listen.port(), 8899);
        assert_eq!(config.min_429_delay, Duration::from_secs(10));
        assert_eq!(config.upstream_max_in_flight, 32);
        assert!(config.log_submits);
        assert_eq!(config.api_key.as_deref(), Some("deadbeef"));
        assert!(config.public.is_none());
    }

    #[test]
    fn parses_ttls_and_override() {
        let yaml = r#"
listen: "0.0.0.0:8899"
upstream: "https://api.devnet.solana.com"
api_key: "deadbeef"
min_429_delay: "5s"
ttls:
  getSlot: "1s"
  getBlock: "10m"
"#;
        let config: Config = serde_yaml::from_str(&substitute_env(yaml).unwrap()).unwrap();
        assert_eq!(config.min_429_delay, Duration::from_secs(5));
        assert_eq!(
            config.ttls.get("getSlot").copied(),
            Some(Duration::from_secs(1))
        );
        assert_eq!(
            config.ttls.get("getBlock").copied(),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn public_defaults() {
        let yaml = r#"
listen: "127.0.0.1:8899"
upstream: "https://api.devnet.solana.com"
public: {}
"#;
        let config: Config = serde_yaml::from_str(yaml).expect("parse");
        assert!(config.validate().is_ok());
        let public = config.public.expect("public section");
        assert_eq!(public.caller_per_second, 20.0);
        assert_eq!(public.caller_burst, 200.0);
        assert_eq!(public.upstream_per_second, 50.0);
        assert_eq!(public.trusted_proxies.len(), 2);
    }

    #[test]
    fn public_overrides() {
        let yaml = r#"
listen: "127.0.0.1:8899"
upstream: "https://api.devnet.solana.com"
public:
  trusted_proxies: ["127.0.0.1", "173.245.48.0/20"]
  caller_per_second: 5
  upstream_burst: 10
"#;
        let config: Config = serde_yaml::from_str(yaml).expect("parse");
        let public = config.public.expect("public section");
        assert_eq!(public.trusted_proxies[1].to_string(), "173.245.48.0/20");
        assert_eq!(public.caller_per_second, 5.0);
        assert_eq!(public.upstream_burst, 10.0);
    }

    #[test]
    fn fleet_needs_key() {
        let yaml = r#"
listen: "0.0.0.0:8899"
upstream: "https://api.devnet.solana.com"
"#;
        let config: Config = serde_yaml::from_str(yaml).expect("parse");
        assert!(config.validate().is_err());
    }
}
