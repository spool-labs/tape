//! Config file + CLI-arg merging. YAML on disk, `${VAR}` substitution
//! borrowed from `tape-network`'s settings loader.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    /// Address the proxy binds to (e.g. `0.0.0.0:8899`).
    pub listen: SocketAddr,

    /// Upstream Solana RPC URL. Typically points at a paid provider.
    pub upstream: String,

    /// Per-method TTL overrides. Keys are canonical JSON-RPC method names
    /// (e.g. `getSlot`). Values are duration strings: `500ms`, `2s`, `5m`.
    #[serde(default, with = "humantime_map")]
    pub ttls: HashMap<String, Duration>,

    /// Minimum delay before retrying upstream after a 429, when the
    /// server doesn't send a `Retry-After` header. Default 10 s.
    #[serde(default = "default_min_429", with = "humantime_serde")]
    pub min_429_delay: Duration,

    /// Write a structured line to stderr for every transaction submit
    /// (`sendTransaction` etc). Used to identify which callers spam
    /// which submit paths. Default true.
    #[serde(default = "default_true")]
    pub log_submits: bool,

    /// Max number of cache entries kept in memory. 10k is plenty for a
    /// 20-node fleet; bump if you see churn.
    #[serde(default = "default_capacity")]
    pub max_entries: u64,

    /// API key required as `?api=<key>` on every request. Purely a
    /// port-scanner filter — not a security boundary.
    pub api_key: String,
}

fn default_min_429() -> Duration {
    Duration::from_secs(10)
}

fn default_true() -> bool {
    true
}

fn default_capacity() -> u64 {
    10_000
}

impl Config {
    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let substituted = substitute_env(&raw)?;
        serde_yaml::from_str(&substituted)
            .with_context(|| format!("parsing config {}", path.display()))
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

/// YAML (de)serializer for `HashMap<String, Duration>` using the
/// human-readable string format (e.g. `"30s"`).
mod humantime_map {
    use std::collections::HashMap;
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        map: &HashMap<String, Duration>,
        ser: S,
    ) -> Result<S::Ok, S::Error> {
        let stringified: HashMap<&str, String> = map
            .iter()
            .map(|(k, v)| (k.as_str(), humantime::format_duration(*v).to_string()))
            .collect();
        stringified.serialize(ser)
    }

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
        assert!(config.log_submits);
        assert_eq!(config.api_key, "deadbeef");
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
}
