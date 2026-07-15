use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer};
use tape_crypto::address::Address;

pub fn expand_path(path: impl AsRef<Path>) -> PathBuf {
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

pub fn deserialize_pathbuf<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: Deserializer<'de>,
{
    let path = String::deserialize(deserializer)?;
    Ok(expand_path(path))
}

/// Deserialize an optional filesystem path, expanding a leading `~` to the home directory
pub fn deserialize_option_pathbuf<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
where
    D: Deserializer<'de>,
{
    let path = Option::<String>::deserialize(deserializer)?;
    Ok(path.map(expand_path))
}

/// Deserialize a socket address from its string representation
pub fn deserialize_socket_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse().map_err(serde::de::Error::custom)
}

/// Deserialize a hostname-to-tape map: keys lowercase, since hostnames are
/// case-insensitive, and values parse to addresses so a typo fails at load
pub fn deserialize_domain_map<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, Address>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = BTreeMap::<String, String>::deserialize(deserializer)?;
    let mut domains = BTreeMap::new();
    for (host, tape) in raw {
        let tape: Address = tape.parse().map_err(|_| {
            serde::de::Error::custom(format!("domain `{host}` needs a valid tape address"))
        })?;
        domains.insert(host.to_ascii_lowercase(), tape);
    }
    Ok(domains)
}

/// Deserialize the site subdomain suffix in its canonical form: lowercase
/// with no leading dot, so request hosts compare against it directly
pub fn deserialize_subdomain_suffix<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.map(|suffix| suffix.trim_matches('.').to_ascii_lowercase()))
}

