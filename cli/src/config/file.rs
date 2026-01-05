//! Configuration file loading and management.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::cluster::Cluster;

/// Default CLI config file path.
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("config.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/config.yaml"))
}

/// Configuration file structure.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigFile {
    /// Config file version.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Default cluster (l, m, d, t, or URL).
    #[serde(default)]
    pub cluster: Option<String>,

    /// Default storage node addresses.
    #[serde(default)]
    pub nodes: Vec<String>,

    /// Keypair paths by name.
    #[serde(default)]
    pub keys: HashMap<String, String>,

    /// Node operator settings.
    #[serde(default)]
    pub node: NodeConfig,

    /// Output format (json, table, plain).
    #[serde(default)]
    pub output: Option<String>,

    /// Logging level.
    #[serde(default)]
    pub log_level: Option<String>,
}

fn default_version() -> u32 {
    1
}

/// Node operator configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeConfig {
    /// Node display name.
    pub name: Option<String>,

    /// Commission rate in basis points (0-10000).
    pub commission: Option<u64>,

    /// Network address (host:port).
    pub address: Option<String>,

    /// Path to BLS keypair.
    pub bls_key: Option<String>,

    /// Path to TLS keypair.
    pub tls_key: Option<String>,
}

impl ConfigFile {
    /// Load config from default path.
    pub fn load() -> Result<Self> {
        Self::load_from(&default_config_path())
    }

    /// Load config from a specific path.
    pub fn load_from(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }

        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))
    }

    /// Save config to default path.
    pub fn save(&self) -> Result<()> {
        self.save_to(&default_config_path())
    }

    /// Save config to a specific path.
    pub fn save_to(&self, path: &Path) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create config directory: {}", parent.display()))?;
        }

        let contents = serde_yaml::to_string(self)
            .context("Failed to serialize config")?;

        std::fs::write(path, contents)
            .with_context(|| format!("Failed to write config file: {}", path.display()))
    }

    /// Get the configured cluster.
    pub fn get_cluster(&self) -> Result<Option<Cluster>> {
        match &self.cluster {
            Some(s) => Ok(Some(s.parse().map_err(|e: String| anyhow::anyhow!(e))?)),
            None => Ok(None),
        }
    }

    /// Get the default keypair path.
    pub fn default_keypair(&self) -> Option<PathBuf> {
        self.keys
            .get("default")
            .map(|s| expand_path(s))
    }

    /// Get a named keypair path.
    pub fn keypair(&self, name: &str) -> Option<PathBuf> {
        self.keys
            .get(name)
            .map(|s| expand_path(s))
    }
}

/// Expand ~ and environment variables in a path.
pub fn expand_path(path: &str) -> PathBuf {
    shellexpand::full(path)
        .map(|s| PathBuf::from(s.as_ref()))
        .unwrap_or_else(|_| PathBuf::from(path))
}

/// Generate default config file content.
pub fn default_config_content() -> &'static str {
    r#"# Tapedrive CLI Configuration
version: 1

# Default cluster (l, m, d, t, or URL)
cluster: d

# Default storage node addresses
nodes: []

# Keypair paths
keys:
  default: ~/.config/solana/id.json

# Node operator settings (used by `tape node register` if not overridden)
node:
  # name: "my-node"
  # commission: 500                    # 5% in basis points
  # address: "node.example.com:8080"
  # bls_key: ~/.tape/keys/bls.json
  # tls_key: ~/.tape/keys/tls.json

# Output format (json, table, plain)
output: table

# Logging level
log_level: info
"#
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConfigFile::default();
        assert_eq!(config.version, 0); // default() gives 0, loading gives 1
    }

    #[test]
    fn test_expand_path() {
        let path = expand_path("~/.tape/config.yaml");
        assert!(!path.to_string_lossy().contains('~'));
    }
}
