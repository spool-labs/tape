//! CLI config persisted at `~/.tape/cli-config.yaml`. Stores the user's
//! default RPC, keypair path, and currently-`use`'d cassette.
//!
//! Everything here is optional. A fresh install has no config file; defaults
//! are filled in by [`Context::load`].

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const DEFAULT_SUBDIR: &str = ".tape";
const DEFAULT_CONFIG_FILE: &str = "cli-config.yaml";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliConfig {
    /// Default RPC URL (overridden by `-u` or env).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,

    /// Default payer keypair path (overridden by `-k` or env).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keypair: Option<PathBuf>,

    /// Currently-selected cassette. Set by `tape use`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_cassette: Option<PathBuf>,
}

impl CliConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path).map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        serde_yaml::from_str(&raw).map_err(|e| Error::Config(e.to_string()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| Error::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }
        let yaml = serde_yaml::to_string(self).map_err(|e| Error::Config(e.to_string()))?;
        std::fs::write(path, yaml).map_err(|source| Error::Io {
            path: path.display().to_string(),
            source,
        })?;
        Ok(())
    }
}

/// Default path to the config file: `~/.tape/cli-config.yaml`.
pub fn default_config_path() -> PathBuf {
    base_dir().join(DEFAULT_CONFIG_FILE)
}

/// Root of the operator's tape state (`~/.tape/`).
pub fn base_dir() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(DEFAULT_SUBDIR))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SUBDIR))
}

/// Default cassette-keypair directory: `~/.tape/cassettes/`.
pub fn cassettes_dir() -> PathBuf {
    base_dir().join("cassettes")
}

/// Default receipt directory: `~/.tape/receipts/`.
pub fn receipts_dir() -> PathBuf {
    base_dir().join("receipts")
}

/// Expand a leading `~/` in paths relative to `$HOME`.
pub fn expand(path: &Path) -> PathBuf {
    let s = match path.to_str() {
        Some(s) => s,
        None => return path.to_path_buf(),
    };
    if let Some(rest) = s.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest);
        }
    } else if s == "~" {
        if let Some(home) = dirs::home_dir() {
            return home;
        }
    }
    path.to_path_buf()
}
