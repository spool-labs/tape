//! CLI config persisted at `~/.tape/cli-config.yaml`. Shared across the
//! user-facing `tape` CLI and the operator `tape-admin` CLI.
//!
//! Everything here is optional. A fresh install has no config file; defaults
//! are filled in by the caller's context loader.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const DEFAULT_SUBDIR: &str = ".tape";
const DEFAULT_CONFIG_FILE: &str = "cli-config.yaml";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("io error at {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("config parse: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, ConfigError>;

/// Unified config shared by every tape CLI tool.
///
/// - `rpc_url`, `keypair` — used by both `tape` and `tape-admin`.
/// - `active_cassette` — used only by `tape`. `tape-admin` ignores it.
///
/// Unset fields are serialized as absent (not null) so hand-edited files
/// stay tidy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliConfig {
    /// Default RPC URL (overridden by `-u` or env).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_url: Option<String>,

    /// Default payer/treasury keypair path (overridden by `-k` or env).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keypair: Option<PathBuf>,

    /// Currently-selected cassette (user CLI only). Set by `tape use`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_cassette: Option<PathBuf>,
}

impl CliConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.display().to_string(),
            source,
        })?;
        serde_yaml::from_str(&raw).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| ConfigError::Io {
                path: parent.display().to_string(),
                source,
            })?;
        }
        let yaml = serde_yaml::to_string(self).map_err(|e| ConfigError::Parse(e.to_string()))?;
        std::fs::write(path, yaml).map_err(|source| ConfigError::Io {
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

/// Default Solana CLI keypair path (`~/.config/solana/id.json`).
pub fn default_solana_keypair() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".config/solana/id.json"))
        .unwrap_or_else(|| PathBuf::from("~/.config/solana/id.json"))
}
