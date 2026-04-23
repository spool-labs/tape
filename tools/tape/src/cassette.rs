//! Cassette = on-disk Solana-style keypair for a TapeKey. Users create one
//! with `tape create`, target it with `--cassette <path>`, or rely on the
//! `use`'d default in `cli-config.yaml`.
//!
//! Convention: files live at `~/.tape/cassettes/<base58-pubkey>.json` when
//! created without an explicit `--out` path.

use std::path::{Path, PathBuf};

use tape_sdk::keys::tape_key::TapeKey;

use crate::config;
use crate::error::{Error, Result};

/// Load a cassette keypair from disk.
pub fn load(path: &Path) -> Result<TapeKey> {
    TapeKey::load(path).map_err(|e| Error::Keypair(e.to_string()))
}

/// Generate a new cassette keypair.
pub fn generate() -> TapeKey {
    TapeKey::generate()
}

/// Save a cassette keypair, creating parent directories if needed.
pub fn save(key: &TapeKey, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| Error::Io {
            path: parent.display().to_string(),
            source,
        })?;
    }
    key.save(path).map_err(|source| Error::Io {
        path: path.display().to_string(),
        source,
    })
}

/// Default on-disk path for a freshly created cassette.
pub fn default_path(key: &TapeKey) -> PathBuf {
    let pk = key.address().to_string();
    config::cassettes_dir().join(format!("{pk}.json"))
}
