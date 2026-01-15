//! Utility functions for the storage node.
//!
//! Common helper functions used across multiple modules.

use std::path::{Path, PathBuf};

use solana_sdk::signature::Keypair;
use tape_core::bls::BlsPrivateKey;

/// Error type for keypair loading operations.
#[derive(Debug, thiserror::Error)]
pub enum KeypairError {
    #[error("failed to load keypair: {0}")]
    Keypair(String),

    #[error("failed to load BLS keypair: {0}")]
    BlsKeypair(String),
}

/// Load a Solana keypair from a JSON file.
///
/// The file should contain a JSON array of 64 bytes (Ed25519 secret key).
pub fn load_keypair(path: &str) -> Result<Keypair, KeypairError> {
    let keypair_bytes = std::fs::read(path)
        .map_err(|e| KeypairError::Keypair(format!("Failed to read keypair file: {}", e)))?;

    let keypair_json: Vec<u8> = serde_json::from_slice(&keypair_bytes)
        .map_err(|e| KeypairError::Keypair(format!("Failed to parse keypair JSON: {}", e)))?;

    Keypair::from_bytes(&keypair_json)
        .map_err(|e| KeypairError::Keypair(format!("Invalid keypair bytes: {}", e)))
}

/// Load a BLS private key from a JSON file.
///
/// The file should contain a JSON array of 32 bytes.
pub fn load_bls_keypair(path: &Path) -> Result<BlsPrivateKey, KeypairError> {
    let contents = std::fs::read(path)
        .map_err(|e| KeypairError::BlsKeypair(format!("Failed to read BLS keypair file: {}", e)))?;

    let bytes: Vec<u8> = serde_json::from_slice(&contents)
        .map_err(|e| KeypairError::BlsKeypair(format!("Failed to parse BLS keypair JSON: {}", e)))?;

    if bytes.len() != 32 {
        return Err(KeypairError::BlsKeypair(format!(
            "BLS keypair must be 32 bytes, got {}",
            bytes.len()
        )));
    }

    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(BlsPrivateKey(tape_crypto::bls12254::min_sig::PrivKey(arr)))
}

/// Default node config file path (~/.tape/node.yaml).
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

/// Expand ~ and environment variables in a path.
pub fn expand_path(path: &str) -> PathBuf {
    shellexpand::full(path)
        .map(|s| PathBuf::from(s.as_ref()))
        .unwrap_or_else(|_| PathBuf::from(path))
}

/// Get the current Unix timestamp in seconds.
pub fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_path_no_expansion() {
        let path = "/absolute/path/to/file";
        assert_eq!(expand_path(path), PathBuf::from(path));
    }

    #[test]
    fn test_expand_path_with_tilde() {
        let expanded = expand_path("~/test");
        // Should not start with ~ after expansion
        assert!(!expanded.to_string_lossy().starts_with('~'));
    }

    #[test]
    fn test_default_config_path() {
        let path = default_config_path();
        assert!(path.to_string_lossy().contains("node.yaml"));
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        // Should be a reasonable Unix timestamp (after year 2020)
        assert!(ts > 1577836800); // Jan 1, 2020
    }
}
