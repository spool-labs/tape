use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use solana_sdk::{signature::Keypair, signer::EncodableKey};

/// Returns the keypair path. If `keypair_path` is `None`, defaults to `~/.config/solana/id.json`.
pub fn get_keypair_path<F: AsRef<Path>>(keypair_path: Option<F>) -> PathBuf {
    keypair_path
        .map(|p| p.as_ref().to_path_buf())
        .unwrap_or_else(|| {
            dirs::home_dir()
                .expect("Could not find home directory")
                .join(".config/solana/id.json")
        })
}

/// Loads a `Keypair` from the given path.
///
/// - If the file does **not exist**, a new keypair is generated and written to the path.
/// - If the file exists but is **malformed** or **unreadable**, returns a detailed error.
pub fn load_keypair<F: AsRef<Path>>(path: F) -> Result<Keypair> {
    let path = path.as_ref();

    if path.exists() {
        // Try to read keypair from file
        return Keypair::read_from_file(path)
            .map_err(|e| anyhow!("Failed to read keypair from {}: {}", path.display(), e));
    }

    // File does not exist — generate and save new keypair
    let keypair = Keypair::new();
    keypair
        .write_to_file(path)
        .map_err(|e| anyhow!("Failed to write new keypair to {}: {}", path.display(), e))?;

    Ok(keypair)
}
