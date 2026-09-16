//! Tape keys, persisted before the tape they control is reserved

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use tape_crypto::address::Address;
use tape_sdk::keys::tape_key::TapeKey;

use crate::chain::error::ChainError;

/// Only the service's user may read a tape key
const KEY_FILE_MODE: u32 = 0o600;

/// Generate a tape key and write it durably into `key_dir`
pub fn persist_new_key(key_dir: &Path) -> Result<(Address, PathBuf), ChainError> {
    let key = TapeKey::generate();
    let address = key.address();
    let path = key_dir.join(format!("{address}.json"));

    fs::create_dir_all(key_dir).map_err(|error| key_error(key_dir, error.to_string()))?;
    let json = serde_json::to_vec(&key.keypair().to_keypair_bytes().to_vec())
        .map_err(|error| key_error(&path, error.to_string()))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(KEY_FILE_MODE)
        .open(&path)
        .map_err(|error| key_error(&path, error.to_string()))?;
    file.write_all(&json)
        .and_then(|()| file.sync_all())
        .map_err(|error| key_error(&path, error.to_string()))?;
    Ok((address, path))
}

/// Load a tape key and check it controls the expected tape
pub async fn load_key(path: &Path, expected: Address) -> Result<TapeKey, ChainError> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|error| key_error(path, error.to_string()))?;
    let key = TapeKey::from_json_bytes(&bytes).map_err(|error| key_error(path, error.to_string()))?;
    if key.address() != expected {
        return Err(ChainError::KeyMismatch {
            path: path.to_path_buf(),
            expected,
            actual: key.address(),
        });
    }
    Ok(key)
}

fn key_error(path: &Path, reason: String) -> ChainError {
    ChainError::TapeKey {
        path: path.to_path_buf(),
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // a persisted key loads back for its tape and refuses any other
    #[tokio::test]
    async fn round_trip() {
        let dir = tempfile::tempdir().expect("temp dir");

        let (address, path) = persist_new_key(&dir.path().join("tapes")).expect("persist");

        let key = load_key(&path, address).await.expect("load");
        assert_eq!(key.address(), address);
        let other = Address::new([7; 32]);
        assert!(matches!(load_key(&path, other).await, Err(ChainError::KeyMismatch { .. })));
    }
}
