//! Secrets read from files the config names

use std::fs;
use std::path::Path;

use crate::config::error::ConfigError;

/// The trimmed contents of a secret file, which must not be empty
pub fn read_secret(path: &Path) -> Result<String, ConfigError> {
    let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let secret = contents.trim();
    if secret.is_empty() {
        return Err(ConfigError::EmptySecret(path.to_path_buf()));
    }
    Ok(secret.to_string())
}
