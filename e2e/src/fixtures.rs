//! Test fixtures and data generators for e2e testing.
//!
//! Provides utilities for generating random test data, temporary files,
//! and common test setup patterns.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rand::RngCore;
use solana_sdk::signature::Keypair;
use tempfile::{NamedTempFile, TempDir};

/// Generate random bytes of specified size.
pub fn random_bytes(size: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

/// Generate a random blob of specified size.
///
/// Alias for `random_bytes` with a more descriptive name.
pub fn random_blob(size: usize) -> Vec<u8> {
    random_bytes(size)
}

/// Generate a small test blob (1 KB).
pub fn small_blob() -> Vec<u8> {
    random_blob(1024)
}

/// Generate a medium test blob (1 MB).
pub fn medium_blob() -> Vec<u8> {
    random_blob(1024 * 1024)
}

/// Generate a large test blob (10 MB).
pub fn large_blob() -> Vec<u8> {
    random_blob(10 * 1024 * 1024)
}

/// Create a temporary file with random content.
pub fn temp_file_with_content(content: &[u8]) -> Result<NamedTempFile> {
    let file = NamedTempFile::new().context("Failed to create temp file")?;
    std::fs::write(file.path(), content).context("Failed to write to temp file")?;
    Ok(file)
}

/// Create a temporary file with random content of specified size.
pub fn temp_file_random(size: usize) -> Result<NamedTempFile> {
    temp_file_with_content(&random_bytes(size))
}

/// Create a temporary file with a small blob.
pub fn temp_file_small() -> Result<NamedTempFile> {
    temp_file_with_content(&small_blob())
}

/// Create a temporary file with a medium blob.
pub fn temp_file_medium() -> Result<NamedTempFile> {
    temp_file_with_content(&medium_blob())
}

/// Create a temporary directory.
pub fn temp_dir() -> Result<TempDir> {
    TempDir::new().context("Failed to create temp directory")
}

/// Generate a new random keypair.
pub fn random_keypair() -> Keypair {
    Keypair::new()
}

/// Generate a random keypair and save it to a file.
pub fn random_keypair_file() -> Result<(NamedTempFile, Keypair)> {
    let keypair = Keypair::new();
    let file = NamedTempFile::new().context("Failed to create temp file")?;
    let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
    std::fs::write(file.path(), &json)?;
    Ok((file, keypair))
}

/// Save a keypair to a file.
pub fn save_keypair(keypair: &Keypair, path: &Path) -> Result<()> {
    let json = serde_json::to_string(&keypair.to_bytes().to_vec())?;
    std::fs::write(path, &json).context("Failed to write keypair file")
}

/// Load a keypair from a file.
pub fn load_keypair(path: &Path) -> Result<Keypair> {
    tape_sdk::load_solana_keypair(path)
        .map_err(|e| anyhow::anyhow!("{}", e))
}

/// Get the default Solana keypair path.
pub fn default_keypair_path() -> PathBuf {
    dirs::home_dir()
        .map(|h: PathBuf| h.join(".config/solana/id.json"))
        .unwrap_or_else(|| PathBuf::from("~/.config/solana/id.json"))
}

/// Load the default Solana keypair.
pub fn default_keypair() -> Result<Keypair> {
    load_keypair(&default_keypair_path())
}

/// Generate a random hex-encoded hash (32 bytes = 64 hex chars).
pub fn random_hash_hex() -> String {
    let bytes = random_bytes(32);
    hex::encode(bytes)
}

/// Generate a deterministic blob for verification tests.
///
/// Creates a blob where each byte is derived from the seed and position,
/// allowing verification that data hasn't been corrupted.
pub fn deterministic_blob(size: usize, seed: u64) -> Vec<u8> {
    use rand::SeedableRng;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
    let mut bytes = vec![0u8; size];
    rng.fill_bytes(&mut bytes);
    bytes
}

/// Verify a deterministic blob matches the expected pattern.
pub fn verify_deterministic_blob(blob: &[u8], seed: u64) -> bool {
    let expected = deterministic_blob(blob.len(), seed);
    blob == expected
}

/// Configuration for a test environment.
#[derive(Debug, Clone)]
pub struct TestEnvConfig {
    /// Path to the tape binary.
    pub tape_bin: PathBuf,
    /// Base directory for test data.
    pub base_dir: PathBuf,
    /// Base port for nodes.
    pub base_port: u16,
    /// Number of nodes to spawn.
    pub node_count: usize,
    /// Initial stake per node.
    pub stake_per_node: u64,
    /// Whether to use a local validator.
    pub use_local_validator: bool,
}

impl Default for TestEnvConfig {
    fn default() -> Self {
        Self {
            tape_bin: PathBuf::from("target/debug/tape"),
            base_dir: PathBuf::from("/tmp/tape-e2e"),
            base_port: 8080,
            node_count: 1,
            stake_per_node: 1000,
            use_local_validator: true,
        }
    }
}

impl TestEnvConfig {
    /// Create config for a single-node test.
    pub fn single_node() -> Self {
        Self::default()
    }

    /// Create config for a multi-node test.
    pub fn multi_node(count: usize) -> Self {
        Self {
            node_count: count,
            ..Default::default()
        }
    }

    /// Create config for a full committee test (24+ nodes).
    pub fn full_committee() -> Self {
        Self {
            node_count: 24,
            ..Default::default()
        }
    }

    /// Set the tape binary path.
    pub fn with_tape_bin(mut self, path: impl Into<PathBuf>) -> Self {
        self.tape_bin = path.into();
        self
    }

    /// Set the base directory.
    pub fn with_base_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.base_dir = path.into();
        self
    }

    /// Set the base port.
    pub fn with_base_port(mut self, port: u16) -> Self {
        self.base_port = port;
        self
    }

    /// Set stake per node.
    pub fn with_stake(mut self, stake: u64) -> Self {
        self.stake_per_node = stake;
        self
    }
}

/// Common file sizes for testing.
pub mod sizes {
    /// 1 KB
    pub const KB: usize = 1024;
    /// 1 MB
    pub const MB: usize = 1024 * 1024;
    /// 1 GB
    pub const GB: usize = 1024 * 1024 * 1024;

    /// Small file (1 KB)
    pub const SMALL: usize = KB;
    /// Medium file (1 MB)
    pub const MEDIUM: usize = MB;
    /// Large file (10 MB)
    pub const LARGE: usize = 10 * MB;
    /// Very large file (100 MB)
    pub const VERY_LARGE: usize = 100 * MB;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_bytes() {
        let bytes1 = random_bytes(100);
        let bytes2 = random_bytes(100);

        assert_eq!(bytes1.len(), 100);
        assert_eq!(bytes2.len(), 100);
        assert_ne!(bytes1, bytes2); // Should be different (extremely high probability)
    }

    #[test]
    fn test_deterministic_blob() {
        let blob1 = deterministic_blob(100, 42);
        let blob2 = deterministic_blob(100, 42);
        let blob3 = deterministic_blob(100, 43);

        assert_eq!(blob1, blob2); // Same seed = same blob
        assert_ne!(blob1, blob3); // Different seed = different blob
    }

    #[test]
    fn test_verify_deterministic_blob() {
        let blob = deterministic_blob(100, 42);
        assert!(verify_deterministic_blob(&blob, 42));
        assert!(!verify_deterministic_blob(&blob, 43));
    }

    #[test]
    fn test_temp_file() {
        let content = b"test content";
        let file = temp_file_with_content(content).unwrap();
        let read_back = std::fs::read(file.path()).unwrap();
        assert_eq!(read_back, content);
    }

    #[test]
    fn test_random_hash_hex() {
        let hash = random_hash_hex();
        assert_eq!(hash.len(), 64); // 32 bytes = 64 hex chars
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
