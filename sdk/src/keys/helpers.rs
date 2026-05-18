//! Shared helper functions for Tapedrive operations.
//!
//! This module provides common utilities used across CLI and other clients:
//! - Keypair loading (Solana, BLS, TLS)
//! - Hash parsing
//! - Committee operations
//! - RPC client creation

use std::path::Path;

use thiserror::Error;

use tape_api::state::Group;
use rpc_client::RpcClient;
use rpc_solana::{RpcConfig, SolanaRpc};
use tape_core::bls::BlsPrivateKey;
use tape_core::system::Member;
use tape_core::types::SpoolIndex;
use tape_crypto::Address;
use tape_crypto::ed25519::errors::KeypairFileError;
use tape_crypto::ed25519::Keypair;
use tape_crypto::Hash;

/// Errors from helper functions.
#[derive(Debug, Error)]
pub enum HelperError {
    #[error(transparent)]
    KeypairFile(#[from] KeypairFileError),

    #[error("Failed to read {path}: {message}")]
    FileRead { path: String, message: String },

    #[error("Invalid JSON in {path}: {message}")]
    JsonParse { path: String, message: String },

    /// Invalid hash format.
    #[error("Invalid {name} hex: {message}")]
    InvalidHex { name: String, message: String },

    /// Invalid hash length.
    #[error("{name} must be {expected} bytes (got {actual})")]
    InvalidLength { name: String, expected: usize, actual: usize },

    /// Node not found in committee.
    #[error("Node {0} not found in committee")]
    NodeNotInCommittee(Address),

    /// RPC client creation failed.
    #[error("Failed to create RPC client: {0}")]
    RpcClient(String),
}

// ============================================================================
// Keypair Loading
// ============================================================================

/// Load a Solana keypair from a JSON file.
///
/// The file should contain a JSON array of 64 bytes (secret key).
///
/// # Example
/// ```rust,ignore
/// let keypair = load_solana_keypair(Path::new("~/.config/solana/id.json"))?;
/// println!("Pubkey: {}", keypair.pubkey());
/// ```
pub fn load_solana_keypair(
    path: &Path,
) -> Result<solana_sdk::signature::Keypair, HelperError> {
    let keypair = Keypair::try_load_json_file(path)?;
    keypair.try_to_solana_keypair().map_err(|error| {
        KeypairFileError::InvalidKeypair(error.to_string()).into()
    })
}

/// Load a Tapedrive ed25519 keypair from a Solana-compatible JSON file.
pub fn load_ed25519_keypair(path: &Path) -> Result<Keypair, HelperError> {
    Keypair::try_load_json_file(path).map_err(Into::into)
}

/// Load a BLS private key from a JSON file.
///
/// The file should contain a JSON array of 32 bytes.
///
/// # Example
/// ```rust,ignore
/// let bls_key = load_bls_keypair(Path::new("bls_key.json"))?;
/// let pubkey = bls_key.public_key()?;
/// ```
pub fn load_bls_keypair(path: &Path) -> Result<BlsPrivateKey, HelperError> {
    let contents = std::fs::read(path).map_err(|e| HelperError::FileRead {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let bytes: Vec<u8> = serde_json::from_slice(&contents).map_err(|e| HelperError::JsonParse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    if bytes.len() != 32 {
        return Err(HelperError::InvalidLength {
            name: "BLS keypair".to_string(),
            expected: 32,
            actual: bytes.len(),
        });
    }

    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(BlsPrivateKey(tape_crypto::bls12254::min_sig::PrivKey(arr)))
}

/// Load an Ed25519 keypair from `path`, or generate and persist a fresh one
/// if the file does not exist.
///
/// The file format is a JSON array of 64 bytes (Solana-compatible keypair
/// encoding). Parent directories are created if missing. Generated files are
/// written with mode 0600 on Unix.
pub fn ensure_ed25519_keypair(path: &Path) -> Result<Keypair, HelperError> {
    if path.exists() {
        return load_ed25519_keypair(path);
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| HelperError::FileRead {
                path: parent.display().to_string(),
                message: e.to_string(),
            })?;
        }
    }

    let mut rng = rand::thread_rng();
    let keypair = Keypair::new(&mut rng);
    let bytes: [u8; 64] = keypair.to_keypair_bytes();
    let json = serde_json::to_vec(&bytes.to_vec()).map_err(|e| HelperError::JsonParse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    std::fs::write(path, &json).map_err(|e| HelperError::FileRead {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        let _ = std::fs::set_permissions(path, perms);
    }

    Ok(keypair)
}

// ============================================================================
// Hash Parsing
// ============================================================================

/// Parse a hex-encoded 32-byte hash.
///
/// # Example
/// ```rust,ignore
/// let hash = parse_hash("abcd1234...", "merkle root")?;
/// ```
pub fn parse_hash(hex_str: &str, name: &str) -> Result<Hash, HelperError> {
    let bytes = hex::decode(hex_str).map_err(|e| HelperError::InvalidHex {
        name: name.to_string(),
        message: e.to_string(),
    })?;

    if bytes.len() != 32 {
        return Err(HelperError::InvalidLength {
            name: name.to_string(),
            expected: 32,
            actual: bytes.len(),
        });
    }

    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(Hash::from(arr))
}

/// Parse a hex-encoded byte array of arbitrary length.
///
/// # Example
/// ```rust,ignore
/// let bitmap = parse_hex_bytes("ff00ff00...", "bitmap", 16)?;
/// ```
pub fn parse_hex_bytes(hex_str: &str, name: &str, expected_len: usize) -> Result<Vec<u8>, HelperError> {
    let bytes = hex::decode(hex_str).map_err(|e| HelperError::InvalidHex {
        name: name.to_string(),
        message: e.to_string(),
    })?;

    if bytes.len() != expected_len {
        return Err(HelperError::InvalidLength {
            name: name.to_string(),
            expected: expected_len,
            actual: bytes.len(),
        });
    }

    Ok(bytes)
}

// ============================================================================
// Committee Operations
// ============================================================================

/// Find a node's index in the committee by node account address.
///
/// Returns `None` if the node is not in the committee.
///
/// # Example
/// ```rust,ignore
/// let system = client.get_system().await?;
/// if let Some(idx) = find_member_index(&committee, node) {
///     println!("Node is at index {}", idx);
/// }
/// ```
pub fn find_member_index(committee: &[Member], node: Address) -> Option<usize> {
    committee.iter().position(|m| m.node == node)
}

/// Get the spool indices assigned to a node in the current epoch.
///
/// This scans the current epoch's group accounts and returns all spools assigned to it.
///
/// # Example
/// ```rust,ignore
/// let system = client.get_system().await?;
/// let spools = get_node_assigned_spools(&groups, node)?;
/// println!("Node is responsible for {} spools", spools.len());
/// ```
pub fn get_node_assigned_spools(
    groups: &[Group],
    node: Address,
) -> Result<Vec<SpoolIndex>, HelperError> {
    let spools = groups
        .iter()
        .flat_map(|group| {
            group
                .spools
                .iter()
                .enumerate()
                .filter_map(move |(position, spool)| {
                    (spool.node == node).then_some(group.id.spool_at(position))
                })
        })
        .collect::<Vec<_>>();

    if spools.is_empty() {
        return Err(HelperError::NodeNotInCommittee(node));
    }

    Ok(spools)
}

// ============================================================================
// RPC Client Creation
// ============================================================================

/// Create an RPC client from a single endpoint URL.
///
/// # Example
/// ```rust,ignore
/// let client = create_rpc_client("https://api.mainnet-beta.solana.com")?;
/// let system = client.get_system().await?;
/// ```
pub fn create_rpc_client(endpoint: &str) -> Result<RpcClient<SolanaRpc>, HelperError> {
    let config = RpcConfig {
        endpoints: vec![endpoint.to_string()],
        ..Default::default()
    };

    RpcClient::new(config).map_err(|e| HelperError::RpcClient(e.to_string()))
}

/// Create an RPC client from configuration.
///
/// # Example
/// ```rust,ignore
/// let config = RpcConfig {
///     endpoints: vec!["https://api.mainnet-beta.solana.com".into()],
///     ..Default::default()
/// };
/// let client = create_rpc_client_with_config(config)?;
/// ```
pub fn create_rpc_client_with_config(config: RpcConfig) -> Result<RpcClient<SolanaRpc>, HelperError> {
    RpcClient::new(config).map_err(|e| HelperError::RpcClient(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hash_valid() {
        let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let result = parse_hash(hex, "test");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_hash_invalid_hex() {
        let hex = "not_valid_hex";
        let result = parse_hash(hex, "test");
        assert!(matches!(result, Err(HelperError::InvalidHex { .. })));
    }

    #[test]
    fn test_parse_hash_wrong_length() {
        let hex = "0123456789abcdef"; // Only 8 bytes
        let result = parse_hash(hex, "test");
        assert!(matches!(result, Err(HelperError::InvalidLength { .. })));
    }

    #[test]
    fn test_parse_hex_bytes() {
        let hex = "ff00ff00";
        let result = parse_hex_bytes(hex, "test", 4);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![0xff, 0x00, 0xff, 0x00]);
    }
}
