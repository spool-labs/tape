//! Shared helper functions for Tapedrive operations.
//!
//! This module provides common utilities used across CLI and other clients:
//! - Keypair loading (Solana, BLS, TLS)
//! - Hash parsing
//! - Committee operations
//! - RPC client creation

use std::path::Path;

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use thiserror::Error;

use rpc_client::{RpcConfig, RpcClient, SolanaRpc};
use tape_core::bls::BlsPrivateKey;
use tape_core::spooler::SpoolIndex;
use tape_core::system::Committee;
use tape_core::types::NodeId;
use tape_crypto::Hash;

/// Errors from helper functions.
#[derive(Debug, Error)]
pub enum HelperError {
    /// Failed to read file.
    #[error("Failed to read file {path}: {message}")]
    FileRead { path: String, message: String },

    /// Failed to parse JSON.
    #[error("Failed to parse JSON from {path}: {message}")]
    JsonParse { path: String, message: String },

    /// Invalid keypair data.
    #[error("Invalid keypair data: {0}")]
    InvalidKeypair(String),

    /// Invalid hash format.
    #[error("Invalid {name} hex: {message}")]
    InvalidHex { name: String, message: String },

    /// Invalid hash length.
    #[error("{name} must be {expected} bytes (got {actual})")]
    InvalidLength { name: String, expected: usize, actual: usize },

    /// Node not found in committee.
    #[error("Node {0} not found in committee")]
    NodeNotInCommittee(NodeId),

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
pub fn load_solana_keypair(path: &Path) -> Result<Keypair, HelperError> {
    let contents = std::fs::read(path).map_err(|e| HelperError::FileRead {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let bytes: Vec<u8> = serde_json::from_slice(&contents).map_err(|e| HelperError::JsonParse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    Keypair::from_bytes(&bytes).map_err(|e| HelperError::InvalidKeypair(e.to_string()))
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

/// Load a TLS keypair from a JSON file.
///
/// The file should contain a JSON array of 64 bytes (ed25519 keypair in Solana format).
///
/// # Example
/// ```rust,ignore
/// let tls_keypair = load_tls_keypair(Path::new("tls.json"))?;
/// let pubkey = tls_keypair.pubkey();
/// ```
pub fn load_tls_keypair(path: &Path) -> Result<Keypair, HelperError> {
    let contents = std::fs::read(path).map_err(|e| HelperError::FileRead {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    let bytes: Vec<u8> = serde_json::from_slice(&contents).map_err(|e| HelperError::JsonParse {
        path: path.display().to_string(),
        message: e.to_string(),
    })?;

    if bytes.len() != 64 {
        return Err(HelperError::InvalidLength {
            name: "TLS keypair".to_string(),
            expected: 64,
            actual: bytes.len(),
        });
    }

    Keypair::from_bytes(&bytes).map_err(|e| HelperError::InvalidKeypair(e.to_string()))
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

/// Find a node's index in the committee by NodeId.
///
/// Returns `None` if the node is not in the committee.
///
/// # Example
/// ```rust,ignore
/// let system = client.get_system().await?;
/// if let Some(idx) = find_member_index(&system.committee, node_id) {
///     println!("Node is at index {}", idx);
/// }
/// ```
pub fn find_member_index<const N: usize>(committee: &Committee<N>, node_id: NodeId) -> Option<usize> {
    committee.iter().position(|m| m.id == node_id)
}

/// Get the spool indices assigned to a node in the current epoch.
///
/// This looks up the node's member index and returns all spools assigned to it.
///
/// # Example
/// ```rust,ignore
/// let system = client.get_system().await?;
/// let node = client.get_node(&authority).await?;
/// let spools = get_node_assigned_spools(&system.committee, &system.spools, node.id)?;
/// println!("Node is responsible for {} spools", spools.len());
/// ```
pub fn get_node_assigned_spools<const N: usize, const S: usize>(
    committee: &Committee<N>,
    spools: &tape_core::spooler::SpoolAssignment<S>,
    node_id: NodeId,
) -> Result<Vec<SpoolIndex>, HelperError> {
    let member_index = find_member_index(committee, node_id)
        .ok_or(HelperError::NodeNotInCommittee(node_id))?;

    Ok(spools.spools_for_member(member_index))
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
