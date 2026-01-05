//! On-chain discovery of storage node addresses.
//!
//! This module provides functions to resolve storage node network addresses
//! from on-chain state. It bridges the gap between:
//! - The System account (which contains the committee with NodeIds)
//! - The Node accounts (which contain network addresses)
//!
//! # Resolution Flow
//!
//! ```text
//! System.committee.iter()        →  CommitteeMember { id: NodeId, ... }
//!                                     ↓
//! tape-client::get_node_by_id()  →  Node { metadata.network_address, ... }
//!                                     ↓
//! NetworkAddress.to_socket_addr()→  SocketAddr
//!                                     ↓
//! format!("http://{}", addr)     →  String URL for HTTP client
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use tape_sdk::discovery::discover_committee_addresses;
//! use tape_client::RpcConfig;
//!
//! let rpc_config = RpcConfig {
//!     endpoints: vec!["https://api.mainnet-beta.solana.com".into()],
//!     ..Default::default()
//! };
//!
//! let addresses = discover_committee_addresses(&rpc_config).await?;
//! println!("Found {} storage nodes", addresses.len());
//! ```

use tape_client::{RpcConfig, TapeClient as RpcClient};
use tape_core::types::NodeId;
use thiserror::Error;

/// Errors that can occur during node discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// Failed to create RPC client.
    #[error("Failed to create RPC client: {0}")]
    ClientCreation(String),

    /// Failed to fetch system state.
    #[error("Failed to fetch system state: {0}")]
    SystemFetch(String),

    /// Failed to fetch node by ID.
    #[error("Failed to fetch node {node_id}: {message}")]
    NodeFetch { node_id: NodeId, message: String },

    /// Invalid network address format.
    #[error("Invalid network address for node {node_id}: {message}")]
    InvalidAddress { node_id: NodeId, message: String },

    /// No active nodes found in committee.
    #[error("No active nodes found in committee")]
    NoActiveNodes,
}

/// Result of node discovery, containing resolved addresses and any warnings.
#[derive(Debug, Default)]
pub struct DiscoveryResult {
    /// Successfully resolved node addresses (HTTP URLs).
    pub addresses: Vec<String>,

    /// Warnings for nodes that could not be resolved.
    pub warnings: Vec<String>,
}

impl DiscoveryResult {
    /// Returns true if at least one node was discovered.
    pub fn has_nodes(&self) -> bool {
        !self.addresses.is_empty()
    }

    /// Returns the number of discovered nodes.
    pub fn node_count(&self) -> usize {
        self.addresses.len()
    }
}

/// Discover storage node addresses from the current on-chain committee.
///
/// This function:
/// 1. Fetches the System account to get the current committee
/// 2. For each committee member, fetches the Node account by NodeId
/// 3. Extracts and formats the network address as an HTTP URL
///
/// # Arguments
/// * `rpc_config` - RPC configuration for connecting to Solana
///
/// # Returns
/// A `DiscoveryResult` containing resolved addresses and any warnings.
///
/// # Errors
/// Returns an error if the RPC client cannot be created or the system state
/// cannot be fetched. Individual node lookup failures are recorded as warnings.
pub async fn discover_committee_addresses(
    rpc_config: &RpcConfig,
) -> Result<DiscoveryResult, DiscoveryError> {
    let client = RpcClient::new(rpc_config.clone())
        .map_err(|e| DiscoveryError::ClientCreation(e.to_string()))?;

    // Fetch system to get current committee
    let system = client
        .get_system()
        .await
        .map_err(|e| DiscoveryError::SystemFetch(e.to_string()))?;

    let mut result = DiscoveryResult::default();

    // Iterate over committee members and resolve their network addresses
    for member in system.committee.iter() {
        // Skip empty slots (NodeId 0 means unoccupied)
        if member.id == NodeId(0) {
            continue;
        }

        // Look up Node account by NodeId to get network_address
        match client.get_node_by_id(member.id).await {
            Ok((_pubkey, node)) => {
                // Convert NetworkAddress to string format
                match node.metadata.network_address.to_socket_addr() {
                    Ok(socket_addr) => {
                        result.addresses.push(format!("http://{}", socket_addr));
                    }
                    Err(e) => {
                        result.warnings.push(format!(
                            "Invalid network address for node {}: {}",
                            member.id, e
                        ));
                    }
                }
            }
            Err(e) => {
                result.warnings.push(format!(
                    "Failed to fetch node {}: {}",
                    member.id, e
                ));
            }
        }
    }

    Ok(result)
}

/// Discover storage node addresses, returning an error if none are found.
///
/// This is a convenience wrapper around `discover_committee_addresses` that
/// returns an error instead of an empty result when no nodes can be resolved.
///
/// # Arguments
/// * `rpc_config` - RPC configuration for connecting to Solana
///
/// # Returns
/// A vector of HTTP URLs for storage nodes.
///
/// # Errors
/// Returns `DiscoveryError::NoActiveNodes` if no nodes can be resolved.
pub async fn discover_committee_addresses_required(
    rpc_config: &RpcConfig,
) -> Result<Vec<String>, DiscoveryError> {
    let result = discover_committee_addresses(rpc_config).await?;

    if result.addresses.is_empty() {
        return Err(DiscoveryError::NoActiveNodes);
    }

    Ok(result.addresses)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_result_default() {
        let result = DiscoveryResult::default();
        assert!(!result.has_nodes());
        assert_eq!(result.node_count(), 0);
    }

    #[test]
    fn test_discovery_result_with_nodes() {
        let result = DiscoveryResult {
            addresses: vec!["http://192.168.1.1:8080".to_string()],
            warnings: vec![],
        };
        assert!(result.has_nodes());
        assert_eq!(result.node_count(), 1);
    }
}
