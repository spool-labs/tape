//! Meta column family for node metadata
//!
//! Stores key-value pairs for node configuration and state:
//! - node_status: NodeStatus (wincode-serialized)
//! - cluster_hash: Hash (32 bytes)
//! - chain_epoch: EpochNumber
//! - node_address: Pubkey

use store::Column;

/// Column family for node metadata
///
/// Key: String (e.g., "node_status", "cluster_hash", "chain_epoch", "node_address")
/// Value: Vec<u8> (serialized data, format depends on key)
pub struct MetaCol;

impl Column for MetaCol {
    const CF_NAME: &'static str = "meta";
    type Key = String;
    type Value = Vec<u8>;
}
