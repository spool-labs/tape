//! Meta column family for node metadata
//!
//! Stores key-value pairs for node configuration and state:
//! - node_status: NodeStatus
//! - cluster_hash: Hash (32 bytes)
//! - current_epoch: EpochNumber

use store::Column;

/// Column family for node metadata
///
/// Key: String (e.g., "node_status", "cluster_hash", "current_epoch")
/// Value: Vec<u8> (serialized data, format depends on key)
pub struct Meta;

impl Column for Meta {
    const CF_NAME: &'static str = "meta";
    type Key = String;
    type Value = Vec<u8>;
}
