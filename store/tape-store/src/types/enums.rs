//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Node status in the network
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum NodeStatus {
    /// Node is registered but not in committee
    Standby = 0,
    /// Node is active in the committee
    Active = 1,
    /// Node is recovering data from peers
    Recovering = 2,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Standby
    }
}

/// Status of a spool assignment
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SpoolStatus {
    /// Not assigned
    None = 0,
    /// Fully synced and serving requests
    Active = 1,
    /// Currently syncing data from peers
    Sync = 2,
    /// Recovering missing slices
    Recover = 3,
    /// Locked for handoff to another node
    Locked = 4,
}

impl Default for SpoolStatus {
    fn default() -> Self {
        Self::None
    }
}

/// Type of slice (primary or recovery)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum SliceType {
    /// Primary data slice
    Primary = 0,
    /// Recovery/parity slice
    Recovery = 1,
}

impl Default for SliceType {
    fn default() -> Self {
        Self::Primary
    }
}

/// Encoding type for blobs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
#[repr(u8)]
pub enum EncodingType {
    /// Unknown encoding
    Unknown = 0,
    /// Basic encoding (single layer)
    Basic = 1,
    /// Striped encoding (interleaved)
    Striped = 2,
    /// Rotated encoding (row-column)
    Rotated = 3,
}

impl Default for EncodingType {
    fn default() -> Self {
        Self::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_status_default() {
        assert_eq!(NodeStatus::default(), NodeStatus::Standby);
    }

    #[test]
    fn test_spool_status_default() {
        assert_eq!(SpoolStatus::default(), SpoolStatus::None);
    }

    #[test]
    fn test_slice_type_default() {
        assert_eq!(SliceType::default(), SliceType::Primary);
    }

    #[test]
    fn test_encoding_type_default() {
        assert_eq!(EncodingType::default(), EncodingType::Unknown);
    }

    #[test]
    fn test_repr_values() {
        assert_eq!(NodeStatus::Standby as u8, 0);
        assert_eq!(NodeStatus::Active as u8, 1);
        assert_eq!(NodeStatus::Recovering as u8, 2);

        assert_eq!(SpoolStatus::None as u8, 0);
        assert_eq!(SpoolStatus::Active as u8, 1);
        assert_eq!(SpoolStatus::Sync as u8, 2);
        assert_eq!(SpoolStatus::Recover as u8, 3);
        assert_eq!(SpoolStatus::Locked as u8, 4);

        assert_eq!(SliceType::Primary as u8, 0);
        assert_eq!(SliceType::Recovery as u8, 1);

        assert_eq!(EncodingType::Unknown as u8, 0);
        assert_eq!(EncodingType::Basic as u8, 1);
        assert_eq!(EncodingType::Striped as u8, 2);
        assert_eq!(EncodingType::Rotated as u8, 3);
    }
}
