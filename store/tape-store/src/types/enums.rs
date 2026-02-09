//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use tape_core::types::EpochNumber;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Node status in the network
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum NodeStatus {
    /// Node is registered but not in committee
    Standby,
    /// Node is active in the committee
    Active,
    /// Node needs to recover metadata before joining
    RecoverMetadata,
    /// Node is catching up during recovery
    RecoveryCatchUp,
    /// Node is actively recovering data for a specific epoch
    RecoveryInProgress { epoch: EpochNumber },
    /// Node is catching up with incomplete history
    RecoveryCatchUpWithIncompleteHistory {
        first_complete_epoch: EpochNumber,
        epoch_at_start: EpochNumber,
    },
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
    ActiveSync = 2,
    /// Recovering missing slices
    ActiveRecover = 3,
    /// Locked for handoff to another node
    LockedToMove = 4,
}

impl Default for SpoolStatus {
    fn default() -> Self {
        Self::None
    }
}

/// How a track's slices are allocated across spools
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SpoolAllocation {
    /// All slices go to a single spool
    SpoolSingle(u16),
    /// Slices are distributed across a spool group (0..SPOOL_GROUP_COUNT-1)
    SpoolGroup(u64),
}

/// Information about a tracked object
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ObjectInfo {
    /// Object has been blacklisted
    Blacklisted,
    /// Object is invalid
    Invalid {
        epoch: EpochNumber,
        slot: tape_core::types::SlotNumber,
    },
    /// Object is valid
    Valid {
        is_stored: bool,
        track_address: crate::types::Pubkey,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: tape_core::types::SlotNumber,
    },
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
    fn test_repr_values() {
        assert_eq!(SpoolStatus::None as u8, 0);
        assert_eq!(SpoolStatus::Active as u8, 1);
        assert_eq!(SpoolStatus::ActiveSync as u8, 2);
        assert_eq!(SpoolStatus::ActiveRecover as u8, 3);
        assert_eq!(SpoolStatus::LockedToMove as u8, 4);
    }

    #[test]
    fn test_node_status_roundtrip() {
        let statuses = vec![
            NodeStatus::Standby,
            NodeStatus::Active,
            NodeStatus::RecoverMetadata,
            NodeStatus::RecoveryCatchUp,
            NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(42),
            },
            NodeStatus::RecoveryCatchUpWithIncompleteHistory {
                first_complete_epoch: EpochNumber(10),
                epoch_at_start: EpochNumber(5),
            },
        ];

        for status in statuses {
            let bytes = wincode::serialize(&status).unwrap();
            let decoded: NodeStatus = wincode::deserialize(&bytes).unwrap();
            assert_eq!(status, decoded);
        }
    }

    #[test]
    fn test_spool_allocation_roundtrip() {
        let allocs = vec![SpoolAllocation::SpoolSingle(42), SpoolAllocation::SpoolGroup(3u64)];

        for alloc in allocs {
            let bytes = wincode::serialize(&alloc).unwrap();
            let decoded: SpoolAllocation = wincode::deserialize(&bytes).unwrap();
            assert_eq!(alloc, decoded);
        }
    }

    #[test]
    fn test_object_info_roundtrip() {
        use crate::types::Pubkey;
        use tape_core::types::SlotNumber;

        let infos = vec![
            ObjectInfo::Blacklisted,
            ObjectInfo::Invalid {
                epoch: EpochNumber(10),
                slot: SlotNumber(100),
            },
            ObjectInfo::Valid {
                is_stored: true,
                track_address: Pubkey::new([1u8; 32]),
                registered_epoch: EpochNumber(5),
                certified_epoch: Some(EpochNumber(6)),
                slot: SlotNumber(50),
            },
            ObjectInfo::Valid {
                is_stored: false,
                track_address: Pubkey::new([2u8; 32]),
                registered_epoch: EpochNumber(7),
                certified_epoch: None,
                slot: SlotNumber(70),
            },
        ];

        for info in infos {
            let bytes = wincode::serialize(&info).unwrap();
            let decoded: ObjectInfo = wincode::deserialize(&bytes).unwrap();
            assert_eq!(info, decoded);
        }
    }
}
