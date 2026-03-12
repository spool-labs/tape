//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::types::{EpochNumber, NodeId, SlotNumber};
use crate::types::Pubkey;

/// Node status in the network
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum NodeStatus {
    /// Node is registered but not in committee
    Standby,
    /// Node is active in the committee
    Active,
    /// Node needs to recover metadata before joining
    RecoverMetadata,
    /// Node is catching up via block processing (lag >= 2 epochs)
    RecoveryReplay,
    /// Node is actively recovering data for a specific epoch
    RecoveryInProgress { epoch: EpochNumber },
    /// Node is catching up with incomplete history
    PartialReplay {
        first_complete_epoch: EpochNumber,
        epoch_at_start: EpochNumber,
    },
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Standby
    }
}

/// Pure lifecycle status for a spool — no associated data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SpoolStatus {
    Active,
    Sync,
    Scan,
    Recover,
    LockedToMove,
}

/// Full spool state: status + epoch + optional handoff context.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SpoolState {
    pub status: SpoolStatus,
    pub epoch: EpochNumber,
    pub prev_owner: Option<NodeId>,
    pub prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
}

impl SpoolState {
    pub fn new(status: SpoolStatus, epoch: EpochNumber) -> Self {
        Self { status, epoch, prev_owner: None, prev_helpers: [None; SPOOL_GROUP_SIZE] }
    }

    pub fn is_locked(&self) -> bool { self.status == SpoolStatus::LockedToMove }
    pub fn is_active(&self) -> bool { self.status == SpoolStatus::Active }
    pub fn is_syncing(&self) -> bool { self.status == SpoolStatus::Sync }
    pub fn is_scanning(&self) -> bool { self.status == SpoolStatus::Scan }
    pub fn is_recovering(&self) -> bool { self.status == SpoolStatus::Recover }
}

/// Information about a tracked object
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum ObjectInfo {
    /// Object has been blacklisted
    Blacklisted,
    /// Object is invalid
    Invalid {
        epoch: EpochNumber,
        slot: SlotNumber,
    },
    /// Object is valid
    Valid {
        is_stored: bool,
        track_address: Pubkey,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: SlotNumber,
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
    fn spool_state_roundtrip() {
        let states = vec![
            SpoolState::new(SpoolStatus::Active, EpochNumber(0)),
            SpoolState::new(SpoolStatus::LockedToMove, EpochNumber(42)),
            SpoolState {
                status: SpoolStatus::Sync,
                epoch: EpochNumber(5),
                prev_owner: Some(NodeId(7)),
                prev_helpers: [Some(NodeId(7)); SPOOL_GROUP_SIZE],
            },
            SpoolState::new(SpoolStatus::Scan, EpochNumber(6)),
            SpoolState {
                status: SpoolStatus::Recover,
                epoch: EpochNumber(7),
                prev_owner: Some(NodeId(3)),
                prev_helpers: [None; SPOOL_GROUP_SIZE],
            },
        ];

        for state in states {
            let bytes = wincode::serialize(&state).unwrap();
            let decoded: SpoolState = wincode::deserialize(&bytes).unwrap();
            assert_eq!(state, decoded);
        }
    }

    #[test]
    fn test_node_status_roundtrip() {
        let statuses = vec![
            NodeStatus::Standby,
            NodeStatus::Active,
            NodeStatus::RecoverMetadata,
            NodeStatus::RecoveryReplay,
            NodeStatus::RecoveryInProgress {
                epoch: EpochNumber(42),
            },
            NodeStatus::PartialReplay {
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
