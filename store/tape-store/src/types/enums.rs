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

/// Spool lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SpoolState {
    /// Fully synced and serving requests.
    Active {
        epoch: EpochNumber,
    },

    /// Syncing data from the prior owner into the newly assigned spool.
    Sync {
        epoch: EpochNumber,
        prev_owner: Option<NodeId>,
        prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
    },

    /// Recovering missing slices after the initial sync pass.
    Recover {
        epoch: EpochNumber,
        prev_owner: Option<NodeId>,
        prev_helpers: [Option<NodeId>; SPOOL_GROUP_SIZE],
    },

    /// Locked on the former owner while the new owner completes handoff.
    LockedToMove {
        epoch: EpochNumber,
    },
}

impl SpoolState {
    pub fn epoch(&self) -> EpochNumber {
        match self {
            Self::Active { epoch }
            | Self::Sync { epoch, .. }
            | Self::Recover { epoch, .. }
            | Self::LockedToMove { epoch } => *epoch,
        }
    }

    pub fn is_locked(&self) -> bool {
        matches!(self, Self::LockedToMove { .. })
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }

    pub fn is_syncing(&self) -> bool {
        matches!(self, Self::Sync { .. })
    }

    pub fn is_recovering(&self) -> bool {
        matches!(self, Self::Recover { .. })
    }

    pub fn prev_owner(&self) -> Option<NodeId> {
        match self {
            Self::Sync { prev_owner, .. } | Self::Recover { prev_owner, .. } => *prev_owner,
            _ => None,
        }
    }

    pub fn prev_helpers(&self) -> Option<&[Option<NodeId>; SPOOL_GROUP_SIZE]> {
        match self {
            Self::Sync { prev_helpers, .. } | Self::Recover { prev_helpers, .. } => Some(prev_helpers),
            _ => None,
        }
    }
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
            SpoolState::Active {
                epoch: EpochNumber(0),
            },
            SpoolState::LockedToMove {
                epoch: EpochNumber(42),
            },
            SpoolState::Sync {
                epoch: EpochNumber(5),
                prev_owner: Some(NodeId(7)),
                prev_helpers: [Some(NodeId(7)); SPOOL_GROUP_SIZE],
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
