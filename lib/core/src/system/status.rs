//! Spool and node lifecycle status types.

use serde::{Deserialize, Serialize};

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::types::EpochNumber;
use tape_crypto::Address;

/// Node status in the network
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wincode", derive(SchemaRead, SchemaWrite))]
#[derive(Default)]
pub enum NodeStatus {
    /// Node is registered but not in committee
    #[default]
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


/// One stored spool this node owns now or owned in the epochs it keeps slices for
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpoolState {
    /// Filling hasn't read the spool's whole track list yet
    pub is_filling: bool,
    /// The last track filling read, zero before its first page
    pub fill_position: Address,
    /// The epoch this node lost the spool, zero while it owns it
    pub locked_at: EpochNumber,
}

/// A spool row's bytes on disk
pub type PackedSpoolState = [u8; SpoolState::SIZE];

impl SpoolState {
    /// Bytes a spool row takes on disk
    pub const SIZE: usize = 41;

    /// The row of a spool this node just gained, with filling at the start
    pub fn gained() -> Self {
        Self {
            is_filling: true,
            fill_position: Address::default(),
            locked_at: EpochNumber(0),
        }
    }

    /// Once the row is locked the spool is locked_to_move: no new uploads
    pub fn is_locked(&self) -> bool {
        self.locked_at != EpochNumber(0)
    }

    /// Write the row in its fixed layout: filling, fill_position, locked_at
    pub fn pack(&self) -> PackedSpoolState {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0] = u8::from(self.is_filling);
        bytes[1..33].copy_from_slice(&self.fill_position.to_bytes());
        bytes[33..].copy_from_slice(&self.locked_at.0.to_le_bytes());
        bytes
    }

    /// Read a row back from its fixed layout
    pub fn unpack(bytes: PackedSpoolState) -> Self {
        let mut position = [0u8; 32];
        position.copy_from_slice(&bytes[1..33]);
        let mut locked = [0u8; 8];
        locked.copy_from_slice(&bytes[33..]);
        Self {
            is_filling: bytes[0] != 0,
            fill_position: Address::new(position),
            locked_at: EpochNumber(u64::from_le_bytes(locked)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_status_default() {
        assert_eq!(NodeStatus::default(), NodeStatus::Standby);
    }

    // a spool row comes back from its 41 bytes as it went in
    #[test]
    fn spool_state_roundtrip() {
        let rows = [
            SpoolState::gained(),
            SpoolState {
                is_filling: false,
                fill_position: Address::new([7u8; 32]),
                locked_at: EpochNumber(42),
            },
        ];

        for row in rows {
            assert_eq!(SpoolState::unpack(row.pack()), row);
        }
        assert!(!SpoolState::gained().is_locked());
        assert!(rows[1].is_locked());
    }

    #[cfg(feature = "wincode")]
    #[test]
    fn node_status_roundtrip() {
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
}
