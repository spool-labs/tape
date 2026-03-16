//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_core::types::{EpochNumber, SlotNumber};
use crate::types::Pubkey;

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
    fn object_info_roundtrip() {
        use crate::types::Pubkey;
        use tape_core::types::SlotNumber;

        let infos = vec![
            ObjectInfo::Blacklisted,
            ObjectInfo::Invalid {
                epoch: EpochNumber(10),
                slot: SlotNumber(100),
            },
            ObjectInfo::Valid {
                track_address: Pubkey::new([1u8; 32]),
                registered_epoch: EpochNumber(5),
                certified_epoch: Some(EpochNumber(6)),
                slot: SlotNumber(50),
            },
            ObjectInfo::Valid {
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
