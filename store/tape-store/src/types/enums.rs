//! Enum types for tape-store

use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaRead, SchemaWrite};

use tape_core::types::{EpochNumber, SlotNumber};
use tape_crypto::address::Address;

/// System-owned object categories.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub enum SystemObjectKind {
    Snapshot { epoch: EpochNumber },
    History,
    Blacklist,
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
        track_address: Address,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: SlotNumber,
    },

    /// System/protocol track. These are live protocol metadata and are not
    /// counted as user objects for assignment sizing.
    System {
        kind: SystemObjectKind,
        track_address: Address,
        registered_epoch: EpochNumber,
        certified_epoch: Option<EpochNumber>,
        slot: SlotNumber,
    },
}

impl ObjectInfo {
    pub fn is_certified(&self) -> bool {
        matches!(
            self,
            ObjectInfo::Valid {
                certified_epoch: Some(_),
                ..
            } | ObjectInfo::System {
                    kind: SystemObjectKind::Snapshot { .. },
                    ..
                }
                | ObjectInfo::System {
                    certified_epoch: Some(_),
                    ..
                }
        )
    }

    pub fn is_live(&self) -> bool {
        matches!(
            self,
            ObjectInfo::Valid { .. } | ObjectInfo::System { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use tape_crypto::address::Address;

    use super::*;

    #[test]
    fn object_info_roundtrip() {
        use tape_core::types::SlotNumber;

        let infos = vec![
            ObjectInfo::Blacklisted,
            ObjectInfo::Invalid {
                epoch: EpochNumber(10),
                slot: SlotNumber(100),
            },
            ObjectInfo::Valid {
                track_address: Address::new([1u8; 32]),
                registered_epoch: EpochNumber(5),
                certified_epoch: Some(EpochNumber(6)),
                slot: SlotNumber(50),
            },
            ObjectInfo::Valid {
                track_address: Address::new([2u8; 32]),
                registered_epoch: EpochNumber(7),
                certified_epoch: None,
                slot: SlotNumber(70),
            },
            ObjectInfo::System {
                kind: SystemObjectKind::Snapshot {
                    epoch: EpochNumber(8),
                },
                track_address: Address::new([3u8; 32]),
                registered_epoch: EpochNumber(8),
                certified_epoch: None,
                slot: SlotNumber(80),
            },
            ObjectInfo::System {
                kind: SystemObjectKind::History,
                track_address: Address::new([4u8; 32]),
                registered_epoch: EpochNumber(8),
                certified_epoch: Some(EpochNumber(8)),
                slot: SlotNumber(80),
            },
        ];

        for info in infos {
            let bytes = wincode::serialize(&info).unwrap();
            let decoded: ObjectInfo = wincode::deserialize(&bytes).unwrap();
            assert_eq!(info, decoded);
        }
    }
}
