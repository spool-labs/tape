//! Node lifecycle status types.

use serde::{Deserialize, Serialize};

#[cfg(feature = "wincode")]
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::types::EpochNumber;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_status_default() {
        assert_eq!(NodeStatus::default(), NodeStatus::Standby);
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
