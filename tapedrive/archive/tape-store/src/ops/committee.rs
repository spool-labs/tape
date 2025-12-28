//! Committee management operations

use crate::types::EpochNumber;
use serde::{Deserialize, Serialize};
use tape_core::system::{Committee, CommitteeMember};
use wincode_derive::{SchemaRead, SchemaWrite};

/// Storage representation of committee data for an epoch
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct CommitteeData {
    pub epoch: EpochNumber,
    pub members: Vec<CommitteeMember>,
    pub total_stake: u64,
}

impl CommitteeData {
    /// Create CommitteeData from a Committee and epoch
    pub fn from_committee<const N: usize>(committee: &Committee<N>, epoch: EpochNumber) -> Self {
        let members: Vec<CommitteeMember> = committee.iter().cloned().collect();
        let total_stake = committee.total_stake().0;
        Self {
            epoch,
            members,
            total_stake,
        }
    }

    /// Convert back to a Committee with the specified capacity.
    /// Members beyond capacity N will be truncated.
    pub fn to_committee<const N: usize>(&self) -> Committee<N> {
        Committee::from_members(&self.members)
    }
}
