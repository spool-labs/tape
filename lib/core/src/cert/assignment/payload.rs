use bytemuck::{Pod, Zeroable, bytes_of};
use tape_crypto::Hash;
use tape_crypto::merkle::{hash_leaf, verify_proof};

use crate::erasure::GROUP_SIZE;
use crate::spooler::GroupIndex;
use crate::types::StorageUnits;

use super::ASSIGNMENT_TREE_HEIGHT;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct AssignmentGroupPayload {
    /// Group index within the target epoch.
    pub group: GroupIndex,

    /// Indices into the target epoch committee.
    pub member_indices: [u64; GROUP_SIZE],

    /// Uniform size assigned to every spool in this group.
    pub size: StorageUnits,

    /// Blacklisted size for each assigned spool owner in this group.
    pub blacklisted: [StorageUnits; GROUP_SIZE],
}

impl AssignmentGroupPayload {
    pub const fn new(
        group: GroupIndex,
        member_indices: [u64; GROUP_SIZE],
        size: StorageUnits,
        blacklisted: [StorageUnits; GROUP_SIZE],
    ) -> Self {
        Self {
            group,
            member_indices,
            size,
            blacklisted,
        }
    }

    pub const fn group(&self) -> GroupIndex {
        self.group
    }

    pub fn hash(&self) -> Hash {
        hash_assignment_group_payload(self)
    }
}

/// Leaf hash for an assignment group payload.
pub fn hash_assignment_group_payload(payload: &AssignmentGroupPayload) -> Hash {
    hash_leaf(bytes_of(payload))
}

/// Verify a compact assignment payload against the canonical assignment hash.
pub fn verify_assignment_group_payload(
    root: &Hash,
    payload: &AssignmentGroupPayload,
    proof: &[Hash; ASSIGNMENT_TREE_HEIGHT],
) -> bool {
    verify_proof(
        bytes_of(payload),
        root,
        proof,
        payload.group.0,
        ASSIGNMENT_TREE_HEIGHT,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_hash_deterministic() {
        let payload = AssignmentGroupPayload::new(
            GroupIndex(3),
            core::array::from_fn(|i| i as u64),
            StorageUnits::mb(100),
            [StorageUnits::zero(); GROUP_SIZE],
        );

        assert_eq!(payload.hash(), payload.hash());
    }

    #[test]
    fn payload_hash_sensitive_to_input() {
        let payload1 = AssignmentGroupPayload::new(
            GroupIndex(3),
            core::array::from_fn(|i| i as u64),
            StorageUnits::mb(100),
            [StorageUnits::zero(); GROUP_SIZE],
        );
        let mut payload2 = payload1;
        payload2.member_indices[0] = 99;

        assert_ne!(payload1.hash(), payload2.hash());

        let mut payload3 = payload1;
        payload3.blacklisted[0] = StorageUnits::mb(1);

        assert_ne!(payload1.hash(), payload3.hash());
    }
}
