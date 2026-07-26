//! Erasure coding constants and parameters.

/// Number of slices per group (fixed network constant).
/// Individual encoding profiles may use n ≤ GROUP_SIZE.
pub const GROUP_SIZE: usize = 20;

/// Merkle tree height for slice commitments.
/// Derived from GROUP_SIZE: 2^5 = 32 >= 20 leaves.
pub const SLICE_TREE_HEIGHT: usize = 5;

/// Bytes covered by one sample leaf beneath a slice root.
/// Kept small because a challenge response carries one leaf and its path.
pub const SUB_LEAF_BYTES: usize = 1024;

/// Merkle tree height for the sub-leaf tree under a single slice.
/// Sized for the worst case any encoding can produce, which is Reed-Solomon at
/// k=1: no erasure at all, so one slice holds the whole track. A 64 MiB track is
/// exactly 65,536 sample leaves, so 2^16 covers it with nothing to spare.
/// Clay at its default k=7 is far under this, at 9,497 leaves.
/// Both counts are measured by lib/slicer/tests/capacity_probe.rs.
pub const SUB_TREE_HEIGHT: usize = 16;

use tape_crypto::Hash;
use tape_crypto::merkle::{MerkleTree, hash_leaf};

use crate::types::{GroupIndex, SpoolIndex};

/// Number of sample leaves a slice of this length is split into.
#[inline]
pub fn sub_leaf_count(slice_len: usize) -> usize {
    slice_len.div_ceil(SUB_LEAF_BYTES)
}

/// Hash every sample leaf of one coded slice, in order.
pub fn sub_leaf_hashes(slice: &[u8]) -> Vec<Hash> {
    slice.chunks(SUB_LEAF_BYTES).map(hash_leaf).collect()
}

/// Merkle root over the sample leaves of one coded slice.
/// None when the slice needs more leaves than the tree can hold.
pub fn slice_root(slice: &[u8]) -> Option<Hash> {
    // Reject on length first, so an oversized slice costs nothing to refuse.
    if sub_leaf_count(slice.len()) > 1 << SUB_TREE_HEIGHT {
        return None;
    }

    let mut tree = MerkleTree::<SUB_TREE_HEIGHT>::new();
    for leaf in slice.chunks(SUB_LEAF_BYTES) {
        tree.add_leaf(leaf).ok()?;
    }
    Some(tree.root())
}

/// Get the group index for a given spool.
#[inline]
pub fn group_for_spool(spool: SpoolIndex) -> GroupIndex {
    GroupIndex::containing(spool)
}

/// Get the first spool index in a group.
#[inline]
pub fn group_start(group: GroupIndex) -> SpoolIndex {
    group.base_spool()
}

/// Get the global spool index for a slice within a group.
#[inline]
pub fn spool_for_slice(group: GroupIndex, slice_in_group: usize) -> SpoolIndex {
    group.spool_at(slice_in_group)
}

/// Get the position within a group for a spool, if the spool belongs to the group.
#[inline]
pub fn slice_for_spool(group: GroupIndex, spool: SpoolIndex) -> Option<usize> {
    group.position_of(spool)
}

/// Check if a spool belongs to a given group.
#[inline]
pub fn spool_in_group(spool: SpoolIndex, group: GroupIndex) -> bool {
    group.contains(spool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spool_group_size() {
        assert_eq!(GROUP_SIZE, 20);
    }

    #[test]
    fn test_sub_leaf_count_rounds_up() {
        assert_eq!(sub_leaf_count(0), 0);
        assert_eq!(sub_leaf_count(1), 1);
        assert_eq!(sub_leaf_count(SUB_LEAF_BYTES), 1);
        assert_eq!(sub_leaf_count(SUB_LEAF_BYTES + 1), 2);
    }

    #[test]
    fn test_slice_root_matches_leaf_hashes() {
        let slice: Vec<u8> = (0..SUB_LEAF_BYTES * 2 + 7).map(|b| b as u8).collect();
        let hashes = sub_leaf_hashes(&slice);

        assert_eq!(hashes.len(), sub_leaf_count(slice.len()));
        assert_eq!(
            slice_root(&slice),
            Some(tape_crypto::merkle::root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&hashes))
        );
    }

    #[test]
    fn test_slice_root_bounded_by_tree_capacity() {
        let capacity = SUB_LEAF_BYTES << SUB_TREE_HEIGHT;

        assert!(slice_root(&vec![0u8; capacity]).is_some());
        assert!(slice_root(&vec![0u8; capacity + 1]).is_none());
    }

    /// The largest legal track must fit, which is what fixes the tree height.
    /// Both counts are measured by lib/slicer/tests/capacity_probe.rs. Deriving
    /// them here would be wrong: Clay pads a slice about 1.4% above track size
    /// over k, while bare Reed-Solomon does not.
    #[test]
    fn test_max_track_slice_fits() {
        const MAX_TRACK_BYTES: usize = 64 * 1024 * 1024;
        const RS_K1_SUB_LEAVES: usize = 65_536;
        const CLAY_K7_SUB_LEAVES: usize = 9_497;

        // Replication is the worst case: one slice carries the whole track.
        assert_eq!(RS_K1_SUB_LEAVES, MAX_TRACK_BYTES / SUB_LEAF_BYTES);
        assert!(RS_K1_SUB_LEAVES <= 1 << SUB_TREE_HEIGHT);
        assert!(RS_K1_SUB_LEAVES > 1 << (SUB_TREE_HEIGHT - 1));

        assert!(CLAY_K7_SUB_LEAVES <= 1 << SUB_TREE_HEIGHT);
    }

    #[test]
    fn test_group_for_spool() {
        assert_eq!(group_for_spool(SpoolIndex(0)), GroupIndex(0));
        assert_eq!(group_for_spool(SpoolIndex(19)), GroupIndex(0));
        assert_eq!(group_for_spool(SpoolIndex(20)), GroupIndex(1));
        assert_eq!(group_for_spool(SpoolIndex(999)), GroupIndex(49));
    }

    #[test]
    fn test_group_start() {
        assert_eq!(group_start(GroupIndex(0)), SpoolIndex(0));
        assert_eq!(group_start(GroupIndex(1)), SpoolIndex(20));
        assert_eq!(group_start(GroupIndex(49)), SpoolIndex(980));
    }

    #[test]
    fn test_spool_for_slice() {
        assert_eq!(spool_for_slice(GroupIndex(0), 0), SpoolIndex(0));
        assert_eq!(spool_for_slice(GroupIndex(0), 19), SpoolIndex(19));
        assert_eq!(spool_for_slice(GroupIndex(1), 0), SpoolIndex(20));
        assert_eq!(spool_for_slice(GroupIndex(49), 19), SpoolIndex(999));
    }

    #[test]
    fn test_spool_in_group() {
        assert!(spool_in_group(SpoolIndex(0), GroupIndex(0)));
        assert!(spool_in_group(SpoolIndex(19), GroupIndex(0)));
        assert!(!spool_in_group(SpoolIndex(20), GroupIndex(0)));
        assert!(spool_in_group(SpoolIndex(20), GroupIndex(1)));
        assert!(spool_in_group(SpoolIndex(999), GroupIndex(49)));
    }

    #[test]
    fn test_slice_for_spool() {
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(0)), Some(0));
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(19)), Some(19));
        assert_eq!(slice_for_spool(GroupIndex(1), SpoolIndex(20)), Some(0));
        assert_eq!(slice_for_spool(GroupIndex(1), SpoolIndex(39)), Some(19));
        assert_eq!(slice_for_spool(GroupIndex(0), SpoolIndex(20)), None);
    }
}
