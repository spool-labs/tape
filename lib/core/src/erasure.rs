//! Erasure coding constants, and the commitment leaves derived from a slice.

use tape_crypto::Hash;
use tape_crypto::merkle::{hash_leaf, root_from_leaf_hashes};

use crate::types::{GroupIndex, SpoolIndex};

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
/// 2^16 sample leaves of SUB_LEAF_BYTES is 64 MiB, which is where the max
/// track size came from. Reed-Solomon at k=1 puts a whole track in one slice,
/// so that is the case the height has to cover.
pub const SUB_TREE_HEIGHT: usize = 16;

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

    Some(root_from_leaf_hashes::<SUB_TREE_HEIGHT>(&sub_leaf_hashes(slice)))
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

    /// The tree must hold a slice that is a whole 64 MiB track, which is what
    /// Reed-Solomon at k=1 produces. This is exact, so a larger track size or
    /// any padding on the RS path needs the height raised with it.
    #[test]
    fn test_capacity_is_one_whole_track() {
        const MAX_TRACK_BYTES: usize = 64 * 1024 * 1024;

        assert_eq!(SUB_LEAF_BYTES << SUB_TREE_HEIGHT, MAX_TRACK_BYTES);
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
