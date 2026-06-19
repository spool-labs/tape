//! Erasure coding constants and parameters.

/// Number of slices per group (fixed network constant).
/// Individual encoding profiles may use n ≤ GROUP_SIZE.
pub const GROUP_SIZE: usize = 20;

/// Merkle tree height for slice commitments.
/// Derived from GROUP_SIZE: 2^5 = 32 >= 20 leaves.
pub const SLICE_TREE_HEIGHT: usize = 5;

use crate::types::{GroupIndex, SpoolIndex};

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
