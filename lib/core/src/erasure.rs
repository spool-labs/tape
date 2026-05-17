//! Erasure coding constants and parameters.

/// Number of slices per spool group (fixed network constant).
/// Individual encoding profiles may use n ≤ GROUP_SIZE.
pub const GROUP_SIZE: usize = 20;

/// Merkle tree height for slice commitments.
/// Derived from GROUP_SIZE: 2^5 = 32 >= 20 leaves.
pub const SLICE_TREE_HEIGHT: usize = 5;

/// Maximum blob size (1 GiB).
pub const MAX_BLOB_SIZE: usize = 1 << 30;

/// Maximum slice size (~143 MiB).
/// With k=7 data slices, each shard is approximately blob_size / 7.
pub const MAX_SLICE_SIZE: usize = MAX_BLOB_SIZE / 7;

use crate::types::{SpoolGroup, SpoolIndex};

/// Get the spool group index (0..SPOOL_GROUP_COUNT-1) for a given spool.
#[inline]
pub fn group_for_spool(spool: SpoolIndex) -> SpoolGroup {
    SpoolGroup::of(spool)
}

/// Get the first spool index in a group.
#[inline]
pub fn group_start(group: SpoolGroup) -> SpoolIndex {
    group.base()
}

/// Get the global spool index for a slice within a group.
#[inline]
pub fn spool_for_slice(group: SpoolGroup, slice_in_group: usize) -> SpoolIndex {
    group.spool_at(slice_in_group)
}

/// Get the slice index within a group for a spool, if the spool belongs to the group.
#[inline]
pub fn slice_for_spool(group: SpoolGroup, spool: SpoolIndex) -> Option<SpoolIndex> {
    group.slice_of(spool)
}

/// Check if a spool belongs to a given group.
#[inline]
pub fn spool_in_group(spool: SpoolIndex, group: SpoolGroup) -> bool {
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
    fn test_max_blob_size() {
        assert_eq!(MAX_BLOB_SIZE, 1 << 30);
    }

    #[test]
    fn test_max_slice_size() {
        // With default k=7, max slice is ~143 MiB
        assert_eq!(MAX_SLICE_SIZE, MAX_BLOB_SIZE / 7);
    }

    #[test]
    fn test_group_for_spool() {
        assert_eq!(group_for_spool(SpoolIndex(0)), SpoolGroup(0));
        assert_eq!(group_for_spool(SpoolIndex(19)), SpoolGroup(0));
        assert_eq!(group_for_spool(SpoolIndex(20)), SpoolGroup(1));
        assert_eq!(group_for_spool(SpoolIndex(999)), SpoolGroup(49));
    }

    #[test]
    fn test_group_start() {
        assert_eq!(group_start(SpoolGroup(0)), SpoolIndex(0));
        assert_eq!(group_start(SpoolGroup(1)), SpoolIndex(20));
        assert_eq!(group_start(SpoolGroup(49)), SpoolIndex(980));
    }

    #[test]
    fn test_spool_for_slice() {
        assert_eq!(spool_for_slice(SpoolGroup(0), 0), SpoolIndex(0));
        assert_eq!(spool_for_slice(SpoolGroup(0), 19), SpoolIndex(19));
        assert_eq!(spool_for_slice(SpoolGroup(1), 0), SpoolIndex(20));
        assert_eq!(spool_for_slice(SpoolGroup(49), 19), SpoolIndex(999));
    }

    #[test]
    fn test_spool_in_group() {
        assert!(spool_in_group(SpoolIndex(0), SpoolGroup(0)));
        assert!(spool_in_group(SpoolIndex(19), SpoolGroup(0)));
        assert!(!spool_in_group(SpoolIndex(20), SpoolGroup(0)));
        assert!(spool_in_group(SpoolIndex(20), SpoolGroup(1)));
        assert!(spool_in_group(SpoolIndex(999), SpoolGroup(49)));
    }

    #[test]
    fn test_slice_for_spool() {
        assert_eq!(slice_for_spool(SpoolGroup(0), SpoolIndex(0)), Some(SpoolIndex(0)));
        assert_eq!(slice_for_spool(SpoolGroup(0), SpoolIndex(19)), Some(SpoolIndex(19)));
        assert_eq!(slice_for_spool(SpoolGroup(1), SpoolIndex(20)), Some(SpoolIndex(0)));
        assert_eq!(slice_for_spool(SpoolGroup(1), SpoolIndex(39)), Some(SpoolIndex(19)));
        assert_eq!(slice_for_spool(SpoolGroup(0), SpoolIndex(20)), None);
    }
}
