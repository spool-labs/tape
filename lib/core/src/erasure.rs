//! Erasure coding constants and parameters.
//!
//! These constants define the network-level erasure coding parameters.
//! SPOOL_GROUP_SIZE is the number of slices per spool group (fixed network constant).
//! A blob's n may be ≤ SPOOL_GROUP_SIZE depending on its encoding profile.
//! SPOOL_COUNT is the total number of spools in the network.
//!
//! Encoding parameters (k, m) are now per-profile via EncodingProfile.
//! See `encoding::RSParams` and `encoding::ClayParams` for profile-specific parameters.

/// Number of slices per spool group (fixed network constant).
/// Individual encoding profiles may use n ≤ SPOOL_GROUP_SIZE.
pub const SPOOL_GROUP_SIZE: usize = 20;

/// Merkle tree height for blob commitment trees.
/// Derived from SPOOL_GROUP_SIZE: 2^5 = 32 >= 20 leaves.
pub const COMMITMENT_TREE_HEIGHT: usize = 5;

/// Number of spool groups in the network.
pub const SPOOL_GROUP_COUNT: usize = 50;

/// Total spools in the network.
pub const SPOOL_COUNT: usize = SPOOL_GROUP_COUNT * SPOOL_GROUP_SIZE;

/// Maximum committee members (storage nodes).
pub const MEMBER_COUNT: usize = 128;

/// Maximum blob size (1 GiB).
pub const MAX_BLOB_SIZE: usize = 1 << 30;

/// Maximum slice size (~143 MiB).
/// With k=7 data slices, each shard is approximately blob_size / 7.
pub const MAX_SLICE_SIZE: usize = MAX_BLOB_SIZE / 7;

use crate::spooler::{SpoolGroup, SpoolIndex};

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
pub fn slice_for_spool(group: SpoolGroup, spool: SpoolIndex) -> Option<usize> {
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
        assert_eq!(SPOOL_GROUP_SIZE, 20);
    }

    #[test]
    fn test_spool_count() {
        assert_eq!(SPOOL_COUNT, 1000);
        assert_eq!(SPOOL_GROUP_COUNT, 50);
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
        assert_eq!(group_for_spool(0), SpoolGroup(0));
        assert_eq!(group_for_spool(19), SpoolGroup(0));
        assert_eq!(group_for_spool(20), SpoolGroup(1));
        assert_eq!(group_for_spool(999), SpoolGroup(49));
    }

    #[test]
    fn test_group_start() {
        assert_eq!(group_start(SpoolGroup(0)), 0);
        assert_eq!(group_start(SpoolGroup(1)), 20);
        assert_eq!(group_start(SpoolGroup(49)), 980);
    }

    #[test]
    fn test_spool_for_slice() {
        assert_eq!(spool_for_slice(SpoolGroup(0), 0), 0);
        assert_eq!(spool_for_slice(SpoolGroup(0), 19), 19);
        assert_eq!(spool_for_slice(SpoolGroup(1), 0), 20);
        assert_eq!(spool_for_slice(SpoolGroup(49), 19), 999);
    }

    #[test]
    fn test_spool_in_group() {
        assert!(spool_in_group(0, SpoolGroup(0)));
        assert!(spool_in_group(19, SpoolGroup(0)));
        assert!(!spool_in_group(20, SpoolGroup(0)));
        assert!(spool_in_group(20, SpoolGroup(1)));
        assert!(spool_in_group(999, SpoolGroup(49)));
    }

    #[test]
    fn test_slice_for_spool() {
        assert_eq!(slice_for_spool(SpoolGroup(0), 0), Some(0));
        assert_eq!(slice_for_spool(SpoolGroup(0), 19), Some(19));
        assert_eq!(slice_for_spool(SpoolGroup(1), 20), Some(0));
        assert_eq!(slice_for_spool(SpoolGroup(1), 39), Some(19));
        assert_eq!(slice_for_spool(SpoolGroup(0), 20), None);
    }
}
