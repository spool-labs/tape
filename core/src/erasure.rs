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
    (spool as usize / SPOOL_GROUP_SIZE) as SpoolGroup
}

/// Get the first spool index in a group.
#[inline]
pub fn group_start(group: SpoolGroup) -> SpoolIndex {
    (group as usize * SPOOL_GROUP_SIZE) as SpoolIndex
}

/// Get the global spool index for a slice within a group.
#[inline]
pub fn spool_for_slice(group: SpoolGroup, slice_in_group: usize) -> SpoolIndex {
    (group as usize * SPOOL_GROUP_SIZE + slice_in_group) as SpoolIndex
}

/// Check if a spool belongs to a given group.
#[inline]
pub fn spool_in_group(spool: SpoolIndex, group: SpoolGroup) -> bool {
    group_for_spool(spool) == group
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
        assert_eq!(group_for_spool(0), 0);
        assert_eq!(group_for_spool(19), 0);
        assert_eq!(group_for_spool(20), 1);
        assert_eq!(group_for_spool(999), 49);
    }

    #[test]
    fn test_group_start() {
        assert_eq!(group_start(0), 0);
        assert_eq!(group_start(1), 20);
        assert_eq!(group_start(49), 980);
    }

    #[test]
    fn test_spool_for_slice() {
        assert_eq!(spool_for_slice(0, 0), 0);
        assert_eq!(spool_for_slice(0, 19), 19);
        assert_eq!(spool_for_slice(1, 0), 20);
        assert_eq!(spool_for_slice(49, 19), 999);
    }

    #[test]
    fn test_spool_in_group() {
        assert!(spool_in_group(0, 0));
        assert!(spool_in_group(19, 0));
        assert!(!spool_in_group(20, 0));
        assert!(spool_in_group(20, 1));
        assert!(spool_in_group(999, 49));
    }
}
