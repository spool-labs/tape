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

/// Maximum blob size (1 GiB).
pub const MAX_BLOB_SIZE: usize = 1 << 30;

/// Maximum slice size (~100 MiB).
/// With k=10 data slices, each shard is approximately blob_size / 10.
pub const MAX_SLICE_SIZE: usize = MAX_BLOB_SIZE / 10;

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
        // With default k=10, max slice is ~100 MiB
        assert_eq!(MAX_SLICE_SIZE, MAX_BLOB_SIZE / 10);
    }
}
