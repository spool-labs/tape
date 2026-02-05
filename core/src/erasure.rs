//! Erasure coding constants and parameters.
//!
//! These constants define the Clay erasure coding scheme used by tapedrive.
//! SPOOL_GROUP_SIZE is the number of slices per spool group (fixed network constant).
//! A blob's n may be ≤ SPOOL_GROUP_SIZE depending on its encoding profile.
//! SPOOL_COUNT is the total number of spools in the network.

/// Number of slices per spool group (fixed network constant).
/// Individual encoding profiles may use n ≤ SPOOL_GROUP_SIZE.
pub const SPOOL_GROUP_SIZE: usize = 20;

/// Default data slices for the default Clay profile (k=10).
pub const DATA_SLICES: usize = 10;

/// Default parity slices for the default Clay profile (m=10).
pub const PARITY_SLICES: usize = 10;

/// Number of spool groups in the network.
pub const SPOOL_GROUP_COUNT: usize = 50;

/// Total spools in the network.
pub const SPOOL_COUNT: usize = SPOOL_GROUP_COUNT * SPOOL_GROUP_SIZE;

/// Maximum blob size (1 GiB).
pub const MAX_BLOB_SIZE: usize = 1 << 30;

/// Maximum slice size. With Clay, each shard is approximately blob_size / DATA_SLICES.
pub const MAX_SLICE_SIZE: usize = MAX_BLOB_SIZE / DATA_SLICES;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_counts_add_up() {
        assert_eq!(DATA_SLICES + PARITY_SLICES, SPOOL_GROUP_SIZE);
    }

    #[test]
    fn test_expected_values() {
        assert_eq!(SPOOL_GROUP_SIZE, 20);
        assert_eq!(DATA_SLICES, 10);
        assert_eq!(PARITY_SLICES, 10);
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
        assert_eq!(MAX_SLICE_SIZE, MAX_BLOB_SIZE / DATA_SLICES);
    }
}
