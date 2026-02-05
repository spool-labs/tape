//! Erasure coding constants and parameters.
//!
//! These constants define the Clay erasure coding scheme used by tapedrive.
//! SLICE_COUNT is the number of slices per blob (one spool group).
//! SPOOL_COUNT is the total number of spools in the network.

/// Slices per blob (one spool group).
pub const SLICE_COUNT: usize = 20;

/// Data slices needed for reconstruction.
pub const DATA_SLICES: usize = 10;

/// Parity slices per blob.
pub const PARITY_SLICES: usize = 10;

/// Number of spool groups in the network.
pub const SPOOL_GROUP_COUNT: usize = 50;

/// Total spools in the network.
pub const SPOOL_COUNT: usize = SPOOL_GROUP_COUNT * SLICE_COUNT;

/// Maximum blob size (1 GiB).
pub const MAX_BLOB_SIZE: usize = 1 << 30;

/// Maximum slice size. With Clay, each shard is approximately blob_size / DATA_SLICES.
pub const MAX_SLICE_SIZE: usize = MAX_BLOB_SIZE / DATA_SLICES;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_counts_add_up() {
        assert_eq!(DATA_SLICES + PARITY_SLICES, SLICE_COUNT);
    }

    #[test]
    fn test_expected_values() {
        assert_eq!(SLICE_COUNT, 20);
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
