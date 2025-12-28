//! Erasure coding constants and parameters.
//!
//! These constants define the Reed-Solomon erasure coding scheme used by tapedrive.
//! The data/parity ratio is derived from BFT tolerance bounds (2/3 + 1 quorum).

use crate::bft::{max_faulty, min_correct};

/// Total number of slices per blob (2^10 = 1024).
///
/// Each slice maps 1:1 to a spool.
pub const SLICE_COUNT: usize = 1 << 10;

/// Number of parity (redundancy) slices.
///
/// Derived from `max_faulty(SLICE_COUNT)` - the maximum number of slices
/// that can be lost while still allowing reconstruction.
///
/// This provides BFT-style fault tolerance: up to 1/3 of slices can be missing.
pub const PARITY_SLICES: usize = max_faulty(SLICE_COUNT as u64) as usize;

/// Number of data slices required for blob reconstruction.
///
/// Derived from `min_correct(SLICE_COUNT)` - the minimum number of slices
/// needed to reconstruct the original blob.
///
/// With Reed-Solomon encoding, any DATA_SLICES slices are sufficient.
pub const DATA_SLICES: usize = min_correct(SLICE_COUNT as u64) as usize;

/// Maximum size of a single slice in bytes (1 MiB).
pub const MAX_SLICE_SIZE: usize = 1 << 20;

/// Maximum blob size in bytes (1 GiB).
///
/// Derived from SLICE_COUNT * MAX_SLICE_SIZE = 1024 * 1MiB = 1GiB.
pub const MAX_BLOB_SIZE: usize = SLICE_COUNT * MAX_SLICE_SIZE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_counts_add_up() {
        assert_eq!(DATA_SLICES + PARITY_SLICES, SLICE_COUNT);
    }

    #[test]
    fn test_derived_from_bft() {
        // Verify constants match bft functions
        assert_eq!(PARITY_SLICES, max_faulty(SLICE_COUNT as u64) as usize);
        assert_eq!(DATA_SLICES, min_correct(SLICE_COUNT as u64) as usize);
    }

    #[test]
    fn test_expected_values() {
        // Sanity check the actual values
        assert_eq!(SLICE_COUNT, 1024);
        assert_eq!(DATA_SLICES, 683);
        assert_eq!(PARITY_SLICES, 341);
    }

    #[test]
    fn test_slice_size() {
        // 1 MiB per slice
        assert_eq!(MAX_SLICE_SIZE, 1024 * 1024);
    }

    #[test]
    fn test_max_blob_size() {
        // 1 GiB maximum
        assert_eq!(MAX_BLOB_SIZE, 1024 * 1024 * 1024);
    }

    #[test]
    fn test_total_slices_is_power_of_two() {
        assert_eq!(SLICE_COUNT, 1 << 10);
        assert!(SLICE_COUNT.is_power_of_two());
    }
}
