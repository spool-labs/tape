//! Erasure coding constants and parameters.
//!
//! These constants define the Reed-Solomon erasure coding scheme used by tapedrive.
//! The scheme splits blobs into 1024 slices, with the data/parity ratio derived
//! from BFT tolerance bounds (2/3 + 1 quorum requirement).

use crate::bft::{max_faulty, min_correct};

/// Total number of slices per blob (data + parity).
///
/// This matches `SPOOL_COUNT` - each slice maps 1:1 to a spool.
pub const TOTAL_SLICES: usize = 1024;

/// Number of parity (redundancy) slices.
///
/// Derived from `max_faulty(TOTAL_SLICES)` - the maximum number of slices
/// that can be lost while still allowing reconstruction.
///
/// This provides BFT-style fault tolerance: up to 1/3 of slices can be missing.
pub const PARITY_SLICES: usize = max_faulty(TOTAL_SLICES as u64) as usize;

/// Number of data slices required for blob reconstruction.
///
/// Derived from `min_correct(TOTAL_SLICES)` - the minimum number of slices
/// needed to reconstruct the original blob.
///
/// With Reed-Solomon encoding, any 683 slices are sufficient.
pub const DATA_SLICES: usize = min_correct(TOTAL_SLICES as u64) as usize;

/// Maximum size of a single slice in bytes (256 KB).
pub const MAX_SLICE_SIZE: usize = 256 * 1024;

/// Maximum blob size in bytes (TOTAL_SLICES * MAX_SLICE_SIZE = 256 MB).
pub const MAX_BLOB_SIZE: usize = TOTAL_SLICES * MAX_SLICE_SIZE;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_counts_add_up() {
        assert_eq!(DATA_SLICES + PARITY_SLICES, TOTAL_SLICES);
    }

    #[test]
    fn test_derived_from_bft() {
        // Verify constants match bft functions
        assert_eq!(PARITY_SLICES, max_faulty(TOTAL_SLICES as u64) as usize);
        assert_eq!(DATA_SLICES, min_correct(TOTAL_SLICES as u64) as usize);
    }

    #[test]
    fn test_expected_values() {
        // Sanity check the actual values
        assert_eq!(TOTAL_SLICES, 1024);
        assert_eq!(DATA_SLICES, 683);
        assert_eq!(PARITY_SLICES, 341);
    }

    #[test]
    fn test_fault_tolerance() {
        // Can lose up to 1/3 of slices (parity count)
        assert!(PARITY_SLICES as f64 / TOTAL_SLICES as f64 >= 0.33);
    }

    #[test]
    fn test_max_blob_size() {
        // 256 MB maximum
        assert_eq!(MAX_BLOB_SIZE, 256 * 1024 * 1024);
    }
}
