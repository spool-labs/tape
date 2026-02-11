//! Inconsistency detection via merkle root comparison.
//!
//! When full recovery detects that re-encoded slices don't match the on-chain
//! commitment, the node should produce an inconsistency proof and submit it.
//! Actual BLS attestation for fraud proofs is not yet implemented, but this
//! module performs the merkle root comparison to detect mismatches.

use tape_crypto::Hash;
use tape_slicer::merkle_helpers::blob_merkle_root;
use tape_store::types::Pubkey;

/// Result of an inconsistency check.
#[derive(Debug)]
pub enum InconsistencyResult {
    /// Slices are consistent with on-chain commitment.
    Consistent,
    /// Inconsistency detected but proof generation not yet implemented.
    DetectedButUnproven {
        track: Pubkey,
        expected_root: Hash,
        computed_root: Hash,
    },
}

/// Check slice consistency against an on-chain commitment.
///
/// Computes the merkle root of the re-encoded slices and compares it
/// against the on-chain commitment hash. Returns `DetectedButUnproven`
/// if they differ (BLS attestation not yet implemented).
pub fn check_consistency(
    track: Pubkey,
    commitment: &Hash,
    reencoded_slices: &[Vec<u8>],
) -> InconsistencyResult {
    let computed_root = blob_merkle_root(reencoded_slices);
    if computed_root != *commitment {
        InconsistencyResult::DetectedButUnproven {
            track,
            expected_root: *commitment,
            computed_root,
        }
    } else {
        InconsistencyResult::Consistent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_slicer::merkle_helpers::blob_merkle_root;

    #[test]
    fn matching_slices_are_consistent() {
        let slices: Vec<Vec<u8>> = (0..20).map(|i| vec![i; 100]).collect();
        let root = blob_merkle_root(&slices);

        let result = check_consistency(Pubkey([0u8; 32]), &root, &slices);
        assert!(matches!(result, InconsistencyResult::Consistent));
    }

    #[test]
    fn mismatched_slices_detected() {
        let slices: Vec<Vec<u8>> = (0..20).map(|i| vec![i; 100]).collect();
        let wrong_root = Hash::default();

        let result = check_consistency(Pubkey([1u8; 32]), &wrong_root, &slices);
        match result {
            InconsistencyResult::DetectedButUnproven {
                expected_root,
                computed_root,
                ..
            } => {
                assert_eq!(expected_root, wrong_root);
                assert_eq!(computed_root, blob_merkle_root(&slices));
            }
            InconsistencyResult::Consistent => panic!("expected inconsistency"),
        }
    }
}
