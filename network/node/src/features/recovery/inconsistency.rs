//! Inconsistency proof stub.
//!
//! When full recovery detects that re-encoded slices don't match the on-chain
//! commitment, the node should produce an inconsistency proof and submit it.
//! This module stubs that flow — actual proof generation is not yet implemented.

use tape_crypto::Hash;
use tape_store::types::Pubkey;
use tracing::warn;

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
/// Stub: always returns `Consistent`. When implemented, this will compare
/// re-encoded merkle root against the on-chain commitment hash and produce
/// a fraud proof if they differ.
pub fn check_consistency(
    _track: Pubkey,
    _commitment: &Hash,
    _reencoded_slices: &[Vec<u8>],
) -> InconsistencyResult {
    warn!("inconsistency proof check is not yet implemented");
    InconsistencyResult::Consistent
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_consistent() {
        let result = check_consistency(
            Pubkey([0u8; 32]),
            &Hash::default(),
            &[vec![1, 2, 3]],
        );
        assert!(matches!(result, InconsistencyResult::Consistent));
    }
}
