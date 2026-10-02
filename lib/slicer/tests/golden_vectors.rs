//! Golden vectors for the production stripe scheme (issue 69).
//!
//! Pins the exact commitment roots and slice geometry writers emit. Any change
//! that alters these values is a storage compatibility break, not a refactor.
//! Regenerate only for a deliberate, fleet-coordinated change.

use tape_slicer::{blob_merkle_root, ErasureCoder, SliceMetadata, Slicer};

/// (blob_len, stripe_size, slice_len, commitment root) under the default
/// Clay profile.
///
/// Roots regenerated for the two-level sub-leaf commitment.
const GOLDEN: &[(usize, usize, usize, &str)] = &[
    (0, 1_400, 248, "DoM27JE3kReomQ4qwxB2qHDqRkHiLH4aBght8EUDuBuY"),
    (1, 1_400, 248, "2KJ8MwxdoDReK11CEmxhZ33mrMuk8gwQCodEsyyN5Gp5"),
    (1_399, 1_400, 248, "89fHjeavCQqRyHkc5PkRMnoy88CxyHDcuGXUqRcFHx3b"),
    (1_400, 1_400, 248, "8LF2CqjvNBtRf48rMTkvmJkhZXsJaKGJ2J1zUDUDHMdd"),
    (100_000, 100_800, 14_448, "CWLL6XqrRBV3tuCnWqR8xGooZJuUYvFsgQxxe8HSTYQ8"),
    (100_001, 100_800, 14_448, "AovFXFgNHokoaZU4jKk1rQEd16J9zGmHAmVc4XgVJrCP"),
    (999_999, 1_001_000, 143_048, "Ffyohm1UopJy4mq6rWYhh22UK56MtXGDAGfffmhSMiWh"),
    (1_000_001, 501_200, 143_248, "C31MwRtf7QPrWs2Fvvo8jFKinR9rGFu7BSDWrkmxWEod"),
    (2_500_000, 834_400, 357_648, "HDySbwKaeDwgB3apSAfE3bLMH1vVtCvhPB5jnaFUzs4k"),
];

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

// writer output cannot drift without breaking every commitment already on the
// network
#[test]
fn encoding_matches_golden_vectors() {
    let mut slicer = Slicer::clay_default();

    for &(len, stripe_size, slice_len, root) in GOLDEN {
        let slices = slicer.encode(&mk(len)).unwrap();
        let meta = SliceMetadata::parse(&slices[0]).unwrap();

        assert_eq!(meta.stripe_size(), stripe_size, "blob_len {len}: stripe size drifted");
        assert_eq!(slices[0].len(), slice_len, "blob_len {len}: slice length drifted");
        assert_eq!(
            blob_merkle_root(&slices).to_string(),
            root,
            "blob_len {len}: commitment root drifted"
        );
    }
}
