//! Golden vectors for the production stripe scheme (issue 69).
//!
//! Pins the exact commitment roots and slice geometry today's writers
//! produce. Data already stored on the network must stay decodable forever,
//! so any change that alters these values is a storage compatibility break,
//! not a refactor. Regenerate only for a deliberate, fleet-coordinated
//! format change.

use tape_slicer::{blob_merkle_root, ErasureCoder, SliceMetadata, Slicer};

/// (blob_len, stripe_size, slice_len, commitment root) under the default
/// Clay profile with the adaptive ladder policy.
const GOLDEN: &[(usize, usize, usize, &str)] = &[
    (0, 100_000, 14_448, "7oJUUK2A8TmvJ6LJA7va5xBkKuDxjtQiYgh9iYkMNhx7"),
    (1, 100_000, 248, "A9PgTbK8nwqXnKpg5TrgH3Es66YXSruUij8e4fY2irkt"),
    (1_399, 100_000, 248, "3atDNmoNomRH1j34ChCTpXDY9bEpdp8AX6nKUeaCo9M4"),
    (1_400, 100_000, 248, "Cs2iWxswjB19U9QshbrAkbCHoqFU11VnccCtxtHCYm2J"),
    (100_000, 100_000, 14_448, "6tU8kuYuo2CxeXgL9vdoHVjKhnwFVy6iSDKH1fmTLTv"),
    (100_001, 100_000, 28_848, "DkMpfumswqBxub48rrJJLujwzs8r4SQV4NeY5CRpKEUS"),
    (999_999, 100_000, 144_048, "2pUti5RcC3AeNrTJ9nzvhjQWje6rzeY7j8XS4MBo1nJJ"),
    (1_000_001, 1_000_000, 286_048, "H5hZC7DHAstBricWBoMZtkxbPFpSjv7A33f5pb6by4HL"),
    (2_500_000, 1_000_000, 429_048, "AACVhQfUf6BPnMnAEf483qwWV1KT11aT118W6Gp7rfdS"),
];

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[test]
fn production_encoding_matches_golden_vectors() {
    for &(len, stripe_size, slice_len, root) in GOLDEN {
        let mut slicer = Slicer::clay_default();
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
