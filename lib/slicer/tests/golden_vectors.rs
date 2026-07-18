//! Golden vectors for the production stripe scheme (issue 69).
//!
//! Pins the exact commitment roots and slice geometry of both formats: the
//! legacy ladder bytes already stored on the network, which must reproduce
//! forever, and the derived bytes production writers emit now. Any change
//! that alters these values is a storage compatibility break, not a
//! refactor. Regenerate only for a deliberate, fleet-coordinated change.

use tape_slicer::{blob_merkle_root, ErasureCoder, SliceMetadata, Slicer, StripePolicy};

/// (blob_len, stripe_size, slice_len, commitment root) under the default
/// Clay profile with the legacy ladder policy.
const GOLDEN_LADDER: &[(usize, usize, usize, &str)] = &[
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

/// The same fixture under the derived policy that production writers emit.
const GOLDEN_DERIVED: &[(usize, usize, usize, &str)] = &[
    (0, 1_400, 248, "BrZJiSC1cAcvk5cY8imLRDEFpcRtYkdRkosmYtjUsxhK"),
    (1, 1_400, 248, "2R1st4qvoTKcrpAPWfoKPM8boRsJyeQbHgr7GahXDeSw"),
    (1_399, 1_400, 248, "AqDcxBgyxTnEBYpaQfSqaUMyxsmVNB8MEaiBEJQ32KuK"),
    (1_400, 1_400, 248, "2ECRSSMn4CZTnud1hefx5Lx3pD3vZdS1f2BqCLa79fQo"),
    (100_000, 100_800, 14_448, "gmqUJ1JeymYFabBDCf7HEMdF5QFusAu2Vip1T87SyWZ"),
    (100_001, 100_800, 14_448, "3b9G4wQgwLmE5i1bQxz4sqhTwmkjrBfGTsWA661EbbLM"),
    (999_999, 1_001_000, 143_048, "Cujsr1SXuWyuMAetY8a54G3ucnvJ5DZdNLTT61E7WBBZ"),
    (1_000_001, 501_200, 143_248, "BfpDY4QWZ8v9kAbo7bZQsbqK7BNJscNZiHQPPhJbLBhn"),
    (2_500_000, 834_400, 357_648, "9Q4mTt8yay5Pdi5bVmjnHvJGsGjsUpPmwQm4rib5TKLT"),
];

fn check(slicer: &mut Slicer<tape_slicer::ClayCoder>, golden: &[(usize, usize, usize, &str)]) {
    for &(len, stripe_size, slice_len, root) in golden {
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

// bytes already stored on the network were written by ladder writers and
// must reproduce forever
#[test]
fn ladder_encoding_matches_golden_vectors() {
    let mut slicer = Slicer::clay_default();
    slicer.set_policy(StripePolicy::Ladder);
    check(&mut slicer, GOLDEN_LADDER);
}

// the derived format production writers emit from now on, pinned so writer
// output cannot drift before the fleet picks up this build
#[test]
fn derived_encoding_matches_golden_vectors() {
    let mut slicer = Slicer::clay_default();
    check(&mut slicer, GOLDEN_DERIVED);
}
