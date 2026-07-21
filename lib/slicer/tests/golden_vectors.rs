//! Golden vectors for the production stripe scheme (issue 69).
//!
//! Pins the exact commitment roots and slice geometry writers emit. Any change
//! that alters these values is a storage compatibility break, not a refactor.
//! Regenerate only for a deliberate, fleet-coordinated change.

use tape_slicer::{blob_merkle_root, ErasureCoder, SliceMetadata, Slicer};

/// (blob_len, stripe_size, slice_len, commitment root) under the default
/// Clay profile.
const GOLDEN: &[(usize, usize, usize, &str)] = &[
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
