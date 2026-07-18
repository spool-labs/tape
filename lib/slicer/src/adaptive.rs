//! Adaptive stripe size selection.
//!
//! Selects optimal stripe size based on blob size to balance:
//! - Encoding efficiency (larger stripes = better throughput)
//! - Memory usage (smaller stripes = less peak memory)
//! - Chunk overhead (fewer stripes = less metadata per chunk)

/// Available stripe sizes for adaptive encoding.
///
/// Sizes cover the blob range encode paths accept, capped at 64 MiB per
/// coded track:
/// - 100 KB: Blobs up to 1 MB
/// - 1 MB: Everything larger
pub const STRIPE_SIZES: [usize; 2] = [
    100_000,   // 100 KB
    1_000_000, //   1 MB
];

/// Default stripe size (1 MB).
pub const DEFAULT_STRIPE_SIZE: usize = STRIPE_SIZES[1];

/// Select optimal stripe size based on blob size.
///
/// Blobs up to 1 MB use 100 KB stripes (1-10 stripes); larger blobs use
/// 1 MB stripes.
#[inline]
pub fn pick_stripe_size(blob_len: usize) -> usize {
    if blob_len <= 1_000_000 {
        STRIPE_SIZES[0] // 100 KB
    } else {
        STRIPE_SIZES[1] // 1 MB
    }
}

/// Calculate number of stripes for a given blob and stripe size.
#[inline]
pub fn num_stripes(blob_len: usize, stripe_size: usize) -> usize {
    if blob_len == 0 {
        1
    } else {
        blob_len.div_ceil(stripe_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stripe_selection() {
        // Small blobs -> 100KB stripes
        assert_eq!(pick_stripe_size(100), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(500_000), STRIPE_SIZES[0]);
        assert_eq!(pick_stripe_size(1_000_000), STRIPE_SIZES[0]);

        // Larger blobs -> 1MB stripes
        assert_eq!(pick_stripe_size(1_000_001), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(50_000_000), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(64 * 1024 * 1024), STRIPE_SIZES[1]);
    }

    #[test]
    fn test_num_stripes() {
        assert_eq!(num_stripes(0, 100_000), 1);
        assert_eq!(num_stripes(1, 100_000), 1);
        assert_eq!(num_stripes(100_000, 100_000), 1);
        assert_eq!(num_stripes(100_001, 100_000), 2);
        assert_eq!(num_stripes(250_000, 100_000), 3);
    }

    #[test]
    fn test_ladder_ascending() {
        for pair in STRIPE_SIZES.windows(2) {
            assert!(pair[0] < pair[1]);
        }
    }
}
