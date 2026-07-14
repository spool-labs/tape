//! Adaptive stripe size selection.
//!
//! Selects optimal stripe size based on blob size to balance:
//! - Encoding efficiency (larger stripes = better throughput)
//! - Memory usage (smaller stripes = less peak memory)
//! - Chunk overhead (fewer stripes = less metadata per chunk)

/// Available stripe sizes for adaptive encoding.
///
/// Multiples of 2000 for Clay alignment (k × α × 2 = 10 × 100 × 2 = 2000).
/// Sizes chosen to cover common blob size ranges:
/// - 100 KB: Small blobs (< 1 MB)
/// - 1 MB: Medium blobs (1-100 MB)
/// - 10 MB: Large blobs (> 100 MB)
pub const STRIPE_SIZES: [usize; 3] = [
    100_000,     // 100 KB
    1_000_000,   //   1 MB
    10_000_000,  //  10 MB
];

/// Default stripe size (10 MB).
pub const DEFAULT_STRIPE_SIZE: usize = STRIPE_SIZES[2];

/// Select optimal stripe size based on blob size.
///
/// Strategy:
/// - Blobs ≤ 1 MB: Use 100 KB stripes (1-10 stripes)
/// - Blobs ≤ 100 MB: Use 1 MB stripes (1-100 stripes)
/// - Blobs > 100 MB: Use 10 MB stripes (10+ stripes)
#[inline]
pub fn pick_stripe_size(blob_len: usize) -> usize {
    if blob_len <= 1_000_000 {
        STRIPE_SIZES[0] // 100 KB
    } else if blob_len <= 100_000_000 {
        STRIPE_SIZES[1] // 1 MB
    } else {
        STRIPE_SIZES[2] // 10 MB
    }
}

/// Calculate number of stripes for a given blob and stripe size.
#[inline]
pub fn num_stripes(blob_len: usize, stripe_size: usize) -> usize {
    if blob_len == 0 {
        1
    } else {
        (blob_len + stripe_size - 1) / stripe_size
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

        // Medium blobs -> 1MB stripes
        assert_eq!(pick_stripe_size(1_000_001), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(50_000_000), STRIPE_SIZES[1]);
        assert_eq!(pick_stripe_size(100_000_000), STRIPE_SIZES[1]);

        // Large blobs -> 10MB stripes
        assert_eq!(pick_stripe_size(100_000_001), STRIPE_SIZES[2]);
        assert_eq!(pick_stripe_size(1_000_000_000), STRIPE_SIZES[2]);
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
    fn test_stripe_alignment() {
        // Verify all stripe sizes are multiples of Clay alignment (2000)
        const CLAY_ALIGNMENT: usize = 2000;
        for &size in &STRIPE_SIZES {
            assert_eq!(size % CLAY_ALIGNMENT, 0, "{size} not aligned");
        }
    }
}
