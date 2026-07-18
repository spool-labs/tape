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

/// Cap for derived stripe sizes, preserving today's encode memory profile.
pub const DERIVED_STRIPE_CAP: usize = 1_000_000;

/// Derive a per-blob stripe size: split the blob into equal stripes no larger
/// than roughly the cap, rounded up to the coder's alignment so total padding
/// stays below one alignment unit per stripe.
pub fn derive_stripe_size(blob_len: usize, alignment: usize, cap: usize) -> usize {
    let count = blob_len.div_ceil(cap).max(1);
    let stripe = blob_len.div_ceil(count).max(1);
    stripe.div_ceil(alignment) * alignment
}

/// Whether a stripe size is one production readers accept for this blob
/// length and coder alignment: a legacy ladder value, or the derived size.
pub fn stripe_size_accepted(stripe_size: usize, blob_len: usize, alignment: usize) -> bool {
    if stripe_size == 0 {
        return false;
    }
    STRIPE_SIZES.contains(&stripe_size)
        || stripe_size == derive_stripe_size(blob_len, alignment, DERIVED_STRIPE_CAP)
}

/// Writer-side stripe sizing policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StripePolicy {
    /// Per-blob equal split, aligned and capped (production default).
    Derived { cap: usize },
    /// Legacy ladder selection by blob size, kept for reading and for
    /// emulating pre-derived writers in tests.
    Ladder,
    /// Honor an explicitly configured stripe size.
    Fixed(usize),
}

impl StripePolicy {
    pub fn stripe_size_for(&self, blob_len: usize, alignment: usize) -> usize {
        match *self {
            Self::Derived { cap } => derive_stripe_size(blob_len, alignment, cap),
            Self::Ladder => pick_stripe_size(blob_len),
            Self::Fixed(size) => size,
        }
    }
}

/// Reader-side stripe size acceptance for slice metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StripeValidation {
    /// Ladder members, or the size the derived policy picks for the blob
    /// length carried in the same metadata (production default).
    LadderOrDerived { cap: usize },
    /// Ladder members or one explicit size.
    LadderOrExact(usize),
}

impl StripeValidation {
    pub fn accepts(&self, stripe_size: usize, blob_len: usize, alignment: usize) -> bool {
        if stripe_size == 0 {
            return false;
        }
        if STRIPE_SIZES.contains(&stripe_size) {
            return true;
        }
        match *self {
            Self::LadderOrDerived { cap } => {
                stripe_size == derive_stripe_size(blob_len, alignment, cap)
            }
            Self::LadderOrExact(size) => stripe_size == size,
        }
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
