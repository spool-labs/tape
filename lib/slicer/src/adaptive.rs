//! Per-blob stripe sizing.
//!
//! A blob splits into equal stripes no larger than the cap, each rounded up to
//! the coder's encode granularity. Every stripe is full by construction, so
//! padding never exceeds one alignment unit per stripe, and the cap holds peak
//! encode memory flat regardless of blob size.

/// Largest stripe a writer emits.
pub const STRIPE_CAP: usize = 1_000_000;

/// Stripe size for a blob: an equal split no larger than the cap, rounded up
/// to the coder's alignment.
#[inline]
pub fn derive_stripe_size(blob_len: usize, alignment: usize, cap: usize) -> usize {
    let count = blob_len.div_ceil(cap).max(1);
    let stripe = blob_len.div_ceil(count).max(1);
    stripe.div_ceil(alignment) * alignment
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

    /// Default Clay profile encode granularity (k * alpha * 2).
    const ALIGN: usize = 1_400;

    #[test]
    fn test_derived_is_aligned_and_capped() {
        let cap_aligned = STRIPE_CAP.div_ceil(ALIGN) * ALIGN;

        for len in [0, 1, 1_399, 1_400, 100_000, 999_999, 1_000_001, 64 * 1024 * 1024] {
            let stripe = derive_stripe_size(len, ALIGN, STRIPE_CAP);
            assert!(stripe.is_multiple_of(ALIGN), "len {len}: stripe {stripe} unaligned");
            assert!(stripe >= ALIGN, "len {len}: stripe {stripe} below alignment");
            assert!(stripe <= cap_aligned, "len {len}: stripe {stripe} above cap");
        }
    }

    #[test]
    fn test_derived_padding_stays_below_one_unit_per_stripe() {
        for len in [1, 100_001, 250_000, 1_000_001, 2_000_001, 4 * 1024 * 1024 + 8] {
            let stripe = derive_stripe_size(len, ALIGN, STRIPE_CAP);
            let count = num_stripes(len, stripe);
            assert!(count * stripe >= len, "len {len}: stripes do not cover the blob");
            assert!(
                count * stripe - len < count * ALIGN,
                "len {len}: padding bound violated (stripe {stripe}, count {count})"
            );
        }
    }

    #[test]
    fn test_num_stripes() {
        assert_eq!(num_stripes(0, 100_000), 1);
        assert_eq!(num_stripes(1, 100_000), 1);
        assert_eq!(num_stripes(100_000, 100_000), 1);
        assert_eq!(num_stripes(100_001, 100_000), 2);
        assert_eq!(num_stripes(250_000, 100_000), 3);
    }
}
