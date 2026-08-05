//! Per-blob stripe sizing.
//!
//! A blob splits into equal stripes no larger than the cap, each rounded up to
//! the coder's encode granularity. Every stripe is full by construction, so
//! padding never exceeds one alignment unit per stripe, and the cap holds peak
//! encode memory flat regardless of blob size.

/// Largest stripe a writer emits. Encode throughput peaks here and flattens
/// above it, so the cap buys nothing to raise and holds peak encode memory
/// flat. The sweep behind that sits in tests/stripe_measure.rs.
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

/// Whether a stripe size read off a slice is the one this blob length derives.
/// Anything else is not something a writer at this cap could have produced.
#[inline]
pub fn stripe_size_accepted(
    stripe_size: usize,
    blob_len: usize,
    alignment: usize,
    cap: usize,
) -> bool {
    stripe_size == derive_stripe_size(blob_len, alignment, cap)
}

// Alignment and padding bounds are pinned over the full track range by the
// conformance battery in tests/conformance.rs.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_num_stripes() {
        assert_eq!(num_stripes(0, 100_000), 1);
        assert_eq!(num_stripes(1, 100_000), 1);
        assert_eq!(num_stripes(100_000, 100_000), 1);
        assert_eq!(num_stripes(100_001, 100_000), 2);
        assert_eq!(num_stripes(250_000, 100_000), 3);
    }
}
