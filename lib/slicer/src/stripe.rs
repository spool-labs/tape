//! Per-blob stripe sizing.
//!
//! A blob splits into equal stripes no larger than the cap, each rounded up to
//! the coder's encode granularity. Every stripe is full by construction, so
//! padding never exceeds one alignment unit per stripe, and the cap holds peak
//! encode memory flat regardless of blob size.

use tape_core::encoding::EncodingProfile;

use crate::clay::ClayCoder;
use crate::metadata::SliceMetadata;

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

/// Byte length of one coded slice, from a registered encoding alone.
///
/// Lets a caller weight a slice without holding it. Mirrors what `encode` lays
/// out, one chunk per stripe then the metadata, pinned by
/// `derived_length_matches_encoding`.
pub fn coded_slice_len(
    profile: EncodingProfile,
    size: usize,
    stripe_size: usize,
    stripe_count: usize,
) -> usize {
    let chunk = ClayCoder::from_params(profile.clay_params()).track_chunk_size(stripe_size, size);
    stripe_count * chunk + SliceMetadata::SIZE
}

// Alignment and padding bounds are pinned over the full track range by the
// conformance battery in tests/conformance.rs.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::coder::ErasureCoder;
    use crate::slicer::Slicer;

    /// A deterministic non-trivial payload, so the codec never sees all zeros.
    fn payload(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    // the length derived from a registered encoding is the length the encoder
    // actually produced, across the payload range a track can hold
    #[test]
    fn derived_length_matches_encoding() {
        for size in [1usize, 1_000, 100_000, 1_000_001, 4 * 1024 * 1024] {
            let mut slicer = Slicer::clay_default();
            let slices = slicer.encode(&payload(size)).expect("encode");

            let derived = coded_slice_len(
                slicer.profile(),
                size,
                slicer.stripe_size(),
                num_stripes(size, slicer.stripe_size()),
            );

            assert_eq!(derived, slices[0].len(), "size {size}");
            for slice in &slices {
                assert_eq!(slice.len(), derived, "size {size}, uneven slices");
            }
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
