//! Maps a byte range of a blob to the stripes and slice windows that hold it

use tape_core::erasure::SAMPLE_WINDOW_BYTES;

use crate::metadata::SliceMetadata;
use crate::stripe::num_stripes;

/// A byte range covers these stripes, and every slice holds them in this run
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StripeWindow {
    pub first_stripe: usize,
    pub stripe_count: usize,
    pub slice_offset: usize,
    pub slice_len: usize,
    pub skip: usize,
    pub take: usize,
}

impl StripeWindow {
    /// The slice window starts in this sidecar window
    pub fn first_sample_window(&self) -> usize {
        self.slice_offset / SAMPLE_WINDOW_BYTES
    }
}

/// Map `[start, end)` of a blob onto its stripes and a window-aligned run of every slice
pub fn stripe_window(
    blob_len: usize,
    stripe_size: usize,
    chunk_size: usize,
    start: usize,
    end: usize,
) -> Option<StripeWindow> {
    if stripe_size == 0 || chunk_size == 0 || start >= end || end > blob_len {
        return None;
    }

    let first_stripe = start / stripe_size;
    let last_stripe = (end - 1) / stripe_size;
    let slice_total = num_stripes(blob_len, stripe_size) * chunk_size + SliceMetadata::SIZE;
    let needed_start = first_stripe * chunk_size;
    let needed_end = (last_stripe + 1) * chunk_size;
    let slice_offset = needed_start / SAMPLE_WINDOW_BYTES * SAMPLE_WINDOW_BYTES;
    let slice_end = needed_end
        .div_ceil(SAMPLE_WINDOW_BYTES)
        .saturating_mul(SAMPLE_WINDOW_BYTES)
        .min(slice_total);

    Some(StripeWindow {
        first_stripe,
        stripe_count: last_stripe - first_stripe + 1,
        slice_offset,
        slice_len: slice_end - slice_offset,
        skip: start - first_stripe * stripe_size,
        take: end - start,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const STRIPE: usize = 987_000;
    const CHUNK: usize = 141_000;
    const BLOB: usize = 64 * 1024 * 1024;

    // a range inside one stripe reads that stripe's windows alone
    #[test]
    fn one_stripe() {
        let window = stripe_window(BLOB, STRIPE, CHUNK, STRIPE * 10 + 5, STRIPE * 10 + 4101).expect("window");

        assert_eq!((window.first_stripe, window.stripe_count), (10, 1));
        assert_eq!((window.skip, window.take), (5, 4096));
        assert_eq!(window.slice_offset % SAMPLE_WINDOW_BYTES, 0);
        assert!(window.slice_offset <= 10 * CHUNK);
        assert!(window.slice_offset + window.slice_len >= 11 * CHUNK);
    }

    // a range across a stripe edge reads both stripes
    #[test]
    fn two_stripes() {
        let window = stripe_window(BLOB, STRIPE, CHUNK, STRIPE - 10, STRIPE + 10).expect("window");

        assert_eq!((window.first_stripe, window.stripe_count), (0, 2));
        assert_eq!((window.skip, window.take), (STRIPE - 10, 20));
        assert_eq!(window.slice_offset, 0);
    }

    // the last window stops at the slice end, metadata included
    #[test]
    fn tail() {
        let stripes = num_stripes(BLOB, STRIPE);
        let window = stripe_window(BLOB, STRIPE, CHUNK, BLOB - 1, BLOB).expect("window");

        assert_eq!(window.first_stripe, stripes - 1);
        assert_eq!(window.slice_offset + window.slice_len, stripes * CHUNK + SliceMetadata::SIZE);
    }

    // empty or out-of-blob ranges map to nothing
    #[test]
    fn bad_ranges() {
        assert!(stripe_window(BLOB, STRIPE, CHUNK, 10, 10).is_none());
        assert!(stripe_window(BLOB, STRIPE, CHUNK, 0, BLOB + 1).is_none());
    }
}
