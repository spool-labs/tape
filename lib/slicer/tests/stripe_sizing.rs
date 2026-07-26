//! Stripe sizing regression tests (issue 69).
//!
//! Writers derive the stripe size per blob. These tests pin the padding floor
//! that buys, the analytic model the padding claims rest on, and the alignment
//! granularity everything is built from.

use tape_core::encoding::ClayParams;
use tape_slicer::{
    derive_stripe_size, num_stripes, ClayCoder, ErasureCoder, SliceMetadata, Slicer, STRIPE_CAP,
};

/// SDK cap on a single coded track (sdk/src/stream/manifest.rs). Redeclared
/// because tape-sdk depends on this crate, so the test cannot import it. A
/// matching test in the SDK ties the scheme to the real constant.
const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Bytes stored across all slices for one blob, metadata suffixes excluded.
fn stored_payload_bytes(slicer: &mut Slicer<ClayCoder>, blob: &[u8]) -> usize {
    let slices = slicer.encode(blob).unwrap();
    slices.iter().map(|s| s.len() - SliceMetadata::SIZE).sum()
}

/// Stored bytes for a blob, derived from chunk arithmetic.
fn modeled_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    let stripe_size = derive_stripe_size(blob_len, coder.stripe_alignment(), STRIPE_CAP);
    let stripes = num_stripes(blob_len, stripe_size);
    stripes * coder.track_chunk_size(stripe_size, blob_len) * coder.n()
}

/// Minimum coded bytes for a blob at rate k/n, before any stripe padding.
fn ideal_payload_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    blob_len * coder.n() / coder.k()
}

// the analytic model used below reproduces the encoder exactly
#[test]
fn analytic_model_matches_encoder() {
    let mut slicer = Slicer::clay_default();
    for len in [1, 1_400, 100_000, 100_001, 250_000, 1_000_000, 1_000_001, 1_500_000] {
        assert_eq!(
            modeled_bytes(len),
            stored_payload_bytes(&mut slicer, &mk(len)),
            "model diverges at blob_len {len}"
        );
    }
}

// padding sits at the alignment floor at every size, including the sizes just
// past a round number where fixed stripe tiers used to pay up to 2x
#[test]
fn padding_stays_at_alignment_floor() {
    for len in [100_001usize, 1_000_001, 1_500_000, 2_000_001, 4 * 1024 * 1024 + 8] {
        let ratio = modeled_bytes(len) as f64 / ideal_payload_bytes(len) as f64;
        assert!(ratio < 1.01, "overhead at {len}: {ratio:.4}");
    }

    let cap_ratio =
        modeled_bytes(MAX_TRACK_SIZE) as f64 / ideal_payload_bytes(MAX_TRACK_SIZE) as f64;
    assert!(cap_ratio < 1.001, "overhead at cap: {cap_ratio:.5}");
}

// the floor the scheme is built on: the default Clay profile encodes in
// multiples of k * alpha * 2 = 1400 bytes, and the params arithmetic agrees
// with the constructed coder
#[test]
fn clay_alignment_floor_is_1400_bytes() {
    let params = ClayParams::default();
    let coder = ClayCoder::from_params(params);

    assert_eq!(coder.k(), 7);
    assert_eq!(coder.alpha(), 100);
    assert_eq!(params.alpha(), 100);
    assert_eq!(params.stripe_alignment(), 1_400);
    assert_eq!(coder.stripe_alignment(), 1_400);

    assert_eq!(coder.chunk_size_for(1) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_400) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_401) * coder.k(), 2_800);
}
