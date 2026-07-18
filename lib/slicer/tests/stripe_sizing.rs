//! Stripe ladder reachability and padding behavior (issue 69).
//!
//! Guards against two regressions:
//! 1. A ladder tier only selectable for blobs larger than any encode path
//!    accepts, which becomes dead code the parser still validates against.
//! 2. Unquantified drift in the final-stripe padding that multi-stripe blobs
//!    pay just past a stripe boundary.

use tape_core::encoding::ClayParams;
use tape_slicer::{
    num_stripes, pick_stripe_size, ClayCoder, ErasureCoder, SliceMetadata, Slicer,
    DEFAULT_STRIPE_SIZE, MAX_CHUNK_BYTES, STRIPE_SIZES,
};

/// SDK cap on a single coded track (sdk/src/stream/manifest.rs). Redeclared
/// because tape-sdk depends on this crate, so the test cannot import it. A
/// matching test in the SDK ties the ladder to the real constant.
const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// Bytes stored across all slices for one blob, metadata suffixes excluded.
fn stored_payload_bytes(blob: &[u8]) -> usize {
    let mut slicer = Slicer::clay_default();
    let slices = slicer.encode(blob).unwrap();
    slices.iter().map(|s| s.len() - SliceMetadata::SIZE).sum()
}

/// The same figure derived from chunk arithmetic without encoding.
fn modeled_payload_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    let stripe_size = pick_stripe_size(blob_len);
    let stripes = num_stripes(blob_len, stripe_size);
    stripes * coder.track_chunk_size(stripe_size, blob_len) * coder.n()
}

/// Minimum coded bytes for a blob at rate k/n, before any stripe padding.
fn ideal_payload_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    blob_len * coder.n() / coder.k()
}

// Every ladder tier is selected by some blob size within the track cap, and
// only ladder values are ever selected, so no tier is dead code.
#[test]
fn every_ladder_tier_reachable_within_track_cap() {
    let mut seen: Vec<usize> = STRIPE_SIZES
        .iter()
        .flat_map(|&size| [size, size + 1])
        .chain([1, MAX_CHUNK_BYTES, MAX_TRACK_SIZE])
        .filter(|&len| len <= MAX_TRACK_SIZE)
        .map(pick_stripe_size)
        .collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen, STRIPE_SIZES);
}

// The constructor default is the top tier, and encode always picks from the
// ladder, so slice metadata never carries an off-ladder size.
#[test]
fn encode_always_picks_a_ladder_stripe_size() {
    assert_eq!(DEFAULT_STRIPE_SIZE, *STRIPE_SIZES.last().unwrap());

    let mut slicer = Slicer::clay_default();
    for len in [1, 500_000, 1_000_000, 2_000_000] {
        let slices = slicer.encode(&mk(len)).unwrap();
        let meta = SliceMetadata::from_slice(&slices[0]).unwrap();
        assert!(STRIPE_SIZES.contains(&meta.stripe_size()), "blob_len {len}");
    }
}

// Rollout constraint: every decode, repair, and recover path parses metadata
// through this check, so a stripe size outside the ladder fails fleet-wide.
// Any sizing scheme that emits new values needs all parsers upgraded first.
#[test]
fn parser_rejects_off_ladder_stripe_size() {
    let mut meta = SliceMetadata::new(3_000_000, STRIPE_SIZES[1]);
    meta.stripe_size = 2_000_000;
    assert!(SliceMetadata::from_slice(&meta.to_bytes()).is_err());
}

// The analytic model used below reproduces the encoder exactly.
#[test]
fn analytic_model_matches_encoder() {
    for len in [1, 1_400, 100_000, 100_001, 250_000, 1_000_000, 1_000_001, 1_500_000] {
        assert_eq!(
            modeled_payload_bytes(len),
            stored_payload_bytes(&mk(len)),
            "model diverges at blob_len {len}"
        );
    }
}

// One byte past a stripe boundary encodes as two full stripes, storing nearly
// twice the k/n coded overhead. At the boundary itself the overhead is only
// the coder's 1400-byte alignment rounding.
#[test]
fn padding_sawtooth_peaks_past_stripe_boundaries() {
    for boundary in [100_000, 1_000_000] {
        let at = stored_payload_bytes(&mk(boundary)) as f64 / ideal_payload_bytes(boundary) as f64;
        let past = stored_payload_bytes(&mk(boundary + 1)) as f64
            / ideal_payload_bytes(boundary + 1) as f64;

        assert!(at < 1.05, "overhead at {boundary} boundary: {at:.3}");
        assert!(past > 1.9, "overhead past {boundary} boundary: {past:.3}");
    }
}

// The sawtooth decays as the final stripe fills back up.
#[test]
fn padding_overhead_decays_as_final_stripe_fills() {
    let ratio = |len: usize| modeled_payload_bytes(len) as f64 / ideal_payload_bytes(len) as f64;
    assert!(ratio(1_000_001) > ratio(1_500_000));
    assert!(ratio(1_500_000) > ratio(1_999_999));
    assert!(ratio(1_999_999) < 1.01);
}

// At the 64 MiB track cap the final-stripe padding amortizes to roughly one
// percent.
#[test]
fn padding_amortizes_at_max_track_size() {
    let ratio = modeled_payload_bytes(MAX_TRACK_SIZE) as f64
        / ideal_payload_bytes(MAX_TRACK_SIZE) as f64;
    assert!(ratio < 1.02, "overhead at MAX_TRACK_SIZE: {ratio:.4}");
}

// The floor for any sizing scheme: the default Clay profile encodes in
// multiples of k * alpha * 2 = 1400 bytes, so padding below that rounding is
// impossible.
#[test]
fn clay_alignment_floor_is_1400_bytes() {
    let coder = ClayCoder::from_params(ClayParams::default());
    assert_eq!(coder.k(), 7);
    assert_eq!(coder.alpha(), 100);
    assert_eq!(coder.k() * coder.alpha() * 2, 1_400);

    assert_eq!(coder.chunk_size_for(1) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_400) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_401) * coder.k(), 2_800);
}
