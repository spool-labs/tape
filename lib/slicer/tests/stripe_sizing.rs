//! Stripe sizing regression tests (issue 69).
//!
//! Production writers derive the stripe size per blob; readers accept derived
//! sizes plus the legacy ladder that already-stored tracks carry. These tests
//! pin the derived scheme's padding floor, the legacy ladder's continued
//! acceptance, and the sawtooth the ladder pays (so old-format costs stay
//! documented, not rediscovered).

use tape_core::encoding::ClayParams;
use tape_slicer::{
    derive_stripe_size, num_stripes, pick_stripe_size, ClayCoder, ErasureCoder, SliceMetadata,
    Slicer, StripePolicy, DERIVED_STRIPE_CAP, MAX_CHUNK_BYTES, STRIPE_SIZES,
};

/// SDK cap on a single coded track (sdk/src/stream/manifest.rs). Redeclared
/// because tape-sdk depends on this crate, so the test cannot import it. A
/// matching test in the SDK ties the ladder to the real constant.
const MAX_TRACK_SIZE: usize = 64 * 1024 * 1024;

/// Default Clay profile encode granularity (k * alpha * 2).
const ALIGN: usize = 1_400;

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn ladder_slicer() -> Slicer<ClayCoder> {
    let mut slicer = Slicer::clay_default();
    slicer.set_policy(StripePolicy::Ladder);
    slicer
}

/// Bytes stored across all slices for one blob, metadata suffixes excluded.
fn stored_payload_bytes(slicer: &mut Slicer<ClayCoder>, blob: &[u8]) -> usize {
    let slices = slicer.encode(blob).unwrap();
    slices.iter().map(|s| s.len() - SliceMetadata::SIZE).sum()
}

/// The ladder scheme's stored bytes, derived from chunk arithmetic.
fn modeled_ladder_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    let stripe_size = pick_stripe_size(blob_len);
    let stripes = num_stripes(blob_len, stripe_size);
    stripes * coder.track_chunk_size(stripe_size, blob_len) * coder.n()
}

/// The derived scheme's stored bytes, derived from chunk arithmetic.
fn modeled_derived_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    let stripe_size = derive_stripe_size(blob_len, ALIGN, DERIVED_STRIPE_CAP);
    let stripes = num_stripes(blob_len, stripe_size);
    stripes * coder.track_chunk_size(stripe_size, blob_len) * coder.n()
}

/// Minimum coded bytes for a blob at rate k/n, before any stripe padding.
fn ideal_payload_bytes(blob_len: usize) -> usize {
    let coder = ClayCoder::from_params(ClayParams::default());
    blob_len * coder.n() / coder.k()
}

// the default writer emits the derived size for every blob and the metadata
// parser accepts it
#[test]
fn default_writer_output_accepted_by_parser() {
    let mut slicer = Slicer::clay_default();
    for len in [1, 500_000, 1_000_000, 2_000_000, 4 * 1024 * 1024 + 8] {
        let slices = slicer.encode(&mk(len)).unwrap();
        let meta = SliceMetadata::from_slice(&slices[0]).unwrap();
        assert_eq!(
            meta.stripe_size(),
            derive_stripe_size(len, ALIGN, DERIVED_STRIPE_CAP),
            "blob_len {len}"
        );
    }
}

// legacy ladder values stay accepted so stored tracks keep decoding; the
// ladder itself stays fully reachable for old-writer emulation
#[test]
fn legacy_ladder_still_accepted_and_reachable() {
    let mut slicer = ladder_slicer();
    for len in [1, 100_001, 1_000_001] {
        let slices = slicer.encode(&mk(len)).unwrap();
        let meta = SliceMetadata::from_slice(&slices[0]).unwrap();
        assert!(STRIPE_SIZES.contains(&meta.stripe_size()), "blob_len {len}");
    }

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

// the parser still fails closed on sizes that are neither ladder nor the
// derived size for the carried blob length
#[test]
fn parser_rejects_off_scheme_stripe_size() {
    let mut meta = SliceMetadata::new(3_000_000, STRIPE_SIZES[1]);
    meta.stripe_size = 2_000_000;
    assert!(SliceMetadata::from_slice(&meta.to_bytes()).is_err());

    meta.stripe_size = 0;
    assert!(SliceMetadata::from_slice(&meta.to_bytes()).is_err());
}

// the analytic models used below reproduce both encoders exactly
#[test]
fn analytic_models_match_encoders() {
    let mut ladder = ladder_slicer();
    let mut derived = Slicer::clay_default();
    for len in [1, 1_400, 100_000, 100_001, 250_000, 1_000_000, 1_000_001, 1_500_000] {
        let blob = mk(len);
        assert_eq!(
            modeled_ladder_bytes(len),
            stored_payload_bytes(&mut ladder, &blob),
            "ladder model diverges at blob_len {len}"
        );
        assert_eq!(
            modeled_derived_bytes(len),
            stored_payload_bytes(&mut derived, &blob),
            "derived model diverges at blob_len {len}"
        );
    }
}

// derived padding sits at the alignment floor at every size, including the
// boundary-adjacent sizes where the ladder paid up to 2x
#[test]
fn derived_padding_stays_at_alignment_floor() {
    for len in [100_001usize, 1_000_001, 1_500_000, 2_000_001, 4 * 1024 * 1024 + 8] {
        let ratio = modeled_derived_bytes(len) as f64 / ideal_payload_bytes(len) as f64;
        assert!(ratio < 1.01, "derived overhead at {len}: {ratio:.4}");
    }
    let cap_ratio = modeled_derived_bytes(MAX_TRACK_SIZE) as f64
        / ideal_payload_bytes(MAX_TRACK_SIZE) as f64;
    assert!(cap_ratio < 1.001, "derived overhead at cap: {cap_ratio:.5}");
}

// the ladder's sawtooth stays documented: one byte past a boundary stored
// nearly twice the coded ideal, decaying to about one percent at the cap
#[test]
fn legacy_ladder_sawtooth_documented() {
    let mut slicer = ladder_slicer();
    for boundary in [100_000usize, 1_000_000] {
        let at = stored_payload_bytes(&mut slicer, &mk(boundary)) as f64
            / ideal_payload_bytes(boundary) as f64;
        let past = stored_payload_bytes(&mut slicer, &mk(boundary + 1)) as f64
            / ideal_payload_bytes(boundary + 1) as f64;
        assert!(at < 1.05, "ladder overhead at {boundary}: {at:.3}");
        assert!(past > 1.9, "ladder overhead past {boundary}: {past:.3}");
    }

    let cap_ratio = modeled_ladder_bytes(MAX_TRACK_SIZE) as f64
        / ideal_payload_bytes(MAX_TRACK_SIZE) as f64;
    assert!(cap_ratio < 1.02, "ladder overhead at cap: {cap_ratio:.4}");
}

// the floor for any sizing scheme: the default Clay profile encodes in
// multiples of k * alpha * 2 = 1400 bytes, and the params arithmetic agrees
// with the constructed coder
#[test]
fn clay_alignment_floor_is_1400_bytes() {
    let params = ClayParams::default();
    let coder = ClayCoder::from_params(params);

    assert_eq!(coder.k(), 7);
    assert_eq!(coder.alpha(), 100);
    assert_eq!(params.alpha(), 100);
    assert_eq!(params.stripe_alignment() as usize, ALIGN);
    assert_eq!(coder.stripe_alignment(), ALIGN);

    assert_eq!(coder.chunk_size_for(1) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_400) * coder.k(), 1_400);
    assert_eq!(coder.chunk_size_for(1_401) * coder.k(), 2_800);
}
