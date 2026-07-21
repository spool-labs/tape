//! Stripe geometry conformance battery (issue 69).
//!
//! Runs identical invariants at several stripe caps, so single-stripe and
//! many-stripe blobs go through one code path under a fixed test surface:
//! roundtrip from any k slices, bit-exact single-slice repair, params-versus-
//! reference repair plan equality, padding bounds, and parser acceptance.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use tape_slicer::{
    derive_stripe_size, num_stripes, ErasureCoder, SliceIndex, SliceMetadata, Slicer, STRIPE_CAP,
};

/// Default Clay profile encode granularity (k * alpha * 2).
const ALIGN: usize = 1_400;

/// Snapshot symbol payload size: 4 MiB plus the chunk number header.
const SNAPSHOT_PACKED: usize = 4 * 1024 * 1024 + 8;

const ROUNDTRIP_SIZES: &[usize] = &[
    1, 247, 1_399, 1_400, 1_401, 99_999, 100_000, 100_001, 250_000, 999_999, 1_000_000,
    1_000_001, 1_999_999, 2_000_001, SNAPSHOT_PACKED,
];

const REPAIR_SIZES: &[usize] = &[1_400, 100_001, 250_000, 1_000_001, SNAPSHOT_PACKED];

/// Production cap, plus a smaller one that pushes the same blobs into more
/// stripes.
const CAPS: [usize; 2] = [STRIPE_CAP, 350_000];

fn slicer_at(cap: usize) -> Slicer<tape_slicer::ClayCoder> {
    let mut slicer = Slicer::clay_default();
    slicer.set_stripe_cap(cap);
    slicer
}

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn subset<'a>(slices: &'a [Vec<u8>], keep: &[usize]) -> Vec<(usize, &'a [u8])> {
    keep.iter().map(|&i| (i, slices[i].as_slice())).collect()
}

// decode(encode(blob)) == blob from all slices, from the first and last k,
// and from seeded random k-subsets, at every cap
#[test]
fn roundtrip_from_any_k_slices() {
    let mut rng = StdRng::seed_from_u64(69);

    for cap in CAPS {
        let mut slicer = slicer_at(cap);
        let n = slicer.n();
        let k = slicer.k();

        for &len in ROUNDTRIP_SIZES {
            let payload = mk(len);
            let slices = slicer.encode(&payload).unwrap();
            assert_eq!(slices.len(), n, "cap {cap} len {len}");
            let first = slices[0].len();
            assert!(
                slices.iter().all(|s| s.len() == first),
                "cap {cap} len {len}: non-uniform slices"
            );

            let mut picks: Vec<Vec<usize>> = vec![
                (0..n).collect(),
                (0..k).collect(),
                (n - k..n).collect(),
            ];
            let random_subsets = if len <= 1_000_000 { 3 } else { 1 };
            for _ in 0..random_subsets {
                let mut idx: Vec<usize> = (0..n).collect();
                idx.shuffle(&mut rng);
                idx.truncate(k);
                idx.sort_unstable();
                picks.push(idx);
            }

            for keep in picks {
                let restored = slicer.decode(&subset(&slices, &keep)).unwrap();
                assert_eq!(
                    restored, payload,
                    "cap {cap} len {len}: roundtrip failed from {keep:?}"
                );
            }
        }
    }
}

// empty blobs survive at every cap
#[test]
fn roundtrip_empty_blob() {
    for cap in CAPS {
        let mut slicer = slicer_at(cap);
        let slices = slicer.encode(&[]).unwrap();
        let refs: Vec<(usize, &[u8])> = slices.iter().enumerate().map(|(i, s)| (i, s.as_slice())).collect();
        assert_eq!(slicer.decode(&refs).unwrap(), Vec::<u8>::new(), "cap {cap}");
    }
}

// every repaired slice is bit-exact against the original, at every cap
#[test]
fn repair_is_bit_exact() {
    for cap in CAPS {
        let mut slicer = slicer_at(cap);
        let n = slicer.n();

        for &len in REPAIR_SIZES {
            let payload = mk(len);
            let slices = slicer.encode(&payload).unwrap();

            let lost_set: Vec<usize> = if len <= 100_001 { (0..n).collect() } else { vec![0, 7, 13, 19] };
            for lost in lost_set {
                let helpers: Vec<(SliceIndex, &[u8])> = slices
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != lost)
                    .map(|(i, s)| (SliceIndex::new(i), s.as_slice()))
                    .collect();

                let repaired = slicer.repair_full(SliceIndex::new(lost), &helpers).unwrap();
                assert_eq!(
                    repaired, slices[lost],
                    "cap {cap} len {len}: repair of slice {lost} not bit-exact"
                );
            }
        }
    }
}

// the plan the node computes from stored params equals the plan computed from
// a reference slice; this is the live repair path's correctness dependency
#[test]
fn repair_plan_params_match_reference() {
    for cap in CAPS {
        let mut slicer = slicer_at(cap);
        let n = slicer.n();

        for &len in REPAIR_SIZES {
            let payload = mk(len);
            let slices = slicer.encode(&payload).unwrap();
            let stripe_size = slicer.stripe_size();

            let available: Vec<SliceIndex> = (1..n).map(SliceIndex::new).collect();
            let ref_plan = slicer
                .repair_plan(SliceIndex::new(0), &available, &slices[1])
                .unwrap();
            let param_plan = slicer
                .repair_plan_from_params(SliceIndex::new(0), &available, len, stripe_size)
                .unwrap();

            assert_eq!(ref_plan.num_stripes, param_plan.num_stripes, "cap {cap} len {len}");
            assert_eq!(ref_plan.chunk_size, param_plan.chunk_size, "cap {cap} len {len}");
            assert_eq!(ref_plan.sub_chunk_size, param_plan.sub_chunk_size, "cap {cap} len {len}");
            for (r, p) in ref_plan.stripes.iter().zip(param_plan.stripes.iter()) {
                assert_eq!(r.lost_shard, p.lost_shard);
                assert_eq!(r.helpers.len(), p.helpers.len());
                for (rh, ph) in r.helpers.iter().zip(p.helpers.iter()) {
                    assert_eq!(rh.slice, ph.slice);
                    assert_eq!(rh.shard, ph.shard);
                    assert_eq!(rh.sub_chunks, ph.sub_chunks);
                }
            }
        }
    }
}

// stripes waste less than one alignment unit each and never exceed the aligned
// cap, across the whole track range
#[test]
fn padding_and_range_bounds() {
    let cap_aligned = STRIPE_CAP.div_ceil(ALIGN) * ALIGN;
    let mut sizes: Vec<usize> = ROUNDTRIP_SIZES.to_vec();
    for boundary in (1_000_000..=67_108_864usize).step_by(1_000_000) {
        sizes.push(boundary);
        sizes.push(boundary + 1);
    }

    for len in sizes {
        let stripe = derive_stripe_size(len, ALIGN, STRIPE_CAP);
        let count = num_stripes(len, stripe);
        assert!(stripe >= ALIGN);
        assert!(stripe <= cap_aligned, "len {len}: stripe {stripe} above cap");
        assert!(stripe.is_multiple_of(ALIGN), "len {len}: stripe {stripe} unaligned");
        assert!(
            count * stripe >= len && count * stripe - len < count * ALIGN,
            "len {len}: padding bound violated (stripe {stripe}, count {count})"
        );
    }
}

// the parser takes what a writer emits and nothing else, so a stripe size the
// blob length does not derive cannot enter the decode path
#[test]
fn parser_takes_writer_output_only() {
    for &len in &[100_001usize, 1_000_001, SNAPSHOT_PACKED] {
        let payload = mk(len);
        let mut writer = Slicer::clay_default();
        let meta = SliceMetadata::parse(&writer.encode(&payload).unwrap()[0]).unwrap();

        assert_eq!(meta.stripe_size(), derive_stripe_size(len, ALIGN, STRIPE_CAP));

        let mut raw = vec![0u8; 64];
        raw.extend_from_slice(&meta.to_bytes());
        assert!(SliceMetadata::from_slice(&raw).is_ok());

        for off_scheme in [0u64, 100_000, 999_999, 1_000_000, 10_000_000] {
            let mut forged = meta;
            forged.stripe_size = off_scheme;
            let mut raw = vec![0u8; 64];
            raw.extend_from_slice(&forged.to_bytes());
            assert!(
                SliceMetadata::from_slice(&raw).is_err(),
                "len {len}: parser accepted off-scheme stripe {off_scheme}"
            );
        }
    }
}

// unbounded writer-chosen sizing would let a hostile writer explode the stripe
// count; deriving from the blob caps it, and the parser rejects the rest
#[test]
fn hostile_stripe_count_rejected() {
    let hostile = num_stripes(SNAPSHOT_PACKED, ALIGN);
    let derived = num_stripes(
        SNAPSHOT_PACKED,
        derive_stripe_size(SNAPSHOT_PACKED, ALIGN, STRIPE_CAP),
    );

    assert!(hostile > 2_900, "hostile stripe count: {hostile}");
    assert_eq!(derived, 5);

    // the mechanics still work at one stripe per alignment unit, so only the
    // parser stands between a hostile writer and per-track resource blowup
    let mut slicer = slicer_at(ALIGN);
    let payload = mk(100_001);
    let slices = slicer.encode(&payload).unwrap();
    assert!(num_stripes(payload.len(), slicer.stripe_size()) > 70);

    let refs: Vec<(usize, &[u8])> = slices.iter().enumerate().map(|(i, s)| (i, s.as_slice())).collect();
    assert_eq!(slicer.decode(&refs).unwrap(), payload);
    assert!(SliceMetadata::from_slice(&slices[0]).is_err());
}
