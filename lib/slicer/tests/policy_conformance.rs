//! Policy-generic conformance battery (issue 69).
//!
//! Runs identical invariants over each candidate stripe sizing scheme, so a
//! sizing change is a config swap under a fixed test surface: roundtrip from
//! any k slices, bit-exact single-slice repair, params-versus-reference
//! repair plan equality, padding bounds, and the fleet rollout matrix.

use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;

use tape_slicer::{
    derive_stripe_size, num_stripes, ErasureCoder, SliceIndex, SliceMetadata, Slicer,
    StripePolicy, StripeValidation, DERIVED_STRIPE_CAP, STRIPE_SIZES,
};

/// Default Clay profile encode granularity (k * alpha * 2).
const ALIGN: usize = 1_400;

/// Stripe ladder the deployed fleet still validates against.
const FLEET_LADDER: [usize; 3] = [100_000, 1_000_000, 10_000_000];

/// Snapshot symbol payload size: 4 MiB plus the chunk number header.
const SNAPSHOT_PACKED: usize = 4 * 1024 * 1024 + 8;

const ROUNDTRIP_SIZES: &[usize] = &[
    1, 247, 1_399, 1_400, 1_401, 99_999, 100_000, 100_001, 250_000, 999_999, 1_000_000,
    1_000_001, 1_999_999, 2_000_001, SNAPSHOT_PACKED,
];

const REPAIR_SIZES: &[usize] = &[1_400, 100_001, 250_000, 1_000_001, SNAPSHOT_PACKED];

struct Scheme {
    name: &'static str,
    policy: StripePolicy,
    validation: StripeValidation,
}

fn schemes() -> Vec<Scheme> {
    vec![
        Scheme {
            name: "ladder",
            policy: StripePolicy::Adaptive,
            validation: StripeValidation::LadderOnly,
        },
        Scheme {
            name: "derived",
            policy: StripePolicy::Derived { alignment: ALIGN, cap: DERIVED_STRIPE_CAP },
            validation: StripeValidation::LadderOrDerived {
                alignment: ALIGN,
                cap: DERIVED_STRIPE_CAP,
            },
        },
        Scheme {
            name: "writer-chosen",
            policy: StripePolicy::Fixed(350_000),
            validation: StripeValidation::LadderOrExact(350_000),
        },
    ]
}

fn slicer_for(scheme: &Scheme) -> Slicer<tape_slicer::ClayCoder> {
    let mut slicer = Slicer::clay_default();
    slicer.set_policy(scheme.policy);
    slicer.set_validation(scheme.validation);
    slicer
}

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn subset<'a>(slices: &'a [Vec<u8>], keep: &[usize]) -> Vec<(usize, &'a [u8])> {
    keep.iter().map(|&i| (i, slices[i].as_slice())).collect()
}

// decode(encode(blob)) == blob from all slices, from the first and last k,
// and from seeded random k-subsets, under every scheme
#[test]
fn roundtrip_from_any_k_slices() {
    let mut rng = StdRng::seed_from_u64(69);

    for scheme in schemes() {
        let mut slicer = slicer_for(&scheme);
        let n = slicer.n();
        let k = slicer.k();

        for &len in ROUNDTRIP_SIZES {
            let payload = mk(len);
            let slices = slicer.encode(&payload).unwrap();
            assert_eq!(slices.len(), n, "{} len {len}", scheme.name);
            let first = slices[0].len();
            assert!(
                slices.iter().all(|s| s.len() == first),
                "{} len {len}: non-uniform slices",
                scheme.name
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
                    "{} len {len}: roundtrip failed from {keep:?}",
                    scheme.name
                );
            }
        }
    }
}

// empty blobs survive under every scheme
#[test]
fn roundtrip_empty_blob() {
    for scheme in schemes() {
        let mut slicer = slicer_for(&scheme);
        let slices = slicer.encode(&[]).unwrap();
        let refs: Vec<(usize, &[u8])> = slices.iter().enumerate().map(|(i, s)| (i, s.as_slice())).collect();
        assert_eq!(slicer.decode(&refs).unwrap(), Vec::<u8>::new(), "{}", scheme.name);
    }
}

// every repaired slice is bit-exact against the original, under every scheme
#[test]
fn repair_is_bit_exact() {
    for scheme in schemes() {
        let mut slicer = slicer_for(&scheme);
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
                    "{} len {len}: repair of slice {lost} not bit-exact",
                    scheme.name
                );
            }
        }
    }
}

// the plan the node computes from stored params equals the plan computed from
// a reference slice, under every scheme; this is the live repair path's
// correctness dependency
#[test]
fn repair_plan_params_match_reference() {
    for scheme in schemes() {
        let mut slicer = slicer_for(&scheme);
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

            assert_eq!(ref_plan.num_stripes, param_plan.num_stripes, "{} len {len}", scheme.name);
            assert_eq!(ref_plan.chunk_size, param_plan.chunk_size, "{} len {len}", scheme.name);
            assert_eq!(ref_plan.sub_chunk_size, param_plan.sub_chunk_size, "{} len {len}", scheme.name);
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

// derived stripes waste less than one alignment unit per stripe, never exceed
// the aligned cap, and never collide with ladder values
#[test]
fn derived_padding_and_range_bounds() {
    let cap_aligned = DERIVED_STRIPE_CAP.div_ceil(ALIGN) * ALIGN;
    let mut sizes: Vec<usize> = ROUNDTRIP_SIZES.to_vec();
    for boundary in (1_000_000..=67_108_864usize).step_by(1_000_000) {
        sizes.push(boundary);
        sizes.push(boundary + 1);
    }

    for len in sizes {
        let stripe = derive_stripe_size(len, ALIGN, DERIVED_STRIPE_CAP);
        let count = num_stripes(len, stripe);
        assert!(stripe >= ALIGN);
        assert!(stripe <= cap_aligned, "len {len}: stripe {stripe} above cap");
        assert!(stripe.is_multiple_of(ALIGN), "len {len}: stripe {stripe} unaligned");
        assert!(
            count * stripe >= len && count * stripe - len < count * ALIGN,
            "len {len}: padding bound violated (stripe {stripe}, count {count})"
        );
        assert!(
            !STRIPE_SIZES.contains(&stripe),
            "len {len}: derived stripe {stripe} collides with the ladder"
        );
    }
}

// rollout matrix: what each parser generation accepts from each writer
// generation; parsers must upgrade before writers switch
#[test]
fn rollout_matrix() {
    let fleet_accepts = |meta: &SliceMetadata| FLEET_LADDER.contains(&meta.stripe_size());
    let upgraded = StripeValidation::LadderOrDerived { alignment: ALIGN, cap: DERIVED_STRIPE_CAP };

    for &len in &[100_001usize, 1_000_001, SNAPSHOT_PACKED] {
        let payload = mk(len);

        let mut ladder_writer = Slicer::clay_default();
        let ladder_meta = SliceMetadata::parse(&ladder_writer.encode(&payload).unwrap()[0]).unwrap();

        let mut derived_writer = Slicer::clay_default();
        derived_writer.set_policy(StripePolicy::Derived { alignment: ALIGN, cap: DERIVED_STRIPE_CAP });
        let derived_meta = SliceMetadata::parse(&derived_writer.encode(&payload).unwrap()[0]).unwrap();

        // today's data keeps working everywhere, before and after upgrade
        assert!(fleet_accepts(&ladder_meta));
        assert!(upgraded.accepts(ladder_meta.stripe_size(), ladder_meta.blob_len()));

        // derived-written data needs the upgraded parser; the fleet parser
        // and today's from_slice both fail closed on it
        assert!(!fleet_accepts(&derived_meta));
        assert!(upgraded.accepts(derived_meta.stripe_size(), derived_meta.blob_len()));
        let mut raw = vec![0u8; 64];
        raw.extend_from_slice(&derived_meta.to_bytes());
        assert!(SliceMetadata::from_slice(&raw).is_err());

        // garbage stays rejected after the upgrade
        assert!(!upgraded.accepts(0, len));
        assert!(!upgraded.accepts(999_999, len));
        assert!(!upgraded.accepts(2_000_000, len));
    }
}

// writer-chosen sizing without bounds lets a hostile writer explode the
// stripe count; derived sizing caps it by construction
#[test]
fn writer_chosen_stripe_count_hazard() {
    let hostile = num_stripes(SNAPSHOT_PACKED, ALIGN);
    let derived = num_stripes(
        SNAPSHOT_PACKED,
        derive_stripe_size(SNAPSHOT_PACKED, ALIGN, DERIVED_STRIPE_CAP),
    );

    assert!(hostile > 2_900, "hostile stripe count: {hostile}");
    assert_eq!(derived, 5);

    // the mechanics still work at pathological sizes, so only validation
    // stands between a hostile writer and per-track resource blowup
    let mut slicer = Slicer::clay_default();
    slicer.set_policy(StripePolicy::Fixed(ALIGN));
    slicer.set_validation(StripeValidation::LadderOrExact(ALIGN));
    let payload = mk(100_001);
    let slices = slicer.encode(&payload).unwrap();
    let refs: Vec<(usize, &[u8])> = slices.iter().enumerate().map(|(i, s)| (i, s.as_slice())).collect();
    assert_eq!(slicer.decode(&refs).unwrap(), payload);
}
