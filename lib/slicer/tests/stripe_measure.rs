//! Measurement harness for stripe sizing (issue 69). Run manually:
//!
//!   cargo test -p tape-slicer --release --test stripe_measure -- --ignored --nocapture

use std::time::Instant;

use tape_slicer::{ErasureCoder, SliceIndex, SliceMetadata, Slicer, STRIPE_CAP};

const ALIGN: usize = 1_400;
const MIB: usize = 1024 * 1024;

fn mk(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

fn mbps(bytes: usize, secs: f64) -> f64 {
    bytes as f64 / 1_000_000.0 / secs
}

fn stored_payload(slices: &[Vec<u8>]) -> usize {
    slices.iter().map(|s| s.len() - SliceMetadata::SIZE).sum()
}

// encode throughput as a function of stripe size, at a fixed 32 MiB blob; this
// is what picks the cap
#[test]
#[ignore]
fn measure_stripe_size_throughput() {
    let blob = mk(32 * MIB);
    println!("stripe_size_bytes encode_mbps");

    for units in [71usize, 143, 357, 715, 1_786, 3_572, 7_143, 11_429] {
        let cap = units * ALIGN;
        let mut slicer = Slicer::clay_default();
        slicer.set_stripe_cap(cap);

        slicer.encode(&blob).unwrap();
        let reps = 3;
        let start = Instant::now();
        for _ in 0..reps {
            let slices = slicer.encode(&blob).unwrap();
            std::hint::black_box(&slices);
        }
        let secs = start.elapsed().as_secs_f64() / reps as f64;
        println!("{} {:.1}", slicer.stripe_size(), mbps(blob.len(), secs));
    }
}

// encode and decode speed, stored bytes, and stripe geometry at representative
// blob sizes under the production cap
#[test]
#[ignore]
fn measure_encode_decode() {
    let sizes = [100_001usize, 1_000_001, 4 * MIB + 8, 16 * MIB, 64 * MIB];

    println!("blob_len stripe stripes stored ratio encode_mbps decode_mbps");
    for &len in &sizes {
        let blob = mk(len);
        let ideal = len as f64 * 20.0 / 7.0;

        let mut slicer = Slicer::clay_default();
        assert_eq!(slicer.stripe_cap, STRIPE_CAP);

        slicer.encode(&blob).unwrap();
        let reps = if len >= 16 * MIB { 2 } else { 5 };
        let start = Instant::now();
        for _ in 0..reps {
            std::hint::black_box(slicer.encode(&blob).unwrap());
        }
        let enc_secs = start.elapsed().as_secs_f64() / reps as f64;

        let slices = slicer.encode(&blob).unwrap();
        let stripe = slicer.stripe_size();
        let stripes = len.div_ceil(stripe);
        let stored = stored_payload(&slices);

        let k = slicer.k();
        let refs: Vec<(usize, &[u8])> =
            slices.iter().enumerate().take(k).map(|(i, s)| (i, s.as_slice())).collect();
        let start = Instant::now();
        for _ in 0..reps {
            std::hint::black_box(slicer.decode(&refs).unwrap());
        }
        let dec_secs = start.elapsed().as_secs_f64() / reps as f64;

        println!(
            "{len} {stripe} {stripes} {stored} {:.4} {:.1} {:.1}",
            stored as f64 / ideal,
            mbps(len, enc_secs),
            mbps(len, dec_secs),
        );
    }
}

// single-slice repair cost under the production cap
#[test]
#[ignore]
fn measure_repair() {
    println!("blob_len repair_ms helper_bytes");
    for &len in &[1_000_001usize, 4 * MIB + 8] {
        let blob = mk(len);
        let mut slicer = Slicer::clay_default();
        let slices = slicer.encode(&blob).unwrap();

        let helpers: Vec<(SliceIndex, &[u8])> = slices
            .iter()
            .enumerate()
            .skip(1)
            .map(|(i, s)| (SliceIndex::new(i), s.as_slice()))
            .collect();

        let available: Vec<SliceIndex> = (1..slicer.n()).map(SliceIndex::new).collect();
        let plan = slicer.repair_plan(SliceIndex::new(0), &available, &slices[1]).unwrap();
        let helper_bytes: u64 = plan
            .stripes
            .iter()
            .flat_map(|s| s.helpers.iter())
            .map(|h| h.sub_chunks.len() as u64 * plan.sub_chunk_size)
            .sum();

        let reps = 5;
        let start = Instant::now();
        for _ in 0..reps {
            std::hint::black_box(slicer.repair_full(SliceIndex::new(0), &helpers).unwrap());
        }
        let secs = start.elapsed().as_secs_f64() / reps as f64;

        println!("{len} {:.2} {helper_bytes}", secs * 1000.0);
    }
}
