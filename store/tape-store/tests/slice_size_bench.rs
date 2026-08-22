#![cfg(feature = "rocks")]

//! Does a bigger track cost more per byte to read back out of the store?
//!
//! A track of size T at Clay k=7 becomes 20 slices of about T/7. This sweeps the
//! slice size across the track sizes under discussion and reports per-byte read
//! throughput, so a flat MB/s column means track size is neutral to the store.
//! Every case writes the same total volume, so a slower per-byte number is the
//! value size and not the dataset.
//!
//! Three arms, one store layout each, caches warm on all of them. Ignored by
//! default. Run with `--ignored --nocapture` on a release build.

use std::time::Instant;

use reel_store::{scaled, BenchArm, MetaBulkStore, ReelStore};
use store_rocks::SplitStore;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::SliceOps;
use tempfile::TempDir;

/// Slice sizes for a Clay k=7 track, scaled off a real 64 MiB encode
///
/// Nothing larger fits: the slice byte limit caps a slice at 10 MiB and the
/// 64 MiB row is already most of it.
const CASES: &[(&str, usize)] = &[
    ("16 MiB track", 9_724_048 / 4),
    ("32 MiB track", 9_724_048 / 2),
    ("64 MiB track", 9_724_048),
];

/// Bytes written per case, so every row reads the same volume back.
const VOLUME: usize = 3 * 1024 * 1024 * 1024;

fn payload(size: usize) -> Vec<u8> {
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut out = Vec::with_capacity(size + 8);
    while out.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(size);
    out
}

fn sweep<A: BenchArm>() {
    println!(
        "{:<7} {:<14} {:>10} {:>7} {:>10} {:>9} {:>9} {:>9}",
        "engine", "track", "slice", "count", "write MB/s", "seq MB/s", "rnd MB/s", "rnd ms/get"
    );

    for &(label, size) in CASES {
        let count = (scaled(VOLUME) / size).max(1);
        let spool = SpoolIndex(7);
        let data = payload(size);

        let dir = TempDir::new().unwrap();
        let store = A::open_bench(&dir.path().join("db"));

        let mut addrs: Vec<Address> = (0..count).map(|_| Address::new_unique()).collect();

        let start = Instant::now();
        for addr in &addrs {
            store.put_slice(spool, *addr, data.clone()).unwrap();
        }
        A::settle(&store);
        let write_mbs = (count * size) as f64 / start.elapsed().as_secs_f64() / 1e6;

        // Sequential in key order, which is what a spool scan does.
        addrs.sort();
        let start = Instant::now();
        let mut read = 0usize;
        for addr in &addrs {
            read += store.get_slice(spool, *addr).unwrap().unwrap().len();
        }
        let seq_mbs = read as f64 / start.elapsed().as_secs_f64() / 1e6;

        // Shuffled, which is what serving unrelated tracks looks like.
        let mut state = 0x243F_6A88_85A3_08D3_u64;
        for i in (1..addrs.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            addrs.swap(i, (state % (i as u64 + 1)) as usize);
        }
        let start = Instant::now();
        let mut read = 0usize;
        for addr in &addrs {
            read += store.get_slice(spool, *addr).unwrap().unwrap().len();
        }
        let elapsed = start.elapsed();
        let rnd_mbs = read as f64 / elapsed.as_secs_f64() / 1e6;
        let rnd_ms = elapsed.as_secs_f64() * 1000.0 / count as f64;

        let engine = A::NAME;
        println!(
            "{engine:<7} {label:<14} {size:>10} {count:>7} {write_mbs:>10.0} {seq_mbs:>9.0} \
             {rnd_mbs:>9.0} {rnd_ms:>9.2}"
        );
    }
}

// what a slice read costs rocks as the slice grows
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn slice_size_rocks() {
    sweep::<SplitStore>();
}

// what a slice read costs the reel as the slice grows
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn slice_size_reel() {
    sweep::<ReelStore>();
}

// what a slice read costs the split arm as the slice grows
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn slice_size_split() {
    sweep::<MetaBulkStore>();
}
