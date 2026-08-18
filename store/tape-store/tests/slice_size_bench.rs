//! Does a bigger track cost more per byte to read back out of the store?
//!
//! Slices land in the `slice` column family, so a track of size T at Clay k=7
//! becomes 20 slices of about T/7 each. This sweeps that slice size across the
//! track sizes under discussion and reports per-byte read throughput, so a flat
//! MB/s column means track size is neutral to the store.
//!
//! Each case writes the same total volume, so a slower per-byte number is the
//! value size and not the dataset. Payload is pseudorandom because a repeating
//! fill compresses to nothing and would measure nothing.
//!
//! Three arms, one store layout each: RocksDB's split meta/bulk layout, the
//! public reel serving every family, and the layout a node would run the reel in,
//! RocksDB metadata beside a reel holding the bulk families.
//!
//! Caches are warm on all three. Ignored by default. Run with:
//!   cargo test -p tape-store --test slice_size_bench --release -- --ignored --nocapture

use std::time::Instant;

use reel_bridge::{scaled, BenchArm, MetaBulkStore, ReelBridge};
use store_rocks::SplitStore;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::SliceOps;
use tempfile::TempDir;

/// Slice sizes for a Clay k=7 track, measured off a real 64 MiB encode and
/// scaled, so the 64 MiB row is the value a node holds today. Nothing larger
/// fits: SLICE_BYTES_LIMIT caps a slice at 10 MiB and the 64 MiB row is already
/// 9.27 of it, so a 128 MiB track cannot be read back at all.
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

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn read_cost_by_slice_size_rocks() {
    sweep::<SplitStore>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn read_cost_by_slice_size_reel() {
    sweep::<ReelBridge>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn read_cost_by_slice_size_split() {
    sweep::<MetaBulkStore>();
}
