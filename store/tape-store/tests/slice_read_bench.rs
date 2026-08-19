//! Read-path microbenchmark: keys-only scan vs value-reading scan over the
//! `slice` column family, swept across slice sizes. This is what
//! `count_slices_by_spool` / `iter_slice_keys_by_spool` do under the hood, so it
//! measures the win from `iter_keys_prefix` not materializing (and, for a store
//! that indirects large values, not dereferencing) them.
//!
//! Three arms, one store layout each: RocksDB's split meta/bulk layout, the
//! public reel serving every family, and the layout a node would run the reel in,
//! RocksDB metadata beside a reel holding the bulk families.
//!
//! Caches are warm on all three (data was just written and settled), so this
//! reflects the memory-resident case; a node under memory pressure would see a
//! larger gap on the value-reading path. Ignored by default. Run with:
//!   cargo test -p tape-store --test slice_read_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel_store::{scaled, BenchArm, MetaBulkStore, ReelStore};
use store::Column;
use store_rocks::SplitStore;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::columns::SliceCol;
use tape_store::ops::SliceOps;
use tape_store::types::SliceKey;
use tempfile::TempDir;

/// An incompressible payload, so a value-reading scan is charged for the bytes
///
/// A repeating fill compresses away and the arm that compresses then reads a
/// fraction of what the other one does, which measures the codec rather than the
/// scan.
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

/// (slice size in bytes, number of slices). The 256 KiB threshold splits inline
/// (SST) values from blob-file values. These model *fat spools* — tens of
/// thousands of slices — where the per-entry value-materialization cost the
/// keys-only path skips accumulates. Large blob sizes can't reach the same
/// counts warm (data volume), so blobs are shown at a lower-but-still-fat count.
const CASES: &[(usize, usize)] = &[
    (16 * 1024, 25_000),  // fat inline spool  (~390 MiB)
    (16 * 1024, 100_000), // very fat inline   (~1.5 GiB)
    (64 * 1024, 25_000),  // fatter inline     (~1.5 GiB)
    (256 * 1024, 8_000),  // fat blob spool    (~2 GiB)
];

/// Best (min) elapsed over a few rounds, to damp scheduler noise.
fn best<F: FnMut() -> usize>(mut scan: F, expect: usize, rounds: u32) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..rounds {
        let t = Instant::now();
        let n = scan();
        let dt = t.elapsed();
        assert_eq!(n, expect);
        best = best.min(dt);
    }
    best
}

fn sweep<A: BenchArm>() {
    let spool = SpoolIndex(7);
    let prefix = SliceKey::spool_prefix(spool);

    println!(
        "{:>7}  {:>8}  {:>6}  {:>9}  {:>13}  {:>11}  {:>8}  {:>11}",
        "engine", "size", "count", "total", "value-reading", "keys-only", "speedup", "saved/scan"
    );

    for &(size, count) in CASES {
        let count = scaled(count);
        // Fresh store per case so earlier cases don't pollute caches/compaction.
        let dir = TempDir::new().unwrap();
        let store = A::open_bench(&dir.path().join("db"));
        let data = payload(size);
        for _ in 0..count {
            store
                .put_slice(spool, Address::new_unique(), data.clone())
                .unwrap();
        }
        A::settle(&store);
        let raw = store.inner().inner();

        let value_reading = best(
            || raw.iter_prefix(SliceCol::CF_NAME, &prefix).unwrap().count(),
            count,
            3,
        );
        let keys_only = best(
            || raw.iter_keys_prefix(SliceCol::CF_NAME, &prefix).unwrap().len(),
            count,
            3,
        );

        let speedup = value_reading.as_secs_f64() / keys_only.as_secs_f64().max(f64::MIN_POSITIVE);
        let saved = value_reading.saturating_sub(keys_only);
        let total_mib = (size * count) as f64 / (1024.0 * 1024.0);
        let size_label = if size >= 1024 * 1024 {
            format!("{} MiB", size / (1024 * 1024))
        } else {
            format!("{} KiB", size / 1024)
        };
        let engine = A::NAME;
        println!(
            "{engine:>7}  {size_label:>8}  {count:>6}  {total_mib:>7.1} MiB  \
             {value_reading:>13.2?}  {keys_only:>11.2?}  {speedup:>7.1}x  {saved:>11.2?}",
        );
    }
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn keys_only_vs_value_reading_scan_rocks() {
    sweep::<SplitStore>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn keys_only_vs_value_reading_scan_reel() {
    sweep::<ReelStore>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn keys_only_vs_value_reading_scan_split() {
    sweep::<MetaBulkStore>();
}
