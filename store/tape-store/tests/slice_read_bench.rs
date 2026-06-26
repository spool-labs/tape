//! Read-path microbenchmark: keys-only scan vs value-reading scan over the
//! blob-backed `slice` column family, swept across slice sizes. This is what
//! `count_slices_by_spool` / `iter_slice_keys_by_spool` do under the hood, so it
//! measures the win from `iter_keys_prefix` not materializing (and, for blobs,
//! not dereferencing) values.
//!
//! Caches are warm (data was just written+flushed), so this reflects the
//! memory-resident case; a node under memory pressure with on-disk blobs would
//! see a larger gap on the value-reading path. Ignored by default. Run with:
//!   cargo test -p tape-store --test slice_read_bench -- --ignored --nocapture

use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::columns::SliceCol;
use tape_store::ops::SliceOps;
use tape_store::types::SliceKey;
use tape_store::TapeStore;
use tempfile::TempDir;

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

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn keys_only_vs_value_reading_scan() {
    let spool = SpoolIndex(7);
    let prefix = SliceKey::spool_prefix(spool);

    println!(
        "{:>8}  {:>6}  {:>9}  {:>13}  {:>11}  {:>8}  {:>11}",
        "size", "count", "total", "value-reading", "keys-only", "speedup", "saved/scan"
    );

    for &(size, count) in CASES {
        // Fresh store per case so earlier cases don't pollute caches/compaction.
        let dir = TempDir::new().unwrap();
        let store = TapeStore::open_primary(dir.path().join("db")).unwrap();
        for _ in 0..count {
            store
                .put_slice(spool, Address::new_unique(), vec![0xAB; size])
                .unwrap();
        }
        let raw = store.inner().inner();
        raw.flush().unwrap();

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
        println!(
            "{size_label:>8}  {count:>6}  {total_mib:>7.1} MiB  {value_reading:>13.2?}  {keys_only:>11.2?}  {speedup:>7.1}x  {saved:>11.2?}",
        );
    }
}
