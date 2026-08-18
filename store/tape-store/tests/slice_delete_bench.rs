//! Delete-path microbenchmark: one native range tombstone
//! (`delete_all_slices_for_spool` → `Store::delete_range`) vs the old
//! scan-and-delete-every-key loop, swept by slice count. Measures the
//! synchronous cost of the delete call; actual space reclamation is deferred to
//! compaction in both cases (range tombstone vs N point tombstones).
//!
//! Three arms, one store layout each: RocksDB's split meta/bulk layout, the
//! public reel serving every family, and the layout a node would run the reel in,
//! RocksDB metadata beside a reel holding the bulk families.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test slice_delete_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel_bridge::{scaled, BenchArm, MetaBulkStore, ReelBridge};
use store::Column;
use store_rocks::SplitStore;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::columns::SliceCol;
use tape_store::ops::SliceOps;
use tape_store::types::SliceKey;
use tape_store::TapeStore;
use tempfile::TempDir;

/// (slice size, number of slices). Delete latency is dominated by count, so the
/// sweep is over count at a fixed modest size to keep data volume feasible.
const CASES: &[(usize, usize)] = &[
    (16 * 1024, 1_000),
    (16 * 1024, 10_000),
    (16 * 1024, 50_000),
];

fn populate<A: BenchArm>(store: &TapeStore<A>, spool: SpoolIndex, count: usize, size: usize) {
    let data = vec![0xAB; size];
    for _ in 0..count {
        store
            .put_slice(spool, Address::new_unique(), data.clone())
            .unwrap();
    }
    A::settle(store);
}

fn sweep<A: BenchArm>() {
    let spool = SpoolIndex(7);
    let prefix = SliceKey::spool_prefix(spool);

    println!(
        "{:>7}  {:>8}  {:>7}  {:>9}  {:>12}  {:>13}  {:>9}",
        "engine", "size", "count", "total", "per-key del", "range tomb", "speedup"
    );

    for &(size, count) in CASES {
        let count = scaled(count);
        // Old path: scan the spool's keys, then delete each one.
        let old = {
            let dir = TempDir::new().unwrap();
            let store = A::open_bench(&dir.path().join("db"));
            populate(&store, spool, count, size);
            let raw = store.inner().inner();
            let t = Instant::now();
            let keys: Vec<Vec<u8>> = raw
                .iter_prefix(SliceCol::CF_NAME, &prefix)
                .unwrap()
                .map(|(k, _)| k)
                .collect();
            for key in &keys {
                raw.delete(SliceCol::CF_NAME, key).unwrap();
            }
            let dt = t.elapsed();
            assert_eq!(store.count_slices_by_spool(spool).unwrap(), 0);
            dt
        };

        // New path: a single range tombstone.
        let new = {
            let dir = TempDir::new().unwrap();
            let store = A::open_bench(&dir.path().join("db"));
            populate(&store, spool, count, size);
            let t = Instant::now();
            store.delete_all_slices_for_spool(spool).unwrap();
            let dt = t.elapsed();
            assert_eq!(store.count_slices_by_spool(spool).unwrap(), 0);
            dt
        };

        let speedup = old.as_secs_f64() / new.as_secs_f64().max(f64::MIN_POSITIVE);
        let total_mib = (size * count) as f64 / (1024.0 * 1024.0);
        let size_label = format!("{} KiB", size / 1024);
        let engine = A::NAME;
        println!(
            "{engine:>7}  {size_label:>8}  {count:>7}  {total_mib:>7.1} MiB  {:>12.2?}  \
             {:>13.2?}  {speedup:>8.0}x",
            old, new,
        );
        let _ = Duration::from_secs(0);
    }
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn range_tombstone_vs_per_key_delete_rocks() {
    sweep::<SplitStore>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn range_tombstone_vs_per_key_delete_reel() {
    sweep::<ReelBridge>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn range_tombstone_vs_per_key_delete_split() {
    sweep::<MetaBulkStore>();
}
