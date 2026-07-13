//! Write-path microbenchmark for the slice size index.
//!
//! Maintaining the index means a slice write commits a two-column batch instead
//! of a single put, which must not cost the payload an extra copy. Four variants
//! pin down where any cost lands: the old single put, the batched write, the two
//! writes unbatched, and a batch carrying only the payload. The last one shows
//! whether the batch machinery or the extra column is responsible.
//!
//! The read side measures what the change buys: summing payload lengths from
//! the index versus reading every blob back, which is what the stats handler
//! used to do.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test slice_write_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use store::{Column, Store};
use tape_core::types::{SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_store::columns::{SliceCol, SliceSizeCol};
use tape_store::ops::SliceOps;
use tape_store::types::{SliceKey, SliceValue};
use tape_store::TapeStore;
use tempfile::TempDir;

/// (slice size in bytes, number of slices). 256 KiB is the blob threshold, so
/// the sweep straddles inline values and blob-file values. Counts hold each
/// case near a constant 128 MiB of payload so sizes stay comparable.
const CASES: &[(usize, usize)] = &[
    (4 * 1024, 8_000),
    (16 * 1024, 4_000),
    (64 * 1024, 2_000),
    (128 * 1024, 1_000),
    (256 * 1024, 512),
    (512 * 1024, 256),
    (1024 * 1024, 128),
    (2 * 1024 * 1024, 64),
    (4 * 1024 * 1024, 32),
    // The protocol slice ceiling (SLICE_BYTES_LIMIT), which is also wincode's
    // decode preallocation default; nothing larger can enter or leave a store.
    (10 * 1024 * 1024, 12),
];

fn size_label(size: usize) -> String {
    if size >= 1024 * 1024 {
        format!("{} MiB", size / (1024 * 1024))
    } else {
        format!("{} KiB", size / 1024)
    }
}

/// Time only the store call, keeping payload cloning out of the measurement.
fn timed_writes<F: FnMut(Address, Vec<u8>)>(count: usize, size: usize, mut write: F) -> Duration {
    let payload = vec![0xABu8; size];
    let mut total = Duration::ZERO;
    for _ in 0..count {
        let data = payload.clone();
        let address = Address::new_unique();

        let start = Instant::now();
        write(address, data);
        total += start.elapsed();
    }
    total
}

/// Variants under test, indexed so rounds can rotate their order.
const VARIANTS: usize = 4;

/// Rounds per case. Each variant gets its own store and the order rotates, so
/// no variant permanently eats the cold-cache or background-compaction cost of
/// running first. Report the min, which is the least noisy estimator here.
const ROUNDS: usize = 6;

fn run_variant(variant: usize, spool: SpoolIndex, size: usize, count: usize) -> Duration {
    // A private directory per variant per round: neighbours must not share a
    // page cache or a compaction backlog.
    let dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(dir.path().join("db")).expect("open primary");

    let elapsed = match variant {
        // Old path: one put into the slice column, no index.
        0 => timed_writes(count, size, |address, data| {
            let key = SliceKey::new(spool, address);
            store
                .inner()
                .put::<SliceCol>(&key, &SliceValue(data))
                .expect("put slice");
        }),
        // New path: both columns in one batch.
        1 => timed_writes(count, size, |address, data| {
            store.put_slice(spool, address, data).expect("put slice");
        }),
        // The same two writes, unbatched.
        2 => timed_writes(count, size, |address, data| {
            let key = SliceKey::new(spool, address);
            store
                .inner()
                .put::<SliceCol>(&key, &SliceValue(data))
                .expect("put slice");
            store
                .inner()
                .put::<SliceSizeCol>(&key, &StorageUnits(size as u64))
                .expect("put size");
        }),
        // A batch carrying only the payload: isolates the batch machinery from
        // the cost of the extra column.
        _ => timed_writes(count, size, |address, data| {
            let key = SliceKey::new(spool, address);
            let key_bytes = wincode::serialize(&key).expect("key");
            let value_bytes = wincode::serialize(&SliceValue(data)).expect("value");
            let mut batch = store::WriteBatch::new();
            batch.put_owned(SliceCol::CF_NAME, key_bytes, value_bytes);
            store.inner().inner().write_batch(batch).expect("write batch");
        }),
    };

    // Settle the write before the next variant opens its store.
    store.inner().inner().flush().expect("flush");
    elapsed
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn index_write_overhead() {
    let spool = SpoolIndex(7);

    println!(
        "{:>8}  {:>6}  {:>11}  {:>11}  {:>11}  {:>11}  {:>9}",
        "size", "count", "single put", "batched", "two puts", "batch-only", "overhead"
    );

    for &(size, count) in CASES {
        let mut best = [Duration::MAX; VARIANTS];
        for round in 0..ROUNDS {
            for offset in 0..VARIANTS {
                let variant = (round + offset) % VARIANTS;
                let elapsed = run_variant(variant, spool, size, count);
                best[variant] = best[variant].min(elapsed);
            }
        }

        let overhead = best[1].as_secs_f64() / best[0].as_secs_f64().max(f64::MIN_POSITIVE);
        let per_op = |elapsed: Duration| elapsed / count as u32;
        println!(
            "{:>8}  {count:>6}  {:>11.2?}  {:>11.2?}  {:>11.2?}  {:>11.2?}  {overhead:>8.2}x",
            size_label(size),
            per_op(best[0]),
            per_op(best[1]),
            per_op(best[2]),
            per_op(best[3]),
        );
    }
}

/// (slice size, count) for the delete sweep. Point-delete cost is dominated by
/// count, but payload size still decides whether tombstones land beside blobs.
const DELETE_CASES: &[(usize, usize)] = &[
    (16 * 1024, 4_000),
    (256 * 1024, 512),
    (1024 * 1024, 128),
];

/// Fill a fresh store and hand back the keys, so a delete pass can be timed alone.
fn populate(
    store: &TapeStore<store_rocks::SplitStore>,
    spool: SpoolIndex,
    size: usize,
    count: usize,
) -> Vec<Address> {
    let mut addresses = Vec::with_capacity(count);
    for _ in 0..count {
        let address = Address::new_unique();
        store.put_slice(spool, address, vec![0xABu8; size]).expect("put slice");
        addresses.push(address);
    }
    store.inner().inner().flush().expect("flush");
    addresses
}

fn run_delete_variant(variant: usize, spool: SpoolIndex, size: usize, count: usize) -> Duration {
    let dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(dir.path().join("db")).expect("open primary");
    let addresses = populate(&store, spool, size, count);

    let mut total = Duration::ZERO;
    for address in addresses {
        let key = SliceKey::new(spool, address);

        let start = Instant::now();
        match variant {
            // Old path: drop the payload, no index to maintain.
            0 => {
                store.inner().delete::<SliceCol>(&key).expect("delete slice");
            }
            // New path: both tombstones in one batch.
            1 => {
                store.delete_slice(spool, address).expect("delete slice");
            }
            // The same two tombstones, unbatched.
            _ => {
                store.inner().delete::<SliceCol>(&key).expect("delete slice");
                store.inner().delete::<SliceSizeCol>(&key).expect("delete size");
            }
        }
        total += start.elapsed();
    }
    total
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn index_delete_overhead() {
    let spool = SpoolIndex(7);

    println!(
        "{:>8}  {:>6}  {:>13}  {:>13}  {:>13}  {:>9}",
        "size", "count", "single delete", "batched", "two deletes", "overhead"
    );

    for &(size, count) in DELETE_CASES {
        let mut best = [Duration::MAX; 3];
        for round in 0..ROUNDS {
            for offset in 0..3 {
                let variant = (round + offset) % 3;
                best[variant] = best[variant].min(run_delete_variant(variant, spool, size, count));
            }
        }

        let overhead = best[1].as_secs_f64() / best[0].as_secs_f64().max(f64::MIN_POSITIVE);
        let per_op = |elapsed: Duration| elapsed / count as u32;
        println!(
            "{:>8}  {count:>6}  {:>13.2?}  {:>13.2?}  {:>13.2?}  {overhead:>8.2}x",
            size_label(size),
            per_op(best[0]),
            per_op(best[1]),
            per_op(best[2]),
        );
    }
}

/// Time one whole-spool delete against a freshly filled store.
fn run_spool_delete(variant: usize, spool: SpoolIndex, size: usize, count: usize) -> Duration {
    let dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(dir.path().join("db")).expect("open primary");
    populate(&store, spool, size, count);

    let start = Instant::now();
    if variant == 0 {
        // Old path: a single range tombstone over the payload column.
        let (range_start, range_end) = SliceKey::spool_key_range(spool);
        let end = range_end.expect("spool below the max prefix");
        store
            .inner()
            .inner()
            .delete_range(SliceCol::CF_NAME, &range_start, &end)
            .expect("delete range");
    } else {
        store.delete_all_slices_for_spool(spool).expect("delete spool");
    }
    start.elapsed()
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn spool_delete_overhead() {
    let spool = SpoolIndex(7);

    println!(
        "{:>8}  {:>6}  {:>14}  {:>15}  {:>9}",
        "size", "count", "one tombstone", "two tombstones", "overhead"
    );

    for &(size, count) in DELETE_CASES {
        let mut best = [Duration::MAX; 2];
        for round in 0..ROUNDS {
            for offset in 0..2 {
                let variant = (round + offset) % 2;
                best[variant] = best[variant].min(run_spool_delete(variant, spool, size, count));
            }
        }

        let overhead = best[1].as_secs_f64() / best[0].as_secs_f64().max(f64::MIN_POSITIVE);
        println!(
            "{:>8}  {count:>6}  {:>14.2?}  {:>15.2?}  {overhead:>8.2}x",
            size_label(size),
            best[0],
            best[1],
        );
    }
}

/// The max spool prefix has no exclusive successor, so it falls back to
/// collecting keys and batch-deleting them. That path now stages two tombstones
/// per slice instead of one.
#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn max_spool_fallback_delete() {
    let spool = SpoolIndex(u16::MAX as u64);

    println!(
        "{:>8}  {:>6}  {:>13}  {:>13}  {:>9}",
        "size", "count", "one column", "two columns", "overhead"
    );

    for &(size, count) in DELETE_CASES {
        let mut best = [Duration::MAX; 2];
        for round in 0..ROUNDS {
            for offset in 0..2 {
                let variant = (round + offset) % 2;
                let dir = TempDir::new().expect("tempdir");
                let store = TapeStore::open_primary(dir.path().join("db")).expect("open primary");
                populate(&store, spool, size, count);
                let raw = store.inner().inner();
                let (range_start, _) = SliceKey::spool_key_range(spool);

                let start = Instant::now();
                if variant == 0 {
                    // Old fallback: one tombstone per slice, payload column only.
                    let keys = raw.iter_keys_prefix(SliceCol::CF_NAME, &range_start).expect("keys");
                    let mut batch = store::WriteBatch::new();
                    for key in &keys {
                        batch.delete(SliceCol::CF_NAME, key);
                    }
                    raw.write_batch(batch).expect("write batch");
                } else {
                    store.delete_all_slices_for_spool(spool).expect("delete spool");
                }
                best[variant] = best[variant].min(start.elapsed());
            }
        }

        let overhead = best[1].as_secs_f64() / best[0].as_secs_f64().max(f64::MIN_POSITIVE);
        println!(
            "{:>8}  {count:>6}  {:>13.2?}  {:>13.2?}  {overhead:>8.2}x",
            size_label(size),
            best[0],
            best[1],
        );
    }
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn totals_read_speedup() {
    let spool = SpoolIndex(7);
    let prefix = SliceKey::spool_prefix(spool);

    println!(
        "{:>8}  {:>6}  {:>9}  {:>14}  {:>11}  {:>8}",
        "size", "count", "total", "blob-reading", "size index", "speedup"
    );

    for &(size, count) in CASES {
        let dir = TempDir::new().expect("tempdir");
        let store = TapeStore::open_primary(dir.path().join("db")).expect("open primary");
        for _ in 0..count {
            store
                .put_slice(spool, Address::new_unique(), vec![0xAB; size])
                .expect("put slice");
        }
        let raw = store.inner().inner();
        raw.flush().expect("flush");

        // What the handler used to do: pull every payload back to sum lengths.
        let start = Instant::now();
        let mut scanned = 0u64;
        for (_, value_bytes) in raw.iter_prefix(SliceCol::CF_NAME, &prefix).expect("iter") {
            let value: SliceValue = wincode::deserialize(&value_bytes).expect("decode slice");
            scanned += value.0.len() as u64;
        }
        let blob_reading = start.elapsed();

        let start = Instant::now();
        let (indexed_count, indexed_bytes) = store.slice_totals_by_spool(spool).expect("totals");
        let size_index = start.elapsed();

        assert_eq!(indexed_count, count as u64);
        assert_eq!(indexed_bytes, StorageUnits(scanned));

        let speedup = blob_reading.as_secs_f64() / size_index.as_secs_f64().max(f64::MIN_POSITIVE);
        let total_mib = (size * count) as f64 / (1024.0 * 1024.0);
        println!(
            "{:>8}  {count:>6}  {total_mib:>7.1} MiB  {blob_reading:>14.2?}  {size_index:>11.2?}  {speedup:>7.1}x",
            size_label(size),
        );
    }
}
