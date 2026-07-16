//! Split-store overhead benchmark: one RocksDB instance holding every column
//! family vs the split meta/bulk layout on the same disk. Measures open time,
//! bulk slice write and read throughput, and metadata operation latency on an
//! idle store and while slice writes are in flight.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test split_overhead_bench --release -- --ignored --nocapture

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use store::Store;
use store_rocks::{RocksStore, SplitStore};
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{GroupIndex, SpoolIndex, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::config::{create_db_options, create_tape_store_configs};
use tape_store::ops::{SliceOps, TrackOps};
use tape_store::TapeStore;
use tempfile::TempDir;

/// Slice payload size, matching typical erasure-coded slices
const SLICE_SIZE: usize = 1024 * 1024;

/// Slices written in the throughput and concurrent-load phases
const SLICE_COUNT: usize = 512;

/// Metadata operations sampled on the idle store
const IDLE_META_OPS: usize = 2_000;

/// Pause between metadata samples so the loop measures latency, not throughput
const META_PAUSE: Duration = Duration::from_micros(200);

/// Flushing is not part of the store trait, so the two arms provide it here
trait Flush {
    fn flush_store(&self);
}

impl Flush for RocksStore {
    fn flush_store(&self) {
        self.flush().unwrap();
    }
}

impl Flush for SplitStore {
    fn flush_store(&self) {
        self.flush().unwrap();
    }
}

// Incompressible payload, like erasure-coded slice data.
fn random_payload(size: usize) -> Vec<u8> {
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

fn certified_track(tape: Address) -> CompressedTrack {
    CompressedTrack {
        tape,
        key: Hash::new_unique(),
        track_number: TrackNumber(0),
        kind: TrackKind::Coded as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(SLICE_SIZE as u64),
        group: GroupIndex(3),
        value_hash: Hash::new_unique(),
    }
}

// One timed metadata write and read-back pair.
fn sample_meta<S: Store>(store: &TapeStore<S>, put: &mut Vec<Duration>, get: &mut Vec<Duration>) {
    let address = Address::new_unique();
    let info = certified_track(Address::new_unique());
    let t = Instant::now();
    store.put_track(address, info).unwrap();
    put.push(t.elapsed());
    let t = Instant::now();
    assert!(store.get_track(address).unwrap().is_some());
    get.push(t.elapsed());
}

fn stats(mut samples: Vec<Duration>) -> String {
    if samples.is_empty() {
        return "no samples".to_string();
    }
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q).round() as usize];
    format!(
        "p50 {:?} p99 {:?} max {:?} ({} ops)",
        at(0.5),
        at(0.99),
        at(1.0),
        samples.len()
    )
}

fn throughput(slices: usize, elapsed: Duration) -> String {
    let mib = (slices * SLICE_SIZE) as f64 / (1024.0 * 1024.0);
    format!(
        "{mib:.0} MiB in {elapsed:.2?} ({:.0} MiB/s)",
        mib / elapsed.as_secs_f64()
    )
}

fn run_arm<S>(label: &str, store: TapeStore<S>, opened: Duration, payload: &[u8])
where
    S: Store + Flush + Send + Sync + 'static,
{
    let store = Arc::new(store);
    println!("{label}  open {opened:?}");

    // Metadata latency with no other traffic.
    let mut idle_put = Vec::with_capacity(IDLE_META_OPS);
    let mut idle_get = Vec::with_capacity(IDLE_META_OPS);
    for _ in 0..IDLE_META_OPS {
        sample_meta(&store, &mut idle_put, &mut idle_get);
        thread::sleep(META_PAUSE);
    }
    println!("  meta idle    put {}", stats(idle_put));
    println!("               get {}", stats(idle_get));

    // Bulk write throughput, including the flush that makes the data durable.
    let spool = SpoolIndex(7);
    let mut addresses = Vec::with_capacity(SLICE_COUNT);
    let t = Instant::now();
    for _ in 0..SLICE_COUNT {
        let address = Address::new_unique();
        store.put_slice(spool, address, payload.to_vec()).unwrap();
        addresses.push(address);
    }
    store.inner().inner().flush_store();
    println!("  slice write  {}", throughput(SLICE_COUNT, t.elapsed()));

    // Bulk read-back throughput.
    let t = Instant::now();
    for address in &addresses {
        assert!(store.get_slice(spool, *address).unwrap().is_some());
    }
    println!("  slice read   {}", throughput(SLICE_COUNT, t.elapsed()));

    // Metadata latency while a background thread writes slices, the case where
    // a shared write-ahead log would queue small commits behind bulk data.
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let store = Arc::clone(&store);
        let done = Arc::clone(&done);
        let payload = payload.to_vec();
        thread::spawn(move || {
            let t = Instant::now();
            for _ in 0..SLICE_COUNT {
                store
                    .put_slice(spool, Address::new_unique(), payload.clone())
                    .unwrap();
            }
            store.inner().inner().flush_store();
            let elapsed = t.elapsed();
            done.store(true, Ordering::Release);
            elapsed
        })
    };

    let mut put = Vec::new();
    let mut get = Vec::new();
    while !done.load(Ordering::Acquire) {
        sample_meta(&store, &mut put, &mut get);
        thread::sleep(META_PAUSE);
    }
    let bulk = writer.join().unwrap();
    println!("  meta loaded  put {}", stats(put));
    println!("               get {}", stats(get));
    println!("  loaded bulk  {}", throughput(SLICE_COUNT, bulk));
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn single_instance_vs_split() {
    let payload = random_payload(SLICE_SIZE);

    {
        let dir = TempDir::new().unwrap();
        let t = Instant::now();
        let store = TapeStore::new(
            RocksStore::open_with_cf_config(
                &dir.path().join("db"),
                create_db_options(),
                create_tape_store_configs(),
            )
            .unwrap(),
        );
        run_arm("single-instance", store, t.elapsed(), &payload);
    }

    {
        let dir = TempDir::new().unwrap();
        let t = Instant::now();
        let store = TapeStore::open_primary(dir.path().join("db")).unwrap();
        run_arm("split-same-disk", store, t.elapsed(), &payload);
    }
}
