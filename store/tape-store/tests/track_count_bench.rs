#![cfg(feature = "rocks")]

//! Count-path microbenchmark for `count_tracks`: keys-only scan
//! (`iter_keys_prefix`) vs the old value-reading scan (`iter_from(..).count()`)
//! over the BlockBased `track` CF. Both the node's and gateway's `/v1/stats`
//! count tracks this way.
//!
//! Three arms. The rocks and split arms are the layout a node runs: `track` is a
//! metadata family on the RocksDB meta volume, so those two rows should agree.
//! The all-reel arm is a hypothetical, since the production reel serves bulk
//! families only and never sees `track`; it says what a metadata scan would cost
//! if it did.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --features rocks --test track_count_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel_store::{scaled, BenchArm, MetaBulkStore, ReelStore};
use store::{Column, Direction};
use store_rocks::SplitStore;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{GroupIndex, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_store::columns::TrackCol;
use tape_store::ops::TrackOps;
use tempfile::TempDir;

const COUNTS: &[usize] = &[10_000, 50_000, 200_000];

fn sample_track() -> CompressedTrack {
    CompressedTrack {
        tape: Address::new_unique(),
        key: Hash::new_unique(),
        track_number: TrackNumber(0),
        kind: TrackKind::Coded as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(1024),
        group: GroupIndex(3),
        value_hash: Hash::new_unique(),
    }
}

fn best<F: FnMut() -> usize>(mut f: F, expect: usize) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let n = f();
        best = best.min(t.elapsed());
        assert_eq!(n, expect);
    }
    best
}

fn sweep<A: BenchArm>() {
    println!(
        "{:>7}  {:>9}  {:>13}  {:>11}  {:>9}",
        "engine", "tracks", "value-reading", "keys-only", "speedup"
    );

    for &count in COUNTS {
        let count = scaled(count);
        let dir = TempDir::new().unwrap();
        let store = A::open_bench(&dir.path().join("db"));
        for _ in 0..count {
            store.put_track(Address::new_unique(), sample_track()).unwrap();
        }
        A::settle(&store);
        let raw = store.inner().inner();

        let value_reading = best(
            || raw.iter_from(TrackCol::CF_NAME, &[], Direction::Asc).unwrap().count(),
            count,
        );
        let keys_only = best(
            || raw.iter_keys_prefix(TrackCol::CF_NAME, &[]).unwrap().len(),
            count,
        );

        let speedup = value_reading.as_secs_f64() / keys_only.as_secs_f64().max(f64::MIN_POSITIVE);
        let engine = A::NAME;
        println!(
            "{engine:>7}  {count:>9}  {value_reading:>13.2?}  {keys_only:>11.2?}  {speedup:>8.1}x"
        );
    }
}

// what a keys-only count saves rocks against reading values
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn key_count_rocks() {
    sweep::<SplitStore>();
}

// what a keys-only count saves the reel against reading values
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn key_count_reel() {
    sweep::<ReelStore>();
}

// what a keys-only count saves the split arm against reading values
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn key_count_split() {
    sweep::<MetaBulkStore>();
}
