//! Count-path microbenchmark for `count_tracks`: keys-only scan
//! (`iter_keys_prefix`) vs the old value-reading scan (`iter_from(..).count()`)
//! over the BlockBased `track` CF. Both the node's and gateway's `/v1/stats`
//! count tracks this way.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test track_count_bench -- --ignored --nocapture

use std::time::{Duration, Instant};

use store::{Column, Direction, Store};
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{GroupIndex, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;
use tape_store::columns::TrackCol;
use tape_store::ops::TrackOps;
use tape_store::TapeStore;
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

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn keys_only_vs_value_reading_count() {
    println!("{:>9}  {:>13}  {:>11}  {:>9}", "tracks", "value-reading", "keys-only", "speedup");

    for &count in COUNTS {
        let dir = TempDir::new().unwrap();
        let store = TapeStore::open_primary(dir.path().join("db")).unwrap();
        for _ in 0..count {
            store.put_track(Address::new_unique(), sample_track()).unwrap();
        }
        let raw = store.inner().inner();
        raw.flush().unwrap();

        let value_reading = best(
            || raw.iter_from(TrackCol::CF_NAME, &[], Direction::Asc).unwrap().count(),
            count,
        );
        let keys_only = best(
            || raw.iter_keys_prefix(TrackCol::CF_NAME, &[]).unwrap().len(),
            count,
        );

        let speedup = value_reading.as_secs_f64() / keys_only.as_secs_f64().max(f64::MIN_POSITIVE);
        println!("{count:>9}  {value_reading:>13.2?}  {keys_only:>11.2?}  {speedup:>8.1}x");
    }
}
