//! Point-read microbenchmark over the `track_data` family, which is what the
//! open shard changes and what nothing else here measures.
//!
//! `track_data` is read by address and never walked, which is why it is the one
//! bulk family that can take an open-addressed shard without a sweep API first.
//! The shard only changes the resident index, so this reads back what it wrote
//! and times the lookups; the payload stays small so the row measures the index
//! rather than the disk.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test track_data_read_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel_bridge::{scaled, BenchArm, MetaBulkStore, ReelBridge};
use store_rocks::SplitStore;
use tape_core::track::data::BlobData;
use tape_crypto::address::Address;
use tape_store::ops::TrackDataOps;
use tempfile::TempDir;

const COUNTS: &[usize] = &[50_000, 200_000];

/// Payload bytes per row, small so the row weighs the index and not the device
const PAYLOAD_LEN: usize = 128;

/// Reads taken per arm, enough that one slow lookup does not carry the figure
const PROBES: usize = 50_000;

fn payload(seed: usize) -> BlobData {
    BlobData::Inline(vec![seed as u8; PAYLOAD_LEN])
}

/// The best of three passes, so a stray scheduler event does not become the row
fn best<F: FnMut() -> usize>(mut f: F, expect: usize) -> Duration {
    let mut best = Duration::MAX;
    for _ in 0..3 {
        let began = Instant::now();
        let found = f();
        best = best.min(began.elapsed());
        assert_eq!(found, expect, "an arm lost rows");
    }
    best
}

fn sweep<A: BenchArm>() {
    println!("{:>7}  {:>9}  {:>12}  {:>12}", "engine", "rows", "total", "per read");

    for &count in COUNTS {
        let count = scaled(count);
        let dir = TempDir::new().unwrap();
        let store = A::open_bench(&dir.path().join("db"));

        let mut addresses = Vec::with_capacity(count);
        for seed in 0..count {
            let address = Address::new_unique();
            store.put_track_data(address, payload(seed)).unwrap();
            addresses.push(address);
        }
        A::settle(&store);

        // Strided rather than sequential, so the read order is not the write
        // order and the shard is asked for a scattered key the way a node asks.
        let probes = PROBES.min(count);
        let stride = (count / probes).max(1);
        let elapsed = best(
            || {
                let mut found = 0;
                for at in 0..probes {
                    if store
                        .get_track_data(addresses[(at * stride) % count])
                        .unwrap()
                        .is_some()
                    {
                        found += 1;
                    }
                }
                found
            },
            probes,
        );

        let per_read = elapsed / probes as u32;
        let engine = A::NAME;
        println!("{engine:>7}  {count:>9}  {elapsed:>12.2?}  {per_read:>12.2?}");
    }
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn track_data_point_reads_rocks() {
    sweep::<SplitStore>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn track_data_point_reads_reel() {
    sweep::<ReelBridge>();
}

#[test]
#[ignore = "performance benchmark; run with --ignored --nocapture"]
fn track_data_point_reads_split() {
    sweep::<MetaBulkStore>();
}
