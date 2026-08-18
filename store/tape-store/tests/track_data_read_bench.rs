//! Point reads over `track_data`, one at a time against asked in batches.
//!
//! Warm throughout: every row is written, settled and then read back in the same
//! process, so nothing here says what a cold volume costs.
//!
//! The one-at-a-time column is what the node did everywhere before the sweeps
//! were batched, and it is the shape an engine with submission depth is worst
//! at: nothing is ever in flight but the read being waited on. The batched
//! column is the same rows through `get_many`, which is the whole point of the
//! wider store trait. Payloads stay small so the row weighs the read path and
//! not the device.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-store --test track_data_read_bench --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel_bridge::{scaled, BenchArm, ReelBridge};
use store_rocks::SplitStore;
use tape_core::track::data::BlobData;
use tape_crypto::address::Address;
use tape_store::ops::TrackDataOps;
use tempfile::TempDir;

const COUNTS: &[usize] = &[50_000, 200_000];

/// Keys asked for at once, the first being the loop the batching replaced
const BATCHES: &[usize] = &[1, 64, 512];

/// Payload bytes per row, small so the row weighs the read path and not the device
const PAYLOAD_LEN: usize = 128;

/// Reads taken per arm, enough that one slow lookup does not carry the figure
const PROBES: usize = 50_000;

/// An incompressible payload, so a block-compressing engine is not handed a win
///
/// A run of one repeated byte compresses to nothing, which flatters whichever
/// arm compresses blocks and measures the codec rather than the read.
fn payload(seed: usize) -> BlobData {
    let mut state = 0x9E3779B97F4A7C15u64 ^ seed as u64;
    let mut bytes = Vec::with_capacity(PAYLOAD_LEN);
    while bytes.len() < PAYLOAD_LEN {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(PAYLOAD_LEN);
    BlobData::Inline(bytes)
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
    println!(
        "{:>7}  {:>9}  {:>7}  {:>12}  {:>12}   (warm)",
        "engine", "rows", "batch", "total", "per read"
    );

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
        let mut asked = Vec::with_capacity(probes);
        for at in 0..probes {
            asked.push(addresses[(at * stride) % count]);
        }

        for &batch in BATCHES {
            let elapsed = best(
                || {
                    let mut found = 0;
                    for chunk in asked.chunks(batch) {
                        // A batch of one is the loop, through the same call, so
                        // the column measures depth and not two code paths.
                        for held in store.get_track_datas(chunk).unwrap() {
                            if held.is_some() {
                                found += 1;
                            }
                        }
                    }
                    found
                },
                probes,
            );

            let per_read = elapsed / probes as u32;
            let engine = A::NAME;
            println!(
                "{engine:>7}  {count:>9}  {batch:>7}  {elapsed:>12.2?}  {per_read:>12.2?}"
            );
        }
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
