#![cfg(feature = "rocks")]

//! Point reads over `track_data`, one at a time against asked in batches
//!
//! Warm throughout: every row is written, settled and read back in one process,
//! so nothing here says what a cold volume costs. The one-at-a-time column is
//! what the node did before the sweeps were batched, and the shape an engine
//! with submission depth is worst at. Payloads stay small so the row weighs the
//! read path and not the device.
//!
//! Ignored by default. Run with `--ignored --nocapture` on a release build.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use reel_store::{scaled, BenchArm, ReelStore};
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

/// What a row's bytes look like, since a codec's answer depends entirely on it
#[derive(Clone, Copy)]
enum Fill {
    /// Pseudorandom, which lz4 declines and stores verbatim
    Random,

    /// Repetitive the way a real coded payload's framing is, which lz4 shrinks
    Packed,
}

impl Fill {
    fn label(self) -> &'static str {
        match self {
            Fill::Random => "random",
            Fill::Packed => "packed",
        }
    }
}

fn payload(seed: usize, fill: Fill) -> BlobData {
    let mut bytes = Vec::with_capacity(PAYLOAD_LEN);
    match fill {
        Fill::Random => {
            let mut state = 0x9E3779B97F4A7C15u64 ^ seed as u64;
            while bytes.len() < PAYLOAD_LEN {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes.extend_from_slice(&state.to_le_bytes());
            }
        }
        Fill::Packed => {
            let head = (seed as u64).to_le_bytes();
            while bytes.len() < PAYLOAD_LEN {
                bytes.extend_from_slice(&head);
                bytes.extend_from_slice(&[0u8; 24]);
            }
        }
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
        "{:>7}  {:>7}  {:>9}  {:>7}  {:>12}  {:>12}  {:>11}  {:>11}   (warm)",
        "engine", "fill", "rows", "batch", "total", "per read", "live", "files"
    );

    for fill in [Fill::Random, Fill::Packed] {
    for &count in COUNTS {
        let count = scaled(count);
        // A named root keeps the volume after the run, so what it laid down can
        // be looked at rather than inferred from a total.
        let dir = TempDir::new().unwrap();
        let root = match std::env::var_os("TAPE_BENCH_KEEP") {
            Some(kept) => PathBuf::from(kept).join(format!("{}-{count}", A::NAME)),
            None => dir.path().join("db"),
        };
        let store = A::open_bench(&root);

        let mut addresses = Vec::with_capacity(count);
        for seed in 0..count {
            let address = Address::new_unique();
            store.put_track_data(address, payload(seed, fill)).unwrap();
            addresses.push(address);
        }
        A::settle(&store);

        // Files on disk include whatever an engine reserved ahead of writing,
        // so one that preallocates looks enormous beside one that does not. Live
        // bytes are the engine's own answer, which is what to compare a codec
        // on.
        let files = store.inner().inner().actual_size_bytes().unwrap_or(0);
        let live = store
            .inner()
            .inner()
            .live_data_size_bytes()
            .ok()
            .flatten()
            .unwrap_or(0);

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
                    match batch {
                        // The engine serves one key and many keys down separate
                        // paths, and only the single one has the mapped read.
                        1 => {
                            for address in &asked {
                                if store.get_track_data(*address).unwrap().is_some() {
                                    found += 1;
                                }
                            }
                        }
                        batch => {
                            for chunk in asked.chunks(batch) {
                                for held in store.get_track_datas(chunk).unwrap() {
                                    if held.is_some() {
                                        found += 1;
                                    }
                                }
                            }
                        }
                    }
                    found
                },
                probes,
            );

            let per_read = elapsed / probes as u32;
            let engine = A::NAME;
            let label = fill.label();
            let live_mib = live as f64 / (1024.0 * 1024.0);
            let files_mib = files as f64 / (1024.0 * 1024.0);
            println!(
                "{engine:>7}  {label:>7}  {count:>9}  {batch:>7}  {elapsed:>12.2?}  \
                 {per_read:>12.2?}  {live_mib:>8.1} MiB  {files_mib:>8.1} MiB"
            );
        }
    }
    }
}

// what a point read costs rocks, singly and in batches
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn point_reads_rocks() {
    sweep::<SplitStore>();
}

// what a point read costs the reel, singly and in batches
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn point_reads_reel() {
    sweep::<ReelStore>();
}
