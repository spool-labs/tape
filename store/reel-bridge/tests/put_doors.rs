//! Which of the two write doors a large payload is charged for
//!
//! The slice write sweep reports a single put of a 10 MiB payload costing several
//! times what a batch carrying the same payload costs, on the bench box only. The
//! two doors plan and place the same record, so the split has to be either the
//! door or the buffer the borrowed-value trait forces the door to take. These four
//! variants separate them: the trait put and the raw per-record put both copy, the
//! owned per-record put and the batch both hand the buffer over.
//!
//! Ignored by default. Run with:
//!   cargo test -p reel-bridge --test put_doors --release -- --ignored --nocapture

use std::time::{Duration, Instant};

use reel::{ColumnId, KeyBytes, RecordKey, SEGMENT_SUFFIX};
use reel_bridge::{bench_config, ReelBridge};
use store::{Store, WriteBatch};
use tape_store::columns::ALL_COLUMN_FAMILIES;
use tempfile::TempDir;

/// The slice family, whose identifier is its position in the declared set plus one
const SLICE_COLUMN: ColumnId = ColumnId(14);

/// Payload sizes, straddling the 4 MiB row that is fine and the 10 MiB row that is not
const SIZES: &[usize] = &[1024 * 1024, 4 * 1024 * 1024, 10 * 1024 * 1024];

/// Puts per variant per round
const COUNT: usize = 12;

/// Rounds, whose order rotates so no variant permanently runs first
const ROUNDS: usize = 6;

const VARIANTS: usize = 4;

const NAMES: [&str; VARIANTS] = ["trait put", "raw put", "put_owned", "batch"];

/// Segment size the sweep opens with, matching the write bench
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

/// The bytes a caller hands the store, allocated and filled the way a serializer would
fn encode(payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(payload.len() + 8);
    value.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    value.extend_from_slice(payload);
    value
}

fn slice_key(at: usize) -> Vec<u8> {
    let mut key = vec![0u8; 34];
    key[26..34].copy_from_slice(&(at as u64).to_be_bytes());
    key
}

/// Minor page faults this process has taken, the gauge on freshly mapped buffers
fn minor_faults() -> u64 {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return 0;
    }
    usage.ru_minflt as u64
}

struct Round {
    elapsed: Duration,
    slowest: Duration,
    faults: u64,
    syncs: u64,
    segments: usize,
}

fn run_variant(variant: usize, size: usize) -> Round {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().join("db");
    let bridge = ReelBridge::open(&root, bench_config(SEGMENT_BYTES)).expect("open reel");
    let cf = ALL_COLUMN_FAMILIES[13];

    let payload = vec![0xABu8; size];
    let mut elapsed = Duration::ZERO;
    let mut slowest = Duration::ZERO;
    let faults_before = minor_faults();
    let syncs_before = bridge.engine().sync_count();

    for at in 0..COUNT {
        let key = slice_key(at);

        let start = Instant::now();
        match variant {
            // What the sweep's single put does: the trait lends the value, so the
            // engine takes a buffer of its own before it can queue it.
            0 => {
                let value = encode(&payload);
                bridge.put(cf, &key, &value).expect("put");
            }
            // The same copy, with the bridge and the trait taken out of it.
            1 => {
                let value = encode(&payload);
                let record = RecordKey::new(SLICE_COLUMN, KeyBytes::new(&key).expect("key"));
                bridge.engine().put(&record, &value).expect("raw put");
            }
            // The per-record door with the buffer handed over rather than lent.
            2 => {
                let value = encode(&payload);
                let record = RecordKey::new(SLICE_COLUMN, KeyBytes::new(&key).expect("key"));
                bridge.engine().put_owned(&record, value).expect("put owned");
            }
            // The batch door, which has taken owned payloads all along.
            _ => {
                let value = encode(&payload);
                let mut batch = WriteBatch::new();
                batch.put_owned(cf, key.clone(), value);
                bridge.write_batch(batch).expect("batch");
            }
        }
        let took = start.elapsed();
        elapsed += took;
        slowest = slowest.max(took);
    }

    let faults = minor_faults().saturating_sub(faults_before);
    let syncs = bridge.engine().sync_count().saturating_sub(syncs_before);
    bridge.flush().expect("flush");
    let segments = std::fs::read_dir(&root)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| entry.file_name().to_string_lossy().ends_with(SEGMENT_SUFFIX))
                .count()
        })
        .unwrap_or(0);

    Round {
        elapsed,
        slowest,
        faults,
        syncs,
        segments,
    }
}

#[test]
#[ignore = "diagnostic; run with --ignored --nocapture"]
fn put_door_split() {
    println!(
        "{:>9}  {:>10}  {:>11}  {:>11}  {:>9}  {:>6}  {:>5}  {:>4}",
        "size", "variant", "per op", "slowest", "faults/op", "MB/s", "syncs", "segs"
    );

    for &size in SIZES {
        let mut best = [Duration::MAX; VARIANTS];
        let mut faults = [0u64; VARIANTS];
        let mut slowest = [Duration::MAX; VARIANTS];
        let mut syncs = [0u64; VARIANTS];
        let mut segments = [0usize; VARIANTS];

        for round in 0..ROUNDS {
            for offset in 0..VARIANTS {
                let variant = (round + offset) % VARIANTS;
                let measured = run_variant(variant, size);
                if measured.elapsed < best[variant] {
                    best[variant] = measured.elapsed;
                    slowest[variant] = measured.slowest;
                    faults[variant] = measured.faults;
                }
                syncs[variant] = syncs[variant].max(measured.syncs);
                segments[variant] = segments[variant].max(measured.segments);
            }
        }

        for variant in 0..VARIANTS {
            let per_op = best[variant] / COUNT as u32;
            let rate = size as f64 / per_op.as_secs_f64() / 1_000_000.0;
            println!(
                "{:>6} MiB  {:>10}  {:>11.2?}  {:>11.2?}  {:>9}  {rate:>6.0}  {:>5}  {:>4}",
                size / (1024 * 1024),
                NAMES[variant],
                per_op,
                slowest[variant],
                faults[variant] / COUNT as u64,
                syncs[variant],
                segments[variant],
            );
        }
    }
}
