#![cfg(feature = "rocks")]

//! Bytes an engine writes, and holds, for what the `track_data` workload hands it
//!
//! `wchar` and `write_bytes` are the write counters. Beside them `live`, the
//! engine's own answer and the one a codec moves, and `actual`, the file bytes
//! with reservations included. A reservation is not an amplified byte, it is a
//! file larger than what it holds.
//!
//! Codec rows use a markdown fill and random is the control, which a codec
//! declines. The codec is taken from `TAPE_BENCH_TRACK_DATA_CODEC` and every row
//! prints which it opened with.
//!
//! Linux only, and one row per process since the counters are process-wide. Run
//! with `--ignored --nocapture --test-threads 1 --exact <row>`.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};

use reel_store::fill::{markdown, random};
use reel_store::written::Written;
use reel_store::{scaled, track_data_codec, BenchArm, ReelStore};
use store_rocks::SplitStore;
use tape_core::track::data::BlobData;
use tape_crypto::address::Address;
use tape_store::ops::TrackDataOps;
use tempfile::TempDir;

/// Key bytes handed to the store per record, one address
const KEY_LEN: usize = 32;

/// Payload floor `reel`'s `admit` attempts a codec above, for the note it earns
const CODEC_FLOOR: usize = 256;

/// What a row's bytes look like, since a codec's answer depends entirely on it
#[derive(Clone, Copy, PartialEq)]
enum Fill {
    /// Pseudorandom, which lz4 declines and stores verbatim
    Random,

    /// Markdown-shaped English, the sub-256 KiB content the product actually holds
    Markdown,
}

impl Fill {
    fn label(self) -> &'static str {
        match self {
            Fill::Random => "random",
            Fill::Markdown => "md",
        }
    }
}

/// One write workload, named by what it stands in for
struct Shape {
    /// What a reported row calls this workload
    label: &'static str,

    /// Records written
    count: usize,

    /// Payload bytes per record
    payload_len: usize,
}

/// The shape the read bench already uses, small enough to weigh per-record cost
const SMALL: Shape = Shape { label: "128B", count: 200_000, payload_len: 128 };

/// A markdown page, the size the product actually stores under the 256 KiB line
const MEDIUM: Shape = Shape { label: "16KiB", count: 20_000, payload_len: 16 * 1024 };

/// The top of the sub-256 KiB regime, where a record is many pages on its own
const LARGE: Shape = Shape { label: "64KiB", count: 5_000, payload_len: 64 * 1024 };

fn payload(seed: usize, len: usize, fill: Fill) -> BlobData {
    BlobData::Inline(match fill {
        Fill::Random => random(seed, len),
        Fill::Markdown => markdown(seed, len),
    })
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn header() {
    println!(
        "{:>7} {:>5} {:>6} {:>6} {:>7} {:>7} {:>9} {:>9} {:>6} {:>11} {:>6} {:>9} {:>6} {:>9} {:>6}",
        "engine", "codec", "fill", "shape", "records", "at", "logical", "wchar", "ratio",
        "write_bytes", "ratio", "live", "ratio", "actual", "ratio",
    );
}

/// Bytes the files under a root occupy, which is measurable with the store closed
fn on_disk(root: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut bytes = 0;
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            bytes += on_disk(&entry.path());
        } else if let Ok(data) = entry.metadata() {
            bytes += data.len();
        }
    }
    bytes
}

/// A size cell, or a dash where the figure cannot be had at that point
fn cell(bytes: Option<u64>, logical: u64) -> String {
    match bytes {
        Some(bytes) => format!("{:>9.1} {:>5.2}x", mib(bytes), bytes as f64 / logical as f64),
        None => format!("{:>9} {:>6}", "-", "-"),
    }
}

#[allow(clippy::too_many_arguments)]
fn row(
    engine: &str,
    codec: &str,
    fill: Fill,
    shape: &Shape,
    count: usize,
    at: &str,
    logical: u64,
    written: &Written,
    live: Option<u64>,
    actual: u64,
) {
    let ratio = |bytes: u64| bytes as f64 / logical as f64;
    println!(
        "{engine:>7} {codec:>5} {:>6} {:>6} {count:>7} {at:>7} {:>9.1} {:>9.1} {:>5.2}x \
         {:>11.1} {:>5.2}x {} {}",
        fill.label(),
        shape.label,
        mib(logical),
        mib(written.wchar),
        ratio(written.wchar),
        mib(written.write_bytes),
        ratio(written.write_bytes),
        cell(live, logical),
        cell(Some(actual), logical),
    );
}

fn sweep<Arm: BenchArm>(shape: &Shape, fill: Fill) {
    let count = scaled(shape.count);
    let codec = track_data_codec();

    if fill == Fill::Markdown && shape.payload_len < CODEC_FLOOR {
        println!("skipped: admit does not attempt a payload below {CODEC_FLOOR} B");
        return;
    }

    // Built up front, so the counters cover what the store wrote and not the fill.
    let mut records = Vec::with_capacity(count);
    for seed in 0..count {
        records.push((Address::new_unique(), payload(seed, shape.payload_len, fill)));
    }

    // A named root keeps the volume after the run, so what it laid down can be
    // looked at rather than inferred from a total.
    let dir = TempDir::new().unwrap();
    let root = match std::env::var_os("TAPE_BENCH_KEEP") {
        Some(kept) => PathBuf::from(kept).join(format!("{}-{codec}-{}", Arm::NAME, shape.label)),
        None => dir.path().join("db"),
    };
    println!("root {}", root.display());

    // Opened before the first reading, so what an engine writes to stand itself up
    // is not charged to the workload.
    let store = Arm::open_bench(&root);

    let began = Written::read();
    for (address, data) in records {
        store.put_track_data(address, data).unwrap();
    }
    Arm::settle(&store);
    let settled = Written::read().since(&began);
    let settled_live = store.inner().inner().live_data_size_bytes().ok().flatten().unwrap_or(0);
    let settled_actual = store.inner().inner().actual_size_bytes().unwrap_or(0);

    let logical = count as u64 * (KEY_LEN + shape.payload_len) as u64;
    header();
    row(Arm::NAME, codec, fill, shape, count, "settled", logical, &settled, Some(settled_live),
        settled_actual);

    // Settling flushes what is buffered but does not wait for whatever background
    // work that flush started. Closing does, so the second row is the one that
    // includes any compaction the workload set off. Live bytes are the engine's
    // own answer and there is nothing left to ask, so the closed row carries the
    // file bytes under the root instead.
    drop(store);
    let closed = Written::read().since(&began);
    row(Arm::NAME, codec, fill, shape, count, "closed", logical, &closed, None, on_disk(&root));
}

// bytes rocks writes for a small record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn rocks_small() {
    sweep::<SplitStore>(&SMALL, Fill::Random);
}

// bytes the reel writes for a small record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn reel_small() {
    sweep::<ReelStore>(&SMALL, Fill::Random);
}

// bytes rocks writes for a mid-size record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn rocks_medium() {
    sweep::<SplitStore>(&MEDIUM, Fill::Random);
}

// bytes the reel writes for a mid-size record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn reel_medium() {
    sweep::<ReelStore>(&MEDIUM, Fill::Random);
}

// bytes rocks writes for a large record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn rocks_large() {
    sweep::<SplitStore>(&LARGE, Fill::Random);
}

// bytes the reel writes for a large record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn reel_large() {
    sweep::<ReelStore>(&LARGE, Fill::Random);
}

// what the codec buys rocks on a mid-size record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn codec_rocks_mid() {
    sweep::<SplitStore>(&MEDIUM, Fill::Markdown);
}

// what the codec buys the reel on a mid-size record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn codec_reel_mid() {
    sweep::<ReelStore>(&MEDIUM, Fill::Markdown);
}

// what the codec buys rocks on a large record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn codec_rocks_big() {
    sweep::<SplitStore>(&LARGE, Fill::Markdown);
}

// what the codec buys the reel on a large record
#[test]
#[ignore = "performance benchmark, run with --ignored --nocapture"]
fn codec_reel_big() {
    sweep::<ReelStore>(&LARGE, Fill::Markdown);
}
