//! Bytes a slice costs an engine, per family, at the payload sizes the product holds
//!
//! Four figures per row. Logical is key plus payload, the only bytes the caller
//! asked to store. `live` is what the engine says it holds and `actual` is the
//! file bytes under the root, reservations included. `wchar` and `write_bytes`
//! are what went to write syscalls and what the kernel attributed a page at a
//! time. Beside them, what each slice family is left holding.
//!
//! `TAPE_BENCH_SLICE_FILL` picks the bytes, `md` or `random`, and every row
//! prints which it wrote. Point `TAPE_BENCH_KEEP` at a real filesystem: on tmpfs
//! `write_bytes` reads zero and `actual` measures RAM.
//!
//! Linux only, and one row per process since the counters are process-wide. Run
//! with `--ignored --nocapture --test-threads 1 --exact <row>`.

#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::time::Instant;

use reel_store::fill::{markdown, random};
use reel_store::written::Written;
use reel_store::{scaled, BenchArm, IndexReport, ReelStore};
use store_rocks::SplitStore;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::columns::ALL_COLUMN_FAMILIES;
use tape_store::ops::SliceOps;
use tape_store::TapeStore;
use tempfile::TempDir;

/// Key bytes handed to the store per slice: the spool big endian, then the track
const KEY_LEN: usize = 34;

/// Environment variable naming the bytes a run writes
const FILL_VAR: &str = "TAPE_BENCH_SLICE_FILL";

/// What a run's slices are made of, since a codec's answer is a property of them
#[derive(Clone, Copy)]
enum Fill {
    /// Markdown-shaped prose, which is what a data slice of a systematic code holds
    Markdown,

    /// Pseudorandom bytes, the parity-shaped control a codec declines
    Random,
}

impl Fill {
    /// The fill the run asked for, markdown unless it asked otherwise
    fn asked() -> Fill {
        match std::env::var(FILL_VAR).ok().as_deref() {
            None | Some("md") => Fill::Markdown,
            Some("random") => Fill::Random,
            Some(other) => panic!("{FILL_VAR} is md or random, not {other}"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Fill::Markdown => "md",
            Fill::Random => "random",
        }
    }

    fn bytes(self, seed: usize, len: usize) -> Vec<u8> {
        match self {
            Fill::Markdown => markdown(seed, len),
            Fill::Random => random(seed, len),
        }
    }
}

/// Spools the workload spreads its slices over, as a node holds several
const SPOOLS: u64 = 20;

/// One write workload, named by what it stands in for
struct Shape {
    /// What a reported row calls this workload
    label: &'static str,

    /// Slices written
    count: usize,

    /// Payload bytes per slice
    payload_len: usize,
}

/// A short md page, the size most of the product's objects code down to
const SMALL: Shape = Shape { label: "1.6KiB", count: 200_000, payload_len: 1_638 };

/// A page in the middle of the md regime
const MEDIUM: Shape = Shape { label: "12.8KiB", count: 25_000, payload_len: 13_107 };

/// The top of the md regime, still well under the 256 KiB sample window
const LARGE: Shape = Shape { label: "25.6KiB", count: 12_500, payload_len: 26_214 };

/// An engine a slice row runs against, and what it can say about its index
trait SliceArm: BenchArm {
    /// What the engine's index holds, or nothing for one that keeps no such figures
    fn index_report(store: &TapeStore<Self>) -> Option<IndexReport>;
}

impl SliceArm for SplitStore {
    fn index_report(_store: &TapeStore<Self>) -> Option<IndexReport> {
        None
    }
}

impl SliceArm for ReelStore {
    fn index_report(store: &TapeStore<Self>) -> Option<IndexReport> {
        Some(store.inner().inner().index_report())
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
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

/// The families a slice is stored under, whichever they are at this commit
fn slice_families() -> Vec<&'static str> {
    ALL_COLUMN_FAMILIES
        .iter()
        .copied()
        .filter(|name| name.starts_with("slice"))
        .collect()
}

fn sweep<Arm: SliceArm>(shape: &Shape) {
    let count = scaled(shape.count);
    let fill = Fill::asked();

    // Built up front, so the counters cover what the store wrote and not the fill.
    let mut records = Vec::with_capacity(count);
    for seed in 0..count {
        let spool = SpoolIndex(seed as u64 % SPOOLS);
        records.push((spool, Address::new_unique(), fill.bytes(seed, shape.payload_len)));
    }

    // A named root keeps the volume after the run, and is the only way to put it
    // on a filesystem that counts written bytes.
    let dir = TempDir::new().unwrap();
    let root = match std::env::var_os("TAPE_BENCH_KEEP") {
        Some(kept) => PathBuf::from(kept)
            .join(format!("{}-slice-{}-{}", Arm::NAME, shape.label, fill.label())),
        None => dir.path().join("db"),
    };
    let _ = std::fs::remove_dir_all(&root);
    println!("root {}", root.display());

    // Opened before the first reading, so what an engine writes to stand itself up
    // is not charged to the workload.
    let store = Arm::open_bench(&root);

    let began = Written::read();
    let started = Instant::now();
    for (spool, track, data) in records {
        store.put_slice(spool, track, data).unwrap();
    }
    let put_loop = started.elapsed();
    Arm::settle(&store);
    let durable = started.elapsed();
    let settled = Written::read().since(&began);

    let raw = store.inner().inner();
    let live = raw.live_data_size_bytes().ok().flatten().unwrap_or(0);
    let actual = raw.actual_size_bytes().unwrap_or(0);
    let usage = raw.cf_disk_usage().unwrap_or_default();

    let logical = count as u64 * (KEY_LEN + shape.payload_len) as u64;
    let per = |bytes: u64| bytes as f64 / count as f64;

    println!(
        "{:>8} {:>8} {:>7} {:>8} {:>9} {:>9} {:>9} {:>9} {:>11}",
        "arm", "shape", "fill", "records", "logical", "live", "actual", "wchar", "write_bytes",
    );
    println!(
        "{:>8} {:>8} {:>7} {count:>8} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>11.1}",
        Arm::NAME,
        shape.label,
        fill.label(),
        mib(logical),
        mib(live),
        mib(actual),
        mib(settled.wchar),
        mib(settled.write_bytes),
    );
    println!(
        "per-slice {} {} {} payload {} logical {:.1} live {:.1} actual {:.1} wchar {:.1} \
         write_bytes {:.1}",
        Arm::NAME,
        shape.label,
        fill.label(),
        shape.payload_len,
        per(logical),
        per(live),
        per(actual),
        per(settled.wchar),
        per(settled.write_bytes),
    );
    let payload_mib = mib(count as u64 * shape.payload_len as u64);
    println!(
        "timing {} {} {} put_us/op {:.2} durable_us/op {:.2} put_MiB/s {:.1} durable_MiB/s {:.1} \
         put_s {:.3} settle_s {:.3} syscw {} syscw/slice {:.2}",
        Arm::NAME,
        shape.label,
        fill.label(),
        put_loop.as_secs_f64() * 1e6 / count as f64,
        durable.as_secs_f64() * 1e6 / count as f64,
        payload_mib / put_loop.as_secs_f64(),
        payload_mib / durable.as_secs_f64(),
        put_loop.as_secs_f64(),
        (durable - put_loop).as_secs_f64(),
        settled.syscw,
        per(settled.syscw),
    );

    for family in slice_families() {
        let keys = raw.key_count_estimate(family).ok().flatten().unwrap_or(0);
        let bytes = usage
            .iter()
            .find(|row| row.cf == family)
            .map(|row| row.total_bytes())
            .unwrap_or(0);
        println!(
            "family {} {} {} {family} keys {keys} bytes {bytes} per-slice {:.1}",
            Arm::NAME,
            shape.label,
            fill.label(),
            per(bytes),
        );
    }

    if let Some(report) = Arm::index_report(&store) {
        println!(
            "index {} {} {} resident_bytes {} per-slice {:.1}",
            Arm::NAME,
            shape.label,
            fill.label(),
            report.resident_bytes,
            per(report.resident_bytes),
        );
        for column in report.columns {
            if !column.column.starts_with("slice") {
                continue;
            }
            println!(
                "index {} {} {} {} records {:?} bytes {:?} resident_keys {}",
                Arm::NAME,
                shape.label,
                fill.label(),
                column.column,
                column.records,
                column.bytes,
                column.keys,
            );
        }
    }

    // Settling flushes what is buffered but does not wait for whatever background
    // work that flush started. Closing does, so the file bytes are read with the
    // store shut, where nothing is left to be written.
    drop(store);
    let closed = Written::read().since(&began);
    let files = on_disk(&root);
    println!(
        "closed {} {} {} actual {:.1} wchar {:.1} write_bytes {:.1} per-slice actual {:.1} \
         wchar {:.1} write_bytes {:.1}",
        Arm::NAME,
        shape.label,
        fill.label(),
        mib(files),
        mib(closed.wchar),
        mib(closed.write_bytes),
        per(files),
        per(closed.wchar),
        per(closed.write_bytes),
    );
}

// what a small slice costs rocks, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn rocks_small() {
    sweep::<SplitStore>(&SMALL);
}

// what a small slice costs the reel, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn reel_small() {
    sweep::<ReelStore>(&SMALL);
}

// what a mid-size slice costs rocks, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn rocks_medium() {
    sweep::<SplitStore>(&MEDIUM);
}

// what a mid-size slice costs the reel, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn reel_medium() {
    sweep::<ReelStore>(&MEDIUM);
}

// what a large slice costs rocks, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn rocks_large() {
    sweep::<SplitStore>(&LARGE);
}

// what a large slice costs the reel, per family
#[test]
#[ignore = "byte-count benchmark, run with --ignored --nocapture"]
fn reel_large() {
    sweep::<ReelStore>(&LARGE);
}
