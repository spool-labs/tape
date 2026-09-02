//! Values wider than an allocation chunk read back whole
//!
//! Found by a 300 MiB multipart upload through the S3 gateway: 5 MiB parts came
//! back with zero runs ending on 4 MiB file offsets.

use reel_store::open_harness_store;
use tape_crypto::Hash;
use tape_store::ops::MultipartOps;
use tape_store::types::MultipartPart;
use tempfile::TempDir;

const PART_BYTES: usize = 5 * 1024 * 1024;
const PARTS: u32 = 60;

/// Deterministic bytes that differ per part, so a swap shows as well as a hole
fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn part(number: u32) -> MultipartPart {
    MultipartPart {
        part_number: number,
        etag: Hash::default(),
        last_modified: 0,
        size: PART_BYTES as u64,
    }
}

/// Every part read back, as (number, bytes returned, first bad offset, bad bytes)
fn mismatches(store: &impl MultipartOps) -> Vec<(u32, usize, usize, usize)> {
    let mut bad = Vec::new();
    for number in 1..=PARTS {
        let data = store
            .get_multipart_part_data("upload", number)
            .expect("read part")
            .expect("part present");
        let want = pattern(number as u64, PART_BYTES);
        if data != want {
            let first = data
                .iter()
                .zip(&want)
                .position(|(have, want)| have != want)
                .unwrap_or(data.len().min(want.len()));
            let wrong = data.iter().zip(&want).filter(|(have, want)| have != want).count();
            bad.push((number, data.len(), first, wrong));
        }
    }
    bad
}

#[test]
fn parts_read_back_whole() {
    let dir = TempDir::new().expect("temp dir");
    {
        let store = open_harness_store(dir.path()).expect("open");
        for number in 1..=PARTS {
            store
                .put_multipart_part("upload", &part(number), pattern(number as u64, PART_BYTES))
                .expect("put part");
        }
        let bad = mismatches(&store);
        assert!(bad.is_empty(), "parts differ before reopen (number, len, first bad, bad bytes): {bad:?}");
    }
    let store = open_harness_store(dir.path()).expect("reopen");
    let bad = mismatches(&store);
    assert!(bad.is_empty(), "parts differ after reopen (number, len, first bad, bad bytes): {bad:?}");
}

/// Parts written from four threads at once, the way a client uploads them
#[test]
#[ignore = "reel corrupts multi-MiB values written concurrently; red until the engine race is fixed"]
fn concurrent_parts_read_back_whole() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_harness_store(dir.path()).expect("open");
    std::thread::scope(|scope| {
        for lane in 0..4u32 {
            let store = &store;
            scope.spawn(move || {
                for number in (1..=PARTS).filter(|number| number % 4 == lane) {
                    store
                        .put_multipart_part("upload", &part(number), pattern(number as u64, PART_BYTES))
                        .expect("put part");
                }
            });
        }
    });
    let bad = mismatches(&store);
    assert!(bad.is_empty(), "parts differ after concurrent writes (number, len, first bad, bad bytes): {bad:?}");
}

/// The value size the concurrent fault first appears at, one store per size
#[test]
#[ignore = "reel corrupts multi-MiB values written concurrently; red until the engine race is fixed"]
fn concurrent_size_sweep() {
    let mut faulty = Vec::new();
    for &bytes in &[64 * 1024, 512 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024] {
        let dir = TempDir::new().expect("temp dir");
        let store = open_harness_store(dir.path()).expect("open");
        std::thread::scope(|scope| {
            for lane in 0..4u32 {
                let store = &store;
                scope.spawn(move || {
                    for number in (1..=PARTS).filter(|number| number % 4 == lane) {
                        let mut meta = part(number);
                        meta.size = bytes as u64;
                        store
                            .put_multipart_part("upload", &meta, pattern(number as u64, bytes))
                            .expect("put part");
                    }
                });
            }
        });
        let mut wrong = 0usize;
        for number in 1..=PARTS {
            let data = store.get_multipart_part_data("upload", number).expect("read").expect("present");
            if data != pattern(number as u64, bytes) {
                wrong += 1;
            }
        }
        eprintln!("size {bytes}: {wrong} of {PARTS} values wrong");
        if wrong > 0 {
            faulty.push((bytes, wrong));
        }
    }
    assert!(faulty.is_empty(), "sizes with wrong values (bytes, count): {faulty:?}");
}

/// The same concurrent writes on the posix backend, wherever the test runs
#[test]
#[ignore = "reel corrupts multi-MiB values written concurrently; red until the engine race is fixed"]
fn concurrent_parts_posix() {
    use reel::IoBackend;
    use reel_store::{node_config, ReelStore, Reserve, DEFAULT_SYNC_BYTES, TAPE_COLUMNS};
    use tape_store::TapeStore;

    let dir = TempDir::new().expect("temp dir");
    let config = node_config(0, DEFAULT_SYNC_BYTES, IoBackend::Posix, Reserve::Small);
    let store = TapeStore::new(ReelStore::open(dir.path(), config, TAPE_COLUMNS).expect("open posix"));
    std::thread::scope(|scope| {
        for lane in 0..4u32 {
            let store = &store;
            scope.spawn(move || {
                for number in (1..=PARTS).filter(|number| number % 4 == lane) {
                    store
                        .put_multipart_part("upload", &part(number), pattern(number as u64, PART_BYTES))
                        .expect("put part");
                }
            });
        }
    });
    let bad = mismatches(&store);
    assert!(bad.is_empty(), "posix parts differ after concurrent writes (number, len, first bad, bad bytes): {bad:?}");
}
