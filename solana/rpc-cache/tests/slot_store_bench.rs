//! Slot store costs at devnet scale. Run with
//! `cargo test --release -p rpc-cache --test slot_store_bench -- --ignored --nocapture`

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rpc_cache::store::block::CachedBlock;
use rpc_cache::store::slot_store::SlotStore;

const SLOTS: u64 = 200_000;
/// A filtered devnet block's bodies, measured 2026-09
const BASE64_BYTES: usize = 200;
const JSON_BYTES: usize = 240;
/// One slot in this many is skipped
const SKIP_EVERY: u64 = 10;
/// The bootstrap fetches this many slots at once
const FILL_CONCURRENCY: usize = 16;
const FILL_WINDOW: u64 = 4096;
const MEMORY_BYTES: u64 = 4 << 30;
const READERS: usize = 64;
const READS: usize = 100_000;
const WRITE_LOAD: Duration = Duration::from_secs(3);
const FINGERPRINT: &str = "bench";
/// Knuth's MMIX constants, so each reader walks its own spread of slots
const STEP_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
const STEP_INCREMENT: u64 = 1_442_695_040_888_963_407;
const READER_SEED: u64 = 7_919;

fn block(slot: u64) -> CachedBlock {
    if slot.is_multiple_of(SKIP_EVERY) {
        return CachedBlock::Skipped;
    }
    CachedBlock::Present {
        base64: Bytes::from(vec![b'b'; BASE64_BYTES]),
        json: Bytes::from(vec![b'j'; JSON_BYTES]),
    }
}

fn percentiles(mut samples: Vec<Duration>) -> String {
    samples.sort_unstable();
    let at = |quantile: f64| samples[((samples.len() - 1) as f64 * quantile) as usize];
    format!("p50 {:?}  p99 {:?}  max {:?}", at(0.50), at(0.99), samples.last().copied().unwrap_or_default())
}

fn file_size(path: &Path) -> u64 {
    let wal = path.with_extension("db-wal");
    let main = std::fs::metadata(path).map(|metadata| metadata.len()).unwrap_or(0);
    main + std::fs::metadata(wal).map(|metadata| metadata.len()).unwrap_or(0)
}

/// `READS` reads of slots at or above `floor` from `READERS` tasks, each read timed
async fn read_load(store: &Arc<SlotStore>, floor: u64) -> (Vec<Duration>, Duration) {
    let started = Instant::now();
    let mut tasks = Vec::new();
    for task in 0..READERS {
        let store = Arc::clone(store);
        tasks.push(tokio::spawn(async move {
            let mut samples = Vec::with_capacity(READS / READERS);
            let mut slot = task as u64 * READER_SEED;
            for _ in 0..READS / READERS {
                slot = floor + slot.wrapping_mul(STEP_MULTIPLIER).wrapping_add(STEP_INCREMENT) % (SLOTS - floor);
                let read = Instant::now();
                let found = store.get(slot).await.expect("read");
                samples.push(read.elapsed());
                assert!(found.is_some(), "slot {slot} missing");
            }
            samples
        }));
    }
    let mut samples = Vec::new();
    for task in tasks {
        samples.extend(task.await.expect("reader task"));
    }
    (samples, started.elapsed())
}

/// Fill every slot the way the bootstrap does, `FILL_CONCURRENCY` at a time
async fn fill(store: &Arc<SlotStore>) -> Duration {
    let started = Instant::now();
    let slots: Vec<u64> = (0..SLOTS).collect();
    for chunk in slots.chunks(FILL_CONCURRENCY) {
        let mut writes = Vec::new();
        for &slot in chunk {
            let store = Arc::clone(store);
            writes.push(tokio::spawn(async move { store.insert(slot, block(slot)).await }));
        }
        for write in writes {
            write.await.expect("write task").expect("write");
        }
    }
    started.elapsed()
}

// the slot store's fill, read, restart and prune costs
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn costs() {
    let directory = std::env::temp_dir().join(format!("rpc-cache-bench-{}", std::process::id()));
    std::fs::remove_dir_all(&directory).ok();
    std::fs::create_dir_all(&directory).expect("temp directory");
    let path = directory.join("blocks.db");

    let store = Arc::new(SlotStore::try_open(&path, MEMORY_BYTES, FINGERPRINT.to_string()).await.expect("open"));
    let took = fill(&store).await;
    println!("fill {SLOTS} slots: {took:?} ({:.0} slots/s)", SLOTS as f64 / took.as_secs_f64());
    println!("file size: {:.1} MB", file_size(&path) as f64 / 1e6);

    let (memory, took) = read_load(&store, 0).await;
    println!("memory reads {READS} x{READERS}: {:.0}/s  {}", READS as f64 / took.as_secs_f64(), percentiles(memory));
    drop(store);

    // A restart: no memory layer, so every read below goes to the file
    let started = Instant::now();
    let store = Arc::new(SlotStore::try_open(&path, 0, FINGERPRINT.to_string()).await.expect("reopen"));
    let opened = started.elapsed();
    let started = Instant::now();
    let mut listed = 0;
    let mut from = 0;
    while from < SLOTS {
        listed += store.stored_in(from, from + FILL_WINDOW - 1).await.expect("list").len();
        from += FILL_WINDOW;
    }
    println!("restart: open {opened:?}, list {listed} stored slots in {:?}", started.elapsed());

    let (file, took) = read_load(&store, 0).await;
    println!("file reads {READS} x{READERS}: {:.0}/s  {}", READS as f64 / took.as_secs_f64(), percentiles(file));

    // File reads while the live tail writes and a prune runs
    let writer_store = Arc::clone(&store);
    let writer = tokio::spawn(async move {
        let mut slot = SLOTS;
        let started = Instant::now();
        while started.elapsed() < WRITE_LOAD {
            writer_store.insert(slot, block(slot)).await.expect("write");
            slot += 1;
        }
        let started = Instant::now();
        let pruned = writer_store.prune_below(SLOTS / 2).await.expect("prune");
        (pruned, started.elapsed())
    });
    let (busy, took) = read_load(&store, SLOTS / 2).await;
    let (pruned, prune_took) = writer.await.expect("writer task");
    println!("file reads under writes and a prune: {:.0}/s  {}", READS as f64 / took.as_secs_f64(), percentiles(busy));
    println!("prune {pruned} rows: {prune_took:?}");

    std::fs::remove_dir_all(&directory).ok();
}
