//! The rocks arm's configuration, sized for the bench box rather than the fleet
//!
//! `tape_store::config` is sized for the fleet, which runs spinning disks behind
//! small memory: compaction is rate limited to 100 MB/s on the bulk volume, the
//! block cache is 96 MiB, and memtable memory across the instance is capped at
//! 128 MiB. A baseline opened under those ceilings reports the ceilings. The
//! bench box is 64 threads, 246 GB of memory and an NVMe that sustains over
//! 2 GB/s, so the arm opens with sizing fit for it.
//!
//! The column families keep the shapes the node declares. Same prefix
//! extractors, same 256 KiB blob threshold, same split of families across the
//! metadata and bulk volumes: `tape_store::config::tape_store_column_configs`
//! is still where they come from, and only the sizing is laid over them here.
//!
//! Every knob with an opposite number on the reel side is set to match it, and
//! `RUNBOOK-tape.md` records each pairing:
//!
//! - Compression is off on both engines. The bench column set declares
//!   `Codec::None` for every column, and the payloads are pseudorandom, so
//!   compression would only spend CPU.
//! - Neither engine paces compaction. The reel arm runs `CompactRate::Auto`,
//!   which is unpaced, so the rocks arm carries no rate limiter.
//! - Both engines read and write buffered. The reel arm resolves
//!   `IoBackend::Auto` to the posix backend and keeps its pages
//!   (`PageCache::Keep`), so the rocks arm takes no direct IO either and its
//!   block cache is what stands in for the reel's page-cache residency.
//! - The write-ahead log stays on. The reel's `SyncPolicy::Never` still leaves
//!   every appended record in the page cache, where it survives a process
//!   crash; a RocksDB memtable does not, so disabling the WAL would make the
//!   baseline promise less than the engine it is measured against.

use std::path::Path;

use store::{Error as StoreError, Result as StoreResult};
use store_rocks::{
    BlockBasedOptions, Cache, ColumnFamilyConfig, ColumnFamilyDescriptor, DBCompressionType,
    Options, RocksStore, SplitStore,
};
use tape_store::config::{
    tape_store_column_configs, BULK_COLUMN_FAMILIES, BULK_SUBDIR, META_SUBDIR,
};
use tape_store::error::TapeStoreError;
use tape_store::ops::SliceOps;
use tape_store::TapeStore;

/// Cache one RocksDB instance shares across its block reads and its blob reads
///
/// The reel answers its reads out of the page cache, which on this box is most
/// of 246 GB. RocksDB consults its own cache first, so the fleet's 96 MiB would
/// have the baseline fault slices back off the filesystem while the reel served
/// them from memory. This is larger than the live set of any single store the
/// campaign opens, and the split arm's two instances still leave the box the
/// great majority of its memory for the page cache both engines write through.
pub const CACHE_BYTES: usize = 32 * 1024 * 1024 * 1024;

/// Data block size for the families whose values are payloads
///
/// A block ends at the first value that crosses the boundary, so a family of
/// hundreds-of-KiB values would put one value in a block whatever this said.
/// The size that matters is the index: 64 KiB quarters the index entries a
/// scan of a bulk family walks against the fleet's 16 KiB.
const PAYLOAD_BLOCK_BYTES: usize = 64 * 1024;

/// Data block size for the families whose values are metadata rows
///
/// The fleet's figure, kept: a point lookup reads a whole block to answer, and
/// a wider block on small rows is read amplification with nothing to show.
const ROW_BLOCK_BYTES: usize = 16 * 1024;

/// Bloom filter bits per key, the fleet's figure
///
/// Ten bits is roughly a 1% false positive rate, which is the standard trade;
/// the cache here is large enough to hold every filter it builds.
const BLOOM_BITS_PER_KEY: f64 = 10.0;

/// Memtable size per column family
///
/// Four times the fleet's, so a bulk family flushes an L0 file worth writing
/// rather than a stream of small ones. The instance-wide ceiling below is what
/// actually bounds the memory, since only a few families are ever hot.
const WRITE_BUFFER_BYTES: usize = 256 * 1024 * 1024;

/// Memtables one column family may hold before a write waits on a flush
const WRITE_BUFFER_COUNT: i32 = 6;

/// Immutable memtables merged into one flush
///
/// Halves the L0 file count against flushing each memtable on its own, with
/// four buffers of headroom left before a write could wait.
const WRITE_BUFFERS_TO_MERGE: i32 = 2;

/// Ceiling on memtable memory across all column families of one instance
///
/// The fleet's 128 MiB would flush a 256 MiB memtable before it ever filled.
/// Crossing this flushes the largest memtable rather than stalling writes, so
/// it is a bound on memory and not a governor on throughput.
const TOTAL_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024 * 1024;

/// Cap on total write-ahead-log size
///
/// Crossing this forces column families to flush so the log can be dropped.
/// The fleet's 1 GiB would force a flush every 1 GiB written, on memtables a
/// quarter full, against benches that write 40 GiB.
const MAX_TOTAL_WAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Bytes written to an SST before RocksDB asks the kernel to start writing back
///
/// A writeback hint, not a durability promise: `sync_file_range` does not wait
/// and nothing is made durable earlier than it would have been. Without it a
/// 246 GB page cache absorbs tens of GB of dirty pages and hands the whole bill
/// to one fsync at file close, which is a latency spike the layout bench would
/// read as a metadata stall.
const BYTES_PER_SYNC: u64 = 1024 * 1024;

/// Threads one compaction may split itself across
///
/// Unset, RocksDB runs a compaction on one thread, which leaves an L0 to L1
/// drain single-threaded on a 64-thread box. Eight is well inside the 48
/// compaction threads the job budget resolves to.
const SUBCOMPACTIONS: u32 = 8;

/// L0 files that start a compaction, RocksDB's default
const L0_COMPACTION_TRIGGER: i32 = 4;

/// L0 files at which writes are slowed
///
/// Sized past the campaign rather than switched off: zero would disable the
/// governor, which RocksDB documents as a way to build an L0 no read can walk.
/// The defaults, 20 and 36, are sized for a device that cannot drain L0 at
/// device speed. This one can.
const L0_SLOWDOWN_TRIGGER: i32 = 48;

/// L0 files at which writes stop until compaction catches up
const L0_STOP_TRIGGER: i32 = 64;

/// Pending compaction bytes at which writes are slowed
///
/// The default is 64 GB, which the 40 GiB write bench can approach on its own.
/// Sized above the campaign's largest dataset, again rather than disabled.
const SOFT_PENDING_COMPACTION_BYTES: usize = 256 * 1024 * 1024 * 1024;

/// Pending compaction bytes at which writes stop
const HARD_PENDING_COMPACTION_BYTES: usize = 1024 * 1024 * 1024 * 1024;

/// Target SST file size at the first level
///
/// Four times the default, matching the memtable, so a level holds files rather
/// than thousands of fragments.
const TARGET_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// Target size of the base level
///
/// The default 256 MiB is smaller than a single L0 file here, which would put
/// every flush into an immediate compaction. Four flushes of two merged
/// memtables is 2 GiB of L0, so the base level is sized to take it.
const LEVEL_BASE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Families whose values are payloads rather than metadata rows
///
/// Not the same list as the bulk volume: `slice_size` and `slice_sidecar` ride
/// the bulk volume because a batch cannot span the two, but they hold eight-byte
/// lengths and sub-leaf nodes and read like metadata.
const PAYLOAD_FAMILIES: &[&str] = &[
    "track_data",
    "slice",
    "snapshot_artifact",
    "s3_multipart_part_data",
];

/// A cache for one instance, which is where the arm's memory ceiling is set
pub fn bench_cache() -> Cache {
    Cache::new_lru_cache(CACHE_BYTES)
}

/// Database-wide options every rocks bench instance opens with
pub fn bench_db_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);

    // RocksDB spends a quarter of the job budget on flushes and the rest on
    // compaction, so a 64-thread box resolves to 16 and 48.
    let cpus = std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(4) as i32;
    options.increase_parallelism(cpus);
    options.set_max_background_jobs(cpus);
    options.set_max_subcompactions(SUBCOMPACTIONS);

    // No rate limiter. The fleet paces compaction because its device cannot
    // serve reads through a compaction storm; this device can, and the reel arm
    // compacts unpaced.

    options.set_db_write_buffer_size(TOTAL_WRITE_BUFFER_BYTES);
    options.set_max_total_wal_size(MAX_TOTAL_WAL_BYTES);
    options.set_bytes_per_sync(BYTES_PER_SYNC);

    // The write-ahead log is left alone: it is deleted as memtables flush, so
    // hinting writeback on a file about to be dropped is work for nothing.

    // Nothing per-family belongs here. A family opened from a descriptor takes
    // its column-family options from that descriptor and reads none of these,
    // so compression, memtables and level shape are all set in `tuned`.

    options
}

/// The table format a family reads through
fn block_options(cache: &Cache, block_bytes: usize) -> BlockBasedOptions {
    let mut block = BlockBasedOptions::default();
    block.set_block_size(block_bytes);
    block.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

    // Index and filter blocks live in the cache, which is bounded, rather than
    // outside it, which is not. RocksDB gives them high priority within it by
    // default, and pinning the L0 ones keeps the newest files' filters resident
    // through a scan that would otherwise evict them.
    block.set_cache_index_and_filter_blocks(true);
    block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block.set_optimize_filters_for_memory(true);

    // Partitioned indexes and filters are for an instance whose filters do not
    // fit its cache. A 256 MiB file of 64 KiB blocks carries an index of a few
    // hundred KiB, and 32 GiB of cache holds every filter these families build,
    // so partitioning would only add a level of indirection to every lookup.

    block.set_block_cache(cache);
    block
}

/// One family's shape with the bench box's sizing laid over it
fn tuned(config: ColumnFamilyConfig, cache: &Cache) -> ColumnFamilyDescriptor {
    let block_bytes = if PAYLOAD_FAMILIES.contains(&config.name()) {
        PAYLOAD_BLOCK_BYTES
    } else {
        ROW_BLOCK_BYTES
    };
    let block = block_options(cache, block_bytes);

    config
        .with_options(|options| {
            // Replaces the table factory the shape carries, cache and all.
            options.set_block_based_table_factory(&block);

            // Pseudorandom payloads do not compress, and the reel arm declares
            // Codec::None for every column.
            options.set_compression_type(DBCompressionType::None);
            options.set_bottommost_compression_type(DBCompressionType::None);
            options.set_blob_compression_type(DBCompressionType::None);

            // A blob read otherwise consults no cache at all, so every slice
            // above the 256 KiB threshold would come off the filesystem on
            // every read. Same cache as the blocks, so the instance still has
            // one ceiling.
            options.set_blob_cache(cache);

            options.set_write_buffer_size(WRITE_BUFFER_BYTES);
            options.set_max_write_buffer_number(WRITE_BUFFER_COUNT);
            options.set_min_write_buffer_number_to_merge(WRITE_BUFFERS_TO_MERGE);

            options.set_target_file_size_base(TARGET_FILE_BYTES);
            options.set_max_bytes_for_level_base(LEVEL_BASE_BYTES);

            options.set_level_zero_file_num_compaction_trigger(L0_COMPACTION_TRIGGER);
            options.set_level_zero_slowdown_writes_trigger(L0_SLOWDOWN_TRIGGER);
            options.set_level_zero_stop_writes_trigger(L0_STOP_TRIGGER);
            options.set_soft_pending_compaction_bytes_limit(SOFT_PENDING_COMPACTION_BYTES);
            options.set_hard_pending_compaction_bytes_limit(HARD_PENDING_COMPACTION_BYTES);
        })
        .build()
}

/// Every tape column family, tuned, for an instance that serves them all
pub fn bench_store_configs(cache: &Cache) -> Vec<ColumnFamilyDescriptor> {
    tape_store_column_configs(cache)
        .into_iter()
        .map(|config| tuned(config, cache))
        .collect()
}

/// The families of one volume, tuned
fn volume_configs(cache: &Cache, bulk: bool) -> Vec<ColumnFamilyDescriptor> {
    tape_store_column_configs(cache)
        .into_iter()
        .filter(|config| BULK_COLUMN_FAMILIES.contains(&config.name()) == bulk)
        .map(|config| tuned(config, cache))
        .collect()
}

/// The metadata volume's families, tuned
pub fn bench_metadata_configs(cache: &Cache) -> Vec<ColumnFamilyDescriptor> {
    volume_configs(cache, false)
}

/// The bulk volume's families, tuned
pub fn bench_bulk_configs(cache: &Cache) -> Vec<ColumnFamilyDescriptor> {
    volume_configs(cache, true)
}

/// Whatever the tape store said while it finished opening, as a store error
fn opening(error: TapeStoreError) -> StoreError {
    match error {
        TapeStoreError::Store(error) => error,
        other => StoreError::Database(other.to_string()),
    }
}

/// The split rocks store the bench arm runs on, under one root
///
/// `TapeStore::open_primary` with the bench box's sizing: same two volumes in
/// the same two subdirectories, same families on each, and the same pair of
/// index backfills afterwards, so the arm's open time is the node's open time
/// and not a shortcut past it.
pub fn open_bench_split(root: &Path) -> StoreResult<TapeStore<SplitStore>> {
    let meta_dir = root.join(META_SUBDIR);
    let bulk_dir = root.join(BULK_SUBDIR);
    std::fs::create_dir_all(&meta_dir)?;
    std::fs::create_dir_all(&bulk_dir)?;

    let meta_cache = bench_cache();
    let meta = RocksStore::open_with_cf_config(
        &meta_dir,
        bench_db_options(),
        bench_metadata_configs(&meta_cache),
    )?;

    let bulk_cache = bench_cache();
    let bulk = RocksStore::open_with_cf_config(
        &bulk_dir,
        bench_db_options(),
        bench_bulk_configs(&bulk_cache),
    )?;

    let bulk_cfs = BULK_COLUMN_FAMILIES
        .iter()
        .map(|name| (*name).to_string())
        .collect();
    let store = TapeStore::new(SplitStore::new(meta, bulk, bulk_cfs));

    store.ensure_slice_size_index().map_err(opening)?;
    store.ensure_slice_sidecars().map_err(opening)?;
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_store::columns::ALL_COLUMN_FAMILIES;

    // the tuned list is the node's family list, neither shorter nor reordered
    #[test]
    fn every_family_is_tuned() {
        let cache = bench_cache();
        let names: Vec<String> = bench_store_configs(&cache)
            .iter()
            .map(|config| config.name().to_string())
            .collect();
        assert_eq!(names.len(), ALL_COLUMN_FAMILIES.len());
        for name in ALL_COLUMN_FAMILIES {
            assert!(names.iter().any(|tuned| tuned == name), "{name} not tuned");
        }
    }

    // the two volumes partition the family list, as the split store expects
    #[test]
    fn volumes_partition_the_families() {
        let cache = bench_cache();
        let meta = bench_metadata_configs(&cache);
        let bulk = bench_bulk_configs(&cache);
        assert_eq!(meta.len() + bulk.len(), ALL_COLUMN_FAMILIES.len());
        for config in &bulk {
            assert!(BULK_COLUMN_FAMILIES.contains(&config.name()));
        }
        for config in &meta {
            assert!(!BULK_COLUMN_FAMILIES.contains(&config.name()));
        }
    }

    // every family named a payload family is one the node actually declares
    #[test]
    fn payload_families_exist() {
        for family in PAYLOAD_FAMILIES {
            assert!(
                ALL_COLUMN_FAMILIES.contains(family),
                "{family} is not a column family"
            );
        }
    }
}
