//! The rocks arm's configuration, sized for the bench box rather than the fleet
//!
//! The fleet's sizing is set for spinning disks behind small memory, and a
//! baseline opened under those ceilings reports the ceilings rather than the
//! engine. The families keep the shapes the node declares and only the sizing is
//! laid over them.
//!
//! Every knob with an opposite number on the reel side is set to match it, so
//! the two arms differ in engine and nothing else: the same codec knob, no rate
//! limiter on either, buffered reads and writes on both, and the write-ahead log
//! left on so the baseline does not promise less than what it is measured
//! against.

use std::path::Path;

use crate::arm::track_data_codec;
use store::Result as StoreResult;
use store_rocks::{
    BlockBasedOptions, Cache, ColumnFamilyConfig, ColumnFamilyDescriptor, DBCompressionType,
    Options, RocksStore, SplitStore,
};
use tape_store::config::{
    tape_store_column_configs, BULK_COLUMN_FAMILIES, BULK_SUBDIR, META_SUBDIR,
};
use tape_store::TapeStore;

/// Cache one RocksDB instance shares across its block reads and its blob reads
///
/// RocksDB consults its own cache before the page cache the reel reads out of,
/// so a small one would have the baseline fault slices back off the filesystem
/// while the reel served them from memory. Sized past the live set of any single
/// store a campaign opens.
pub const CACHE_BYTES: usize = 32 * 1024 * 1024 * 1024;

/// Data block size for the families whose values are payloads
///
/// A block ends at the first value that crosses the boundary, so a family of
/// large values holds one per block whatever this says. What it buys is a
/// shorter index for a scan to walk.
const PAYLOAD_BLOCK_BYTES: usize = 64 * 1024;

/// Data block size for the families whose values are metadata rows
///
/// A point lookup reads a whole block to answer, so a wider block on small rows
/// is read amplification with nothing to show.
const ROW_BLOCK_BYTES: usize = 16 * 1024;

/// Bloom filter bits per key, roughly a 1% false positive rate
const BLOOM_BITS_PER_KEY: f64 = 10.0;

/// Memtable size per column family
///
/// Sized so a bulk family flushes an L0 file worth writing rather than a stream
/// of small ones. The instance-wide ceiling below is what bounds the memory.
const WRITE_BUFFER_BYTES: usize = 256 * 1024 * 1024;

/// Memtables one column family may hold before a write waits on a flush
const WRITE_BUFFER_COUNT: i32 = 6;

/// Immutable memtables merged into one flush
///
/// Halves the L0 file count against flushing each memtable on its own.
const WRITE_BUFFERS_TO_MERGE: i32 = 2;

/// Ceiling on memtable memory across all column families of one instance
///
/// Crossing it flushes the largest memtable rather than stalling writes, so it
/// bounds memory without governing throughput.
const TOTAL_WRITE_BUFFER_BYTES: usize = 8 * 1024 * 1024 * 1024;

/// Cap on total write-ahead-log size
///
/// Crossing it forces column families to flush so the log can be dropped, so it
/// is sized past what a campaign writes rather than against it.
const MAX_TOTAL_WAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// Bytes written to an SST before RocksDB asks the kernel to start writing back
///
/// A hint, not a durability promise: nothing is made durable earlier than it
/// would have been. Without it the page cache absorbs the whole file and hands
/// the bill to one fsync at close, which reads as a stall.
const BYTES_PER_SYNC: u64 = 1024 * 1024;

/// Threads one compaction may split itself across
///
/// Unset, RocksDB drains L0 to L1 on one thread whatever the box has.
const SUBCOMPACTIONS: u32 = 8;

/// L0 files that start a compaction, RocksDB's default
const L0_COMPACTION_TRIGGER: i32 = 4;

/// L0 files at which writes are slowed
///
/// Sized past the campaign rather than switched off: zero disables the governor
/// and builds an L0 no read can walk. The defaults assume a device that cannot
/// drain L0 at device speed.
const L0_SLOWDOWN_TRIGGER: i32 = 48;

/// L0 files at which writes stop until compaction catches up
const L0_STOP_TRIGGER: i32 = 64;

/// Pending compaction bytes at which writes are slowed
///
/// Sized above the campaign's largest dataset, again rather than disabled.
const SOFT_PENDING_COMPACTION_BYTES: usize = 256 * 1024 * 1024 * 1024;

/// Pending compaction bytes at which writes stop
const HARD_PENDING_COMPACTION_BYTES: usize = 1024 * 1024 * 1024 * 1024;

/// Target SST file size at the first level, matched to the memtable
const TARGET_FILE_BYTES: u64 = 256 * 1024 * 1024;

/// Target size of the base level
///
/// The default is smaller than a single L0 file here, which would put every
/// flush into an immediate compaction.
const LEVEL_BASE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Families whose values are payloads rather than metadata rows
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
    // compaction.
    let cpus = std::thread::available_parallelism()
        .map(|threads| threads.get())
        .unwrap_or(4) as i32;
    options.increase_parallelism(cpus);
    options.set_max_background_jobs(cpus);
    options.set_max_subcompactions(SUBCOMPACTIONS);

    // No rate limiter: the fleet paces compaction for a device that cannot serve
    // reads through a compaction storm, and the reel arm compacts unpaced.
    options.set_db_write_buffer_size(TOTAL_WRITE_BUFFER_BYTES);
    options.set_max_total_wal_size(MAX_TOTAL_WAL_BYTES);
    options.set_bytes_per_sync(BYTES_PER_SYNC);

    // Nothing per-family belongs here. A family opened from a descriptor takes
    // its options from that descriptor and reads none of these, so compression,
    // memtables and level shape are all set in `tuned`.
    options
}

/// The table format a family reads through
fn block_options(cache: &Cache, block_bytes: usize) -> BlockBasedOptions {
    let mut block = BlockBasedOptions::default();
    block.set_block_size(block_bytes);
    block.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

    // Index and filter blocks live in the cache, which is bounded, rather than
    // outside it, which is not. Pinning the L0 ones keeps the newest files'
    // filters resident through a scan that would otherwise evict them.
    block.set_cache_index_and_filter_blocks(true);
    block.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block.set_optimize_filters_for_memory(true);

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

            // Whatever the reel arm was asked to declare, so the two engines are
            // compared on the same codec. Set on every family rather than on
            // track_data alone, since a run measures one family at a time.
            let compression = match track_data_codec() {
                "none" => DBCompressionType::None,
                _ => DBCompressionType::Lz4,
            };
            options.set_compression_type(compression);
            options.set_bottommost_compression_type(compression);
            options.set_blob_compression_type(compression);

            // A blob read otherwise consults no cache at all. Same cache as the
            // blocks, so the instance still has one ceiling.
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

/// The split rocks store the bench arm runs on, under one root
///
/// The same two volumes in the same two subdirectories and the same families on
/// each, with the bench box's sizing laid over them.
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
    Ok(TapeStore::new(SplitStore::new(meta, bulk, bulk_cfs)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_store::columns::ALL_COLUMN_FAMILIES;

    // the tuned list is the node's family list, neither shorter nor reordered
    #[test]
    fn all_tuned() {
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
    fn volume_split() {
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
    fn payload_names() {
        for family in PAYLOAD_FAMILIES {
            assert!(
                ALL_COLUMN_FAMILIES.contains(family),
                "{family} is not a column family"
            );
        }
    }
}
