//! Column family and database configuration for TapeStore
//!
//! This module provides optimized RocksDB configurations for all column families
//! in the tape-store, using different table types based on the access patterns:
//!
//! - **PlainTable**: Fixed-size keys for fast point lookups
//! - **BlockBased**: Structured data with bloom filters for range queries
//! - **BlobDB**: Large values (slices up to 32 MiB) to reduce write amplification
//! - **Prefix Extractors**: Enable efficient range scans by prefix

use store_rocks::{ColumnFamilyConfig, ColumnFamilyDescriptor, Options};

// Re-export rocksdb types needed for configuration
use rocksdb;

/// Create optimized column family configurations for all TapeStore column families
///
/// Returns a vector of `ColumnFamilyDescriptor` instances, one for each column family
/// in the tape-store. Each CF is configured based on its access patterns and data characteristics.
///
/// # Column Family Configurations
///
/// ## Metadata Columns
/// - `meta` - String keys, arbitrary values (BlockBased)
/// - `tape` - 32-byte Address keys (PlainTable)
/// - `track` - 32-byte Address keys, packed compressed-track values (PlainTable)
/// - `track_lookup` - 72-byte ordered tape track index with 32-byte tape prefix (BlockBased)
/// - `track_data` - 32-byte Address keys, local track payload values (PlainTable)
/// - `object_info` - 32-byte Address keys (PlainTable)
///
/// ## Sync Columns
/// - `sync_cursor` - Singleton (0-byte key) (BlockBased)
/// - `gc` - String keys (BlockBased)
///
/// ## Spool Columns (NOT epoch-namespaced)
/// - `spool_status` - 2-byte SpoolIndexKey (PlainTable)
/// - `spool_pending_repair` - 34-byte SliceKey with 2-byte spool prefix (BlockBased)
/// - `spool_pending_recovery` - 34-byte SliceKey with 2-byte spool prefix (BlockBased)
/// - `spool_sync_cursor` - 2-byte SpoolIndexKey (PlainTable)
///
/// ## Slice Data Column (BlobDB)
/// - `slice` - 34-byte SliceKey, large (~1MB) values (BlobDB with 2-byte prefix)
///
/// ## Event Log Column
/// - `event_log` - 20-byte EventLogKey with 8-byte epoch prefix (BlockBased)
///
/// ## Vote Coordination Columns
/// - `vote_sig` - 96-byte key with 64-byte candidate/group prefix (BlockBased)
///
/// ## Snapshot Coordination Columns
/// - `snapshot_artifact` - 24-byte key with 16-byte group prefix (BlobDB)
pub fn create_tape_store_configs() -> Vec<ColumnFamilyDescriptor> {
    vec![
        // Meta - variable-size keys and values, infrequent access
        ColumnFamilyConfig::new("meta")
            .with_block_based()
            .build(),

        // Tape - 32-byte Address keys, small TapeInfo values
        ColumnFamilyConfig::new("tape")
            .with_plain_table(32)
            .build(),

        // Track - 32-byte Address keys, PackedTrack values
        ColumnFamilyConfig::new("track")
            .with_plain_table(32)
            .build(),

        // Track lookup - ordered by (tape, track_number, key)
        // 32-byte tape prefix for efficient per-tape scans
        ColumnFamilyConfig::new("track_lookup")
            .with_block_based()
            .with_prefix_extractor(32)
            .build(),

        // Track data - 32-byte Address keys, local payload values
        ColumnFamilyConfig::new("track_data")
            .with_plain_table(32)
            .build(),

        // Object info - 32-byte Address keys, ObjectInfo values
        ColumnFamilyConfig::new("object_info")
            .with_plain_table(32)
            .build(),

        // Sync cursor - singleton (empty key)
        ColumnFamilyConfig::new("sync_cursor")
            .with_block_based()
            .build(),

        // GC progress - String keys
        ColumnFamilyConfig::new("gc")
            .with_block_based()
            .build(),

        // Spool status - 2-byte SpoolIndexKey (PlainTable)
        ColumnFamilyConfig::new("spool_status")
            .with_plain_table(2)
            .build(),

        // Spool pending repair - 34-byte SliceKey
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("spool_pending_repair")
            .with_block_based()
            .with_prefix_extractor(2)
            .build(),

        // Spool pending recovery - 34-byte SliceKey
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("spool_pending_recovery")
            .with_block_based()
            .with_prefix_extractor(2)
            .build(),

        // Slice - 34-byte SliceKey, large (~1MB) values
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("slice")
            .with_blob_db(256 * 1024) // 256 KiB threshold
            .with_prefix_extractor(2)
            .build(),

        // Spool sync progress - 2-byte SpoolIndexKey (PlainTable)
        ColumnFamilyConfig::new("spool_sync_cursor")
            .with_plain_table(2)
            .build(),

        // Event log - 20-byte EventLogKey (epoch 8B + slot 8B + seq 4B)
        // 8-byte epoch prefix for efficient per-epoch scanning and deletion
        ColumnFamilyConfig::new("event_log")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Vote signatures - prefix scans by (voting_epoch, kind, target_epoch, hash, group)
        ColumnFamilyConfig::new("vote_sig")
            .with_block_based()
            .with_prefix_extractor(64)
            .build(),

        // Snapshot artifacts - staged local slices indexed by (epoch, group, chunk)
        ColumnFamilyConfig::new("snapshot_artifact")
            .with_blob_db(256 * 1024)
            .with_prefix_extractor(16)
            .build(),
    ]
}

/// Create database-wide options for TapeStore
///
/// Returns a configured `Options` instance with settings optimized for the
/// TapeStore workload:
///
/// - **Write Buffers**: 64 MiB per CF, up to 4 buffers
/// - **Parallelism**: Scales with CPU count
/// - **Compression**: LZ4 for fast compression/decompression
/// - **Rate Limiting**: 100 MB/s to prevent I/O spikes during compaction
pub fn create_db_options() -> Options {
    create_db_options_with_compaction_rate_limit_mb_per_sec(100)
}

pub fn create_db_options_with_compaction_rate_limit_mb_per_sec(
    rate_limit_mb_per_sec: u64,
) -> Options {
    let mut opts = Options::default();

    // Basic database options
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Memory and write buffer tuning
    // 64 MiB per write buffer, up to 4 buffers per CF
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(4);
    opts.set_min_write_buffer_number_to_merge(2);

    // Parallelism - scale with CPU count
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4) as i32;
    opts.increase_parallelism(cpus);
    opts.set_max_background_jobs(cpus);

    // Compression - LZ4 is fast and good enough
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

    // Rate limiting for compaction to prevent I/O spikes
    // 100 MB/s should be gentle on the system
    // set_ratelimiter(rate_bytes_per_sec, refill_period_us, fairness)
    let rate_limit_bytes_per_sec = rate_limit_mb_per_sec
        .saturating_mul(1024 * 1024)
        .min(i64::MAX as u64) as i64;
    opts.set_ratelimiter(rate_limit_bytes_per_sec, 100_000, 10);

    opts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_count() {
        let configs = create_tape_store_configs();
        assert_eq!(configs.len(), 16);
    }

    #[test]
    fn test_config_names() {
        let configs = create_tape_store_configs();
        let names: Vec<&str> = configs.iter().map(|cf| cf.name()).collect();

        let expected = vec![
            "meta",
            "tape",
            "track",
            "track_lookup",
            "track_data",
            "object_info",
            "sync_cursor",
            "gc",
            "spool_status",
            "spool_pending_repair",
            "spool_pending_recovery",
            "slice",
            "spool_sync_cursor",
            "event_log",
            "vote_sig",
            "snapshot_artifact",
        ];

        assert_eq!(names, expected);
    }

    #[test]
    fn test_db_options() {
        let opts = create_db_options();
        drop(opts);
    }
}
