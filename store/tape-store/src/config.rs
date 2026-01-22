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
/// # Column Family Configurations (12 total)
///
/// ## Metadata Columns (PlainTable/BlockBased)
/// - `meta` - String keys, arbitrary values (BlockBased)
/// - `slice_info` - 32-byte Pubkey keys (PlainTable)
/// - `tape_info` - 32-byte Pubkey keys (PlainTable)
/// - `track_info` - 32-byte Pubkey keys (PlainTable)
///
/// ## Sync Columns
/// - `sync_cursor` - Singleton (0-byte key) (BlockBased)
/// - `gc` - String keys ("started", "completed") (BlockBased)
///
/// ## Epoch-Namespaced Spool Columns (BlockBased + Prefix)
/// - `spool/assigned` - 10-byte SpoolEpochKey (8-byte epoch prefix)
/// - `spool/sync_progress` - 10-byte SpoolEpochKey (8-byte epoch prefix)
/// - `spool/pending_recovery` - 43-byte PendingRecoveryKey (8-byte epoch prefix)
///
/// ## Slice Data Columns (BlobDB)
/// - `spool/primary_slices` - 34-byte SliceKey (2-byte spool prefix)
/// - `spool/recovery_slices` - 34-byte SliceKey (2-byte spool prefix)
///
/// ## Committee Column
/// - `committee` - 8-byte EpochKey (PlainTable)
pub fn create_tape_store_configs() -> Vec<ColumnFamilyDescriptor> {
    vec![
        // Meta - variable-size keys and values, infrequent access
        ColumnFamilyConfig::new("meta")
            .with_block_based()
            .build(),

        // Slice info - 32-byte Pubkey keys, variable-size SliceInfo values
        ColumnFamilyConfig::new("slice_info")
            .with_plain_table(32)
            .build(),

        // Tape info - 32-byte Pubkey keys, small TapeInfo values
        ColumnFamilyConfig::new("tape_info")
            .with_plain_table(32)
            .build(),

        // Track info - 32-byte Pubkey keys, TrackInfo values
        ColumnFamilyConfig::new("track_info")
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

        // Spool status - 10-byte SpoolEpochKey (epoch BE + spool_id BE)
        // 8-byte epoch prefix for range cleanup
        ColumnFamilyConfig::new("spool_status")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Sync cursors - 10-byte SpoolEpochKey
        // 8-byte epoch prefix for range cleanup
        ColumnFamilyConfig::new("sync_cursors")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Recovery queue - 43-byte PendingRecoveryKey
        // 8-byte epoch prefix for range cleanup
        ColumnFamilyConfig::new("recovery_queue")
            .with_block_based()
            .with_prefix_extractor(8)
            .build(),

        // Primary slices - 34-byte SliceKey, large (~1MB) values
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("primary_slices")
            .with_blob_db(256 * 1024) // 256 KiB threshold
            .with_prefix_extractor(2)
            .build(),

        // Recovery slices - 34-byte SliceKey, large (~1MB) values
        // 2-byte spool prefix for iteration by spool
        ColumnFamilyConfig::new("recovery_slices")
            .with_blob_db(256 * 1024) // 256 KiB threshold
            .with_prefix_extractor(2)
            .build(),

        // Committee - 8-byte EpochKey, CommitteeCache values
        ColumnFamilyConfig::new("committee")
            .with_plain_table(8)
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
    opts.set_ratelimiter(100 * 1024 * 1024, 100_000, 10);

    opts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_count() {
        let configs = create_tape_store_configs();
        // Should have exactly 12 column families
        assert_eq!(configs.len(), 12);
    }

    #[test]
    fn test_config_names() {
        let configs = create_tape_store_configs();
        let names: Vec<&str> = configs.iter().map(|cf| cf.name()).collect();

        // Verify all expected column families are present
        let expected = vec![
            "meta",
            "slice_info",
            "tape_info",
            "track_info",
            "sync_cursor",
            "gc",
            "spool_status",
            "sync_cursors",
            "recovery_queue",
            "primary_slices",
            "recovery_slices",
            "committee",
        ];

        assert_eq!(names, expected);
    }

    #[test]
    fn test_db_options() {
        let opts = create_db_options();
        // Just verify it returns a valid Options instance
        drop(opts);
    }
}
