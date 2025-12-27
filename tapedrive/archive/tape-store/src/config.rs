//! Column family and database configuration for TapeStore
//!
//! This module provides optimized RocksDB configurations for all column families
//! in the tape-store, using different table types based on the access patterns:
//!
//! - **PlainTable**: Fixed-size keys (8, 32 bytes) for fast point lookups
//! - **BlockBased**: Structured data with bloom filters for range queries
//! - **BlobDB**: Large values (slices up to 32 MiB) to reduce write amplification
//! - **Prefix Extractors**: Enable efficient range scans by prefix
//!
//! # Example
//!
//! ```
//! use tape_store::config::{create_tape_store_configs, create_db_options};
//! use store_rocks::RocksStore;
//!
//! let db_opts = create_db_options();
//! let cf_configs = create_tape_store_configs();
//! let rocks = RocksStore::open_with_cf_config("/data/tapes", db_opts, cf_configs)?;
//! # Ok::<(), store::Error>(())
//! ```

use store_rocks::{ColumnFamilyConfig, ColumnFamilyDescriptor, Options};

// Re-export rocksdb types needed for configuration
use rocksdb;

/// Create optimized column family configurations for all TapeStore column families
///
/// Returns a vector of `ColumnFamilyDescriptor` instances, one for each column family
/// in the tape-store. Each CF is configured based on its access patterns and data characteristics:
///
/// # Column Family Configurations
///
/// ## Fixed-Size Key Indices (PlainTable)
/// - `tapes/by_id` - 8-byte TapeKey
/// - `tapes/active_index` - 8-byte TapeKey
/// - `tapes/by_address` - 32-byte Pubkey
/// - `tracks/by_id` - 8-byte TrackKey
/// - `tracks/by_address` - 32-byte Pubkey
/// - `tracks/by_blob_key` - 32-byte Hash
/// - `assignment/status` - 2-byte SpoolKey
/// - `assignment/progress` - 2-byte SpoolKey
/// - `committee/by_epoch` - 8-byte EpochNumber
///
/// ## Composite Keys with Range Scans (BlockBased + Prefix)
/// - `tracks/by_tape` - 16-byte TapeTrackKey (prefix: 8-byte TapeKey)
/// - `slices/meta` - 10-byte SliceKey (prefix: 8-byte TrackNumber)
/// - `slices/state` - 10-byte SliceKey (prefix: 8-byte TrackNumber)
/// - `pending_recover` - 10-byte RecoveryKey (prefix: 2-byte spool_idx)
/// - `gc_index` - 18-byte GcKey (prefix: 8-byte gc_at timestamp)
///
/// ## Large Values (BlobDB)
/// - `slices/data` - Up to 32 MiB values with 1 MiB threshold
///
/// ## Variable-Size Data (BlockBased)
/// - `meta` - String keys, arbitrary values
///
/// # Performance Expectations
///
/// - **PlainTable**: 20-30% faster reads vs BlockBased, 10-15% less memory
/// - **BlobDB**: 50-70% reduction in write amplification for large slices
/// - **Prefix Extractors**: 10-100x faster prefix scans (depending on selectivity)
///
/// # Example
///
/// ```
/// use tape_store::config::create_tape_store_configs;
///
/// let configs = create_tape_store_configs();
/// assert_eq!(configs.len(), 16); // One for each column family
/// ```
pub fn create_tape_store_configs() -> Vec<ColumnFamilyDescriptor> {
    vec![
        // Meta - variable-size keys and values, infrequent access
        // Use default BlockBased table
        ColumnFamilyConfig::new("meta")
            .with_block_based()
            .build(),

        // Tapes - fixed 8-byte keys, small values
        // PlainTable optimized for point lookups
        ColumnFamilyConfig::new("tapes/by_id")
            .with_plain_table(8)
            .build(),

        // Tape address index - fixed 32-byte Pubkey keys
        ColumnFamilyConfig::new("tapes/by_address")
            .with_plain_table(32)
            .build(),

        // Active tape index - fixed 8-byte keys, unit values
        ColumnFamilyConfig::new("tapes/active_index")
            .with_plain_table(8)
            .build(),

        // Tracks - fixed 8-byte keys, small values
        ColumnFamilyConfig::new("tracks/by_id")
            .with_plain_table(8)
            .build(),

        // Track address index - fixed 32-byte Pubkey keys
        ColumnFamilyConfig::new("tracks/by_address")
            .with_plain_table(32)
            .build(),

        // Tracks by tape - composite 16-byte keys (tape_id + track_id)
        // Need range scans by tape_id prefix (8 bytes)
        ColumnFamilyConfig::new("tracks/by_tape")
            .with_block_based()
            .with_prefix_extractor(8) // TapeKey prefix
            .build(),

        // Track blob key index - fixed 32-byte Hash keys
        ColumnFamilyConfig::new("tracks/by_blob_key")
            .with_plain_table(32)
            .build(),

        // Slice data - VERY large values (up to 32 MiB)
        // BlobDB moves large values out of LSM tree to reduce write amplification
        // Prefix extractor enables range queries by track_id (first 8 bytes)
        ColumnFamilyConfig::new("slices/data")
            .with_blob_db(1024 * 1024) // 1 MiB threshold
            .with_prefix_extractor(8)  // TrackNumber prefix
            .build(),

        // Slice metadata - composite 10-byte keys, small structured values
        // Range scans by track_id prefix (first 8 bytes)
        ColumnFamilyConfig::new("slices/meta")
            .with_block_based()
            .with_prefix_extractor(8) // TrackNumber prefix
            .build(),

        // Slice state - composite 10-byte keys, small structured values
        // Range scans by track_id prefix (first 8 bytes)
        ColumnFamilyConfig::new("slices/state")
            .with_block_based()
            .with_prefix_extractor(8) // TrackNumber prefix
            .build(),

        // Assignment status - fixed 2-byte spool index keys
        // Only 1024 entries max (one per spool)
        ColumnFamilyConfig::new("assignment/status")
            .with_plain_table(2)
            .build(),

        // Assignment progress - fixed 2-byte spool index keys
        ColumnFamilyConfig::new("assignment/progress")
            .with_plain_table(2)
            .build(),

        // Committee - fixed 8-byte epoch keys, small values
        // Infrequent writes (once per epoch)
        ColumnFamilyConfig::new("committee/by_epoch")
            .with_plain_table(8)
            .build(),

        // Pending recovery - composite 10-byte keys (spool_idx + track_id)
        // Range scans by spool_idx prefix (first 2 bytes)
        ColumnFamilyConfig::new("pending_recover")
            .with_block_based()
            .with_prefix_extractor(2) // spool_idx prefix
            .build(),

        // GC index - composite 18-byte keys (gc_at + track_id + spool_idx)
        // Time-ordered range scans by gc_at prefix (first 8 bytes)
        ColumnFamilyConfig::new("gc_index")
            .with_block_based()
            .with_prefix_extractor(8) // gc_at timestamp prefix
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
///
/// # Example
///
/// ```
/// use tape_store::config::create_db_options;
///
/// let opts = create_db_options();
/// // Use with RocksStore::open_with_cf_config()
/// ```
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
        // Should have exactly 16 column families
        assert_eq!(configs.len(), 16);
    }

    #[test]
    fn test_config_names() {
        let configs = create_tape_store_configs();
        let names: Vec<&str> = configs.iter().map(|cf| cf.name()).collect();

        // Verify all expected column families are present
        let expected = vec![
            "meta",
            "tapes/by_id",
            "tapes/by_address",
            "tapes/active_index",
            "tracks/by_id",
            "tracks/by_address",
            "tracks/by_tape",
            "tracks/by_blob_key",
            "slices/data",
            "slices/meta",
            "slices/state",
            "assignment/status",
            "assignment/progress",
            "committee/by_epoch",
            "pending_recover",
            "gc_index",
        ];

        assert_eq!(names, expected);
    }

    #[test]
    fn test_db_options() {
        let opts = create_db_options();
        // Just verify it returns a valid Options instance
        // Actual values are hard to test without accessing internal state
        drop(opts);
    }
}
