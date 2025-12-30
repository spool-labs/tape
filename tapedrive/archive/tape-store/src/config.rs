//! Column family and database configuration for TapeStore
//!
//! This module provides optimized RocksDB configurations for all column families
//! in the tape-store, using different table types based on the access patterns:
//!
//! - **PlainTable**: Fixed-size keys (2, 8, 32 bytes) for fast point lookups
//! - **BlockBased**: Structured data with bloom filters for range queries
//! - **BlobDB**: Large values (slices up to 32 MiB) to reduce write amplification
//! - **Prefix Extractors**: Enable efficient range scans by prefix
//!
//! # Example
//!
//! ```no_run
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
/// # Column Family Configurations (9 total)
///
/// ## Fixed-Size Key Indices (PlainTable)
/// - `tracks` - 32-byte Pubkey keys
/// - `spools/assigned` - 2-byte SpoolKey
/// - `committee` - 8-byte EpochNumber
///
/// ## Composite Keys with Range Scans (BlockBased + Prefix)
/// - `slices/meta` - 34-byte SliceKey (prefix: 2-byte spool_idx)
/// - `pending/recover` - 34-byte SliceKey (prefix: 2-byte spool_idx)
/// - `pending/handoff` - 34-byte SliceKey (prefix: 2-byte spool_idx)
/// - `gc/scheduled` - 42-byte GcKey (prefix: 8-byte timestamp)
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
/// assert_eq!(configs.len(), 9); // One for each column family
/// ```
pub fn create_tape_store_configs() -> Vec<ColumnFamilyDescriptor> {
    vec![
        // Meta - variable-size keys and values, infrequent access
        // Use default BlockBased table
        ColumnFamilyConfig::new("meta")
            .with_block_based()
            .build(),

        // Tracks - 32-byte Pubkey keys, small TrackInfo values
        ColumnFamilyConfig::new("tracks")
            .with_plain_table(32)
            .build(),

        // Slice data - VERY large values (up to 32 MiB)
        // BlobDB moves large values out of LSM tree to reduce write amplification
        // Prefix extractor enables range queries by spool_idx (first 2 bytes)
        ColumnFamilyConfig::new("slices/data")
            .with_blob_db(1024 * 1024) // 1 MiB threshold
            .with_prefix_extractor(2)  // spool_idx prefix
            .build(),

        // Slice metadata - 34-byte SliceKey, small structured values
        // Range scans by spool_idx prefix (first 2 bytes)
        ColumnFamilyConfig::new("slices/meta")
            .with_block_based()
            .with_prefix_extractor(2) // spool_idx prefix
            .build(),

        // Spools assigned - 2-byte spool index keys
        // Only ~1024 entries max (one per spool we own)
        ColumnFamilyConfig::new("spools/assigned")
            .with_plain_table(2)
            .build(),

        // Committee cache - 8-byte epoch keys
        // Infrequent writes (once per epoch)
        ColumnFamilyConfig::new("committee")
            .with_plain_table(8)
            .build(),

        // Pending recovery - 34-byte SliceKey
        // Range scans by spool_idx prefix (first 2 bytes)
        ColumnFamilyConfig::new("pending/recover")
            .with_block_based()
            .with_prefix_extractor(2) // spool_idx prefix
            .build(),

        // Pending handoff - 34-byte SliceKey
        // Range scans by spool_idx prefix (first 2 bytes)
        ColumnFamilyConfig::new("pending/handoff")
            .with_block_based()
            .with_prefix_extractor(2) // spool_idx prefix
            .build(),

        // GC scheduled - 42-byte GcKey (timestamp + spool_idx + track_address)
        // Time-ordered range scans by timestamp prefix (first 8 bytes)
        ColumnFamilyConfig::new("gc/scheduled")
            .with_block_based()
            .with_prefix_extractor(8) // timestamp prefix
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
        // Should have exactly 9 column families
        assert_eq!(configs.len(), 9);
    }

    #[test]
    fn test_config_names() {
        let configs = create_tape_store_configs();
        let names: Vec<&str> = configs.iter().map(|cf| cf.name()).collect();

        // Verify all expected column families are present
        let expected = vec![
            "meta",
            "tracks",
            "slices/data",
            "slices/meta",
            "spools/assigned",
            "committee",
            "pending/recover",
            "pending/handoff",
            "gc/scheduled",
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
