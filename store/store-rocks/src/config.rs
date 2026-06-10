//! Column family configuration builder for RocksDB
//!
//! This module provides a builder-pattern API for configuring RocksDB column families
//! with different table types and options optimized for different workloads.

use rocksdb::{BlockBasedOptions, ColumnFamilyDescriptor, Options, SliceTransform};

/// Configuration builder for RocksDB column families
///
/// Provides a fluent API to configure column families with different storage
/// formats and options optimized for specific use cases:
///
/// - **BlockBased**: For structured data with bloom filters
/// - **BlobDB**: For very large values (moves data out of LSM tree)
/// - **Prefix Extractors**: For efficient range queries
///
/// All column families use the BlockBased table format: it is the only
/// format that honors the `Store` trait's ordered-iteration contract
/// (`iter_from` from an arbitrary start key). Hash-based formats like
/// PlainTable silently return empty seek iterators for flushed data.
///
/// # Examples
///
/// ```
/// use store_rocks::ColumnFamilyConfig;
///
/// // Large values with BlobDB
/// let config = ColumnFamilyConfig::new("blobs")
///     .with_blob_db(1024 * 1024) // 1 MiB threshold
///     .with_prefix_extractor(8)
///     .build();
///
/// // Structured data with bloom filters
/// let config = ColumnFamilyConfig::new("metadata")
///     .with_block_based()
///     .build();
/// ```
pub struct ColumnFamilyConfig {
    name: String,
    options: Options,
}

impl ColumnFamilyConfig {
    /// Create a new column family configuration with default options
    ///
    /// # Arguments
    /// * `name` - Name of the column family
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// let config = ColumnFamilyConfig::new("my_column");
    /// ```
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            options: Self::default_options(),
        }
    }

    /// Create default options for a column family
    fn default_options() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    /// Configure for structured data with BlockBased table and bloom filters
    ///
    /// BlockBased tables are the default in RocksDB and work well for variable-size
    /// data, large values, and range scans. Includes bloom filters for faster lookups
    /// and caches index/filter blocks in memory.
    ///
    /// # Configuration
    /// - `block_size: 16 KiB`: Size of data blocks
    /// - `bloom_filter(10.0, false)`: 10 bits per key bloom filter
    /// - `cache_index_and_filter_blocks: true`: Keep indices in block cache
    /// - `level_compaction_dynamic_level_bytes: true`: Better space efficiency
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// let config = ColumnFamilyConfig::new("metadata")
    ///     .with_block_based()
    ///     .build();
    /// ```
    pub fn with_block_based(mut self) -> Self {
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_size(16 * 1024); // 16 KiB
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_cache_index_and_filter_blocks(true);

        self.options.set_block_based_table_factory(&block_opts);
        self.options.set_level_compaction_dynamic_level_bytes(true);

        self
    }

    /// Configure for very large values using BlobDB
    ///
    /// BlobDB moves large values out of the LSM tree and stores them in separate
    /// blob files. This reduces write amplification and improves performance for
    /// workloads with large values (e.g., >1 MiB).
    ///
    /// # Arguments
    /// * `min_blob_size` - Minimum value size to store in blob files (in bytes)
    ///
    /// # Configuration
    /// - `enable_blob_files: true`: Enable BlobDB
    /// - `min_blob_size`: Threshold for storing in blob files
    /// - `blob_file_size: 256 MiB`: Size of each blob file
    /// - `blob_compression_type: Lz4`: Compression for blob values
    /// - `enable_blob_gc: true`: Enable garbage collection
    /// - `blob_gc_age_cutoff: 0.25`: GC blobs older than 25% of base
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// // Store values larger than 1 MiB in blob files
    /// let config = ColumnFamilyConfig::new("large_blobs")
    ///     .with_blob_db(1024 * 1024)
    ///     .build();
    /// ```
    pub fn with_blob_db(mut self, min_blob_size: u64) -> Self {
        self.options.set_enable_blob_files(true);
        self.options.set_min_blob_size(min_blob_size);
        self.options.set_blob_file_size(256 * 1024 * 1024); // 256 MiB per blob file
        self.options
            .set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
        self.options.set_enable_blob_gc(true);
        self.options.set_blob_gc_age_cutoff(0.25); // GC blobs older than 25% of base

        self
    }

    /// Set prefix extractor for efficient range queries
    ///
    /// Prefix extractors enable prefix-based bloom filters, which significantly
    /// improve performance for prefix-based range scans (e.g., all keys with a
    /// common prefix like a user ID or timestamp).
    ///
    /// # Arguments
    /// * `prefix_len` - Length of the prefix in bytes
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// // Keys are (user_id: u64, timestamp: u64)
    /// // Use 8-byte prefix for range queries by user_id
    /// let config = ColumnFamilyConfig::new("user_events")
    ///     .with_prefix_extractor(8)
    ///     .build();
    /// ```
    pub fn with_prefix_extractor(mut self, prefix_len: usize) -> Self {
        self.options
            .set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
        self
    }

    /// Set custom database options directly
    ///
    /// Provides low-level access to RocksDB options for advanced use cases.
    ///
    /// # Arguments
    /// * `f` - Function that modifies the options
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// let config = ColumnFamilyConfig::new("custom")
    ///     .with_options(|opts| {
    ///         opts.set_write_buffer_size(128 * 1024 * 1024); // 128 MiB
    ///         opts.set_max_write_buffer_number(4);
    ///     })
    ///     .build();
    /// ```
    pub fn with_options<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Options),
    {
        f(&mut self.options);
        self
    }

    /// Build the column family descriptor
    ///
    /// Consumes the builder and returns a `ColumnFamilyDescriptor` that can be
    /// passed to `RocksStore::open_with_cf_config`.
    ///
    /// # Example
    /// ```
    /// use store_rocks::ColumnFamilyConfig;
    ///
    /// let descriptor = ColumnFamilyConfig::new("my_column")
    ///     .with_block_based()
    ///     .build();
    /// ```
    pub fn build(self) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(self.name, self.options)
    }

    /// Get the name of this column family
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ColumnFamilyConfig::new("test");
        assert_eq!(config.name(), "test");

        let descriptor = config.build();
        assert_eq!(descriptor.name(), "test");
    }

    #[test]
    fn test_block_based_config() {
        let config = ColumnFamilyConfig::new("structured").with_block_based();
        let descriptor = config.build();
        assert_eq!(descriptor.name(), "structured");
    }

    #[test]
    fn test_blob_db_config() {
        let config = ColumnFamilyConfig::new("large_values").with_blob_db(1024 * 1024);
        let descriptor = config.build();
        assert_eq!(descriptor.name(), "large_values");
    }

    #[test]
    fn test_prefix_extractor_config() {
        let config = ColumnFamilyConfig::new("range_scan").with_prefix_extractor(8);
        let descriptor = config.build();
        assert_eq!(descriptor.name(), "range_scan");
    }

    #[test]
    fn test_chained_config() {
        let config = ColumnFamilyConfig::new("complex")
            .with_block_based()
            .with_blob_db(2 * 1024 * 1024)
            .with_prefix_extractor(16);

        let descriptor = config.build();
        assert_eq!(descriptor.name(), "complex");
    }

    #[test]
    fn test_custom_options() {
        let config = ColumnFamilyConfig::new("custom").with_options(|opts| {
            opts.set_write_buffer_size(64 * 1024 * 1024);
        });

        let descriptor = config.build();
        assert_eq!(descriptor.name(), "custom");
    }
}
