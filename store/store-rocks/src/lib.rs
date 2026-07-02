//! store-rocks: RocksDB backend implementation for the store crate
//!
//! This crate provides a RocksDB-based persistent storage backend that implements
//! the `Store` trait from the `store` crate.
//!
//! # Features
//!
//! - **RocksStore**: Production-ready RocksDB implementation
//! - **ColumnFamilyConfig**: Builder API for configuring column families
//! - **Multiple modes**: Read/write, read-only, and secondary instances
//! - **Advanced configuration**: BlockBased and BlobDB support
//!
//! # Example
//!
//! ```no_run
//! use store::{Store, TypedStore};
//! use store_rocks::RocksStore;
//!
//! // Open a database
//! let rocks = RocksStore::open("/tmp/mydb", &["users", "posts"]).unwrap();
//! let store = TypedStore::new(rocks);
//!
//! // Use the store...
//! ```
//!
//! # Advanced Configuration
//!
//! ```no_run
//! use store_rocks::{RocksStore, ColumnFamilyConfig};
//! use rocksdb::Options;
//!
//! let mut db_opts = Options::default();
//! db_opts.create_if_missing(true);
//! db_opts.create_missing_column_families(true);
//!
//! let cf_configs = vec![
//!     ColumnFamilyConfig::new("fixed_keys").with_block_based().build(),
//!     ColumnFamilyConfig::new("large_blobs").with_blob_db(1024 * 1024).build(),
//! ];
//!
//! let rocks = RocksStore::open_with_cf_config("/tmp/mydb", db_opts, cf_configs).unwrap();
//! ```

pub mod config;
mod rocks;
mod split;

pub use config::ColumnFamilyConfig;
pub use rocks::RocksStore;
pub use split::SplitStore;

// Re-export commonly used RocksDB types for convenience
pub use rocksdb::{ColumnFamilyDescriptor, Options};
