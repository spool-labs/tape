//! Storage feature module.
//!
//! Manages persistent storage of slices in RocksDB.

pub mod service;

pub use service::{SliceMeta, StorageError, StorageService, TrackInfo, MERKLE_HEIGHT};
