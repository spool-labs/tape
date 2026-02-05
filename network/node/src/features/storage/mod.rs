//! Storage feature module.
//!
//! Manages persistent storage of slices in RocksDB.

pub mod service;

pub use service::{StorageError, StorageService, TrackInfo};
