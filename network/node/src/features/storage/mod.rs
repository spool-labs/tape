//! Storage feature module.
//!
//! Manages persistent storage of slices in RocksDB.

pub mod service;

pub use service::{
    PrimarySliceData, RecoverySliceData, StorageError, StorageService, TrackInfo,
};
