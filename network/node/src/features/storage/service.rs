//! Storage service for managing slice storage and retrieval.

use std::path::Path;
use std::sync::Arc;

use tape_core::spooler::SpoolIndex;
use tape_crypto::Pubkey;
use store::Store;
use store_rocks::RocksStore;
use tape_store::ops::{SliceOps, TrackOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::TapeStore;

pub use tape_store::types::TrackInfo;

use crate::metrics::NodeMetrics;

/// Error type for storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage initialization failed: {0}")]
    InitFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(#[from] store::Error),

    #[error("tape store error: {0}")]
    TapeStore(#[from] tape_store::error::TapeStoreError),
}

/// Storage service for managing slice data.
///
/// Wraps a `TapeStore` and provides methods for storing and retrieving
/// slices with optional metrics tracking.
pub struct StorageService<S: Store = RocksStore> {
    /// The underlying tape store (public for direct access to ops traits).
    pub store: TapeStore<S>,
    /// Optional metrics for tracking operations.
    metrics: Option<Arc<NodeMetrics>>,
}

impl StorageService<RocksStore> {
    /// Open a storage service with RocksDB backend at the given path.
    ///
    /// Creates the directory if it doesn't exist.
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        if !path.exists() {
            std::fs::create_dir_all(path)?;
            tracing::info!(path = %path.display(), "Created storage directory");
        }

        let store = TapeStore::open_primary(path)
            .map_err(|e| StorageError::InitFailed(e.to_string()))?;

        tracing::info!(path = %path.display(), "Storage service initialized with RocksDB");

        Ok(Self {
            store,
            metrics: None,
        })
    }
}

impl<S: Store> StorageService<S> {
    /// Create a storage service with the given store.
    pub fn new(store: TapeStore<S>) -> Self {
        Self {
            store,
            metrics: None,
        }
    }

    /// Add metrics tracking to this service.
    pub fn with_metrics(mut self, metrics: Arc<NodeMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Store a slice.
    pub fn put_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
        data: Vec<u8>,
    ) -> Result<(), StorageError> {
        let data_len = data.len();
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        self.store.put_slice(spool_idx.into(), track_pubkey, data)?;

        if let Some(metrics) = &self.metrics {
            metrics.slices_stored_total.inc();
            metrics.bytes_stored_total.add(data_len as i64);
        }

        Ok(())
    }

    /// Retrieve a slice.
    pub fn get_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        let result = self.store.get_slice(spool_idx.into(), track_pubkey)?;

        if let Some(ref data) = result {
            if let Some(metrics) = &self.metrics {
                metrics.slices_retrieved_total.inc();
                metrics.bytes_retrieved_total.add(data.len() as i64);
            }
        }

        Ok(result)
    }

    /// Delete a slice.
    pub fn delete_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_slice(spool_idx.into(), track_pubkey)?;
        Ok(())
    }

    /// Store track metadata.
    pub fn put_track(
        &self,
        track_address: Pubkey,
        info: TrackInfo,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.put_track(track_pubkey, info)?;
        Ok(())
    }

    /// Get track metadata.
    pub fn get_track(
        &self,
        track_address: Pubkey,
    ) -> Result<Option<TrackInfo>, StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        Ok(self.store.get_track(track_pubkey)?)
    }

    /// Delete track metadata.
    pub fn delete_track(
        &self,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_track(track_pubkey)?;
        Ok(())
    }

    /// Shutdown storage gracefully.
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        tracing::info!("Storage service shutting down");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn create_test_service() -> StorageService<MemoryStore> {
        StorageService::new(TapeStore::new(MemoryStore::new()))
    }

    #[test]
    fn test_slice_roundtrip() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;
        let data = vec![0xAB; 1024];

        service.put_slice(spool_idx, track, data.clone()).unwrap();

        let retrieved = service
            .get_slice(spool_idx, track)
            .unwrap()
            .expect("slice should exist");

        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_get_nonexistent_slice() {
        let service = create_test_service();
        let result = service.get_slice(0, Pubkey::new_unique()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_slice() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;

        service.put_slice(spool_idx, track, vec![0xAB; 100]).unwrap();
        assert!(service.get_slice(spool_idx, track).unwrap().is_some());

        service.delete_slice(spool_idx, track).unwrap();
        assert!(service.get_slice(spool_idx, track).unwrap().is_none());
    }
}
