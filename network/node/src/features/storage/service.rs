//! Storage service for managing slice storage and retrieval.

use std::path::Path;
use std::sync::Arc;

use tape_core::spooler::SpoolIndex;
use tape_crypto::Pubkey;
use store::Store;
use store_rocks::RocksStore;
use tape_store::ops::{SliceDataOps, TrackInfoOps};
use tape_store::types::Pubkey as StorePubkey;
use tape_store::TapeStore;

pub use tape_store::types::{PrimarySliceData, RecoverySliceData, TrackInfo};

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
/// primary and recovery slices with optional metrics tracking.
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

    /// Store a primary slice.
    pub fn put_primary_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
        data: PrimarySliceData,
    ) -> Result<(), StorageError> {
        let data_len = data.symbols.len();
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        self.store.put_primary_slice(spool_idx.into(), track_pubkey, data)?;

        if let Some(metrics) = &self.metrics {
            metrics.slices_stored_total.inc();
            metrics.bytes_stored_total.add(data_len as i64);
        }

        Ok(())
    }

    /// Retrieve a primary slice.
    pub fn get_primary_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<Option<PrimarySliceData>, StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        let result = self.store.get_primary_slice(spool_idx.into(), track_pubkey)?;

        if let Some(ref data) = result {
            if let Some(metrics) = &self.metrics {
                metrics.slices_retrieved_total.inc();
                metrics.bytes_retrieved_total.add(data.symbols.len() as i64);
            }
        }

        Ok(result)
    }

    /// Delete a primary slice.
    pub fn delete_primary_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_primary_slice(spool_idx.into(), track_pubkey)?;
        Ok(())
    }

    /// Store a recovery slice.
    pub fn put_recovery_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
        data: RecoverySliceData,
    ) -> Result<(), StorageError> {
        let data_len = data.symbols.len();
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        self.store.put_recovery_slice(spool_idx.into(), track_pubkey, data)?;

        if let Some(metrics) = &self.metrics {
            metrics.slices_stored_total.inc();
            metrics.bytes_stored_total.add(data_len as i64);
        }

        Ok(())
    }

    /// Retrieve a recovery slice.
    pub fn get_recovery_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<Option<RecoverySliceData>, StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        let result = self.store.get_recovery_slice(spool_idx.into(), track_pubkey)?;

        if let Some(ref data) = result {
            if let Some(metrics) = &self.metrics {
                metrics.slices_retrieved_total.inc();
                metrics.bytes_retrieved_total.add(data.symbols.len() as i64);
            }
        }

        Ok(result)
    }

    /// Delete a recovery slice.
    pub fn delete_recovery_slice(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_recovery_slice(spool_idx.into(), track_pubkey)?;
        Ok(())
    }

    /// Store primary and recovery slices atomically.
    pub fn put_slices(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
        primary: PrimarySliceData,
        recovery: RecoverySliceData,
    ) -> Result<(), StorageError> {
        let total_len = primary.symbols.len() + recovery.symbols.len();
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        self.store.put_both_slices(spool_idx.into(), track_pubkey, primary, recovery)?;

        if let Some(metrics) = &self.metrics {
            metrics.slices_stored_total.add(2);
            metrics.bytes_stored_total.add(total_len as i64);
        }

        Ok(())
    }

    /// Delete primary and recovery slices atomically.
    pub fn delete_slices(
        &self,
        spool_idx: SpoolIndex,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_both_slices(spool_idx.into(), track_pubkey)?;
        Ok(())
    }

    /// Store track metadata.
    pub fn put_track_info(
        &self,
        track_address: Pubkey,
        info: TrackInfo,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.put_track_info(track_pubkey, info)?;
        Ok(())
    }

    /// Get track metadata.
    pub fn get_track_info(
        &self,
        track_address: Pubkey,
    ) -> Result<Option<TrackInfo>, StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        Ok(self.store.get_track_info(track_pubkey)?)
    }

    /// Delete track metadata.
    pub fn delete_track_info(
        &self,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_track_info(track_pubkey)?;
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
    fn test_primary_slice_roundtrip() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;
        let data = PrimarySliceData::new(vec![0xAB; 1024], 0);

        service.put_primary_slice(spool_idx, track, data.clone()).unwrap();

        let retrieved = service
            .get_primary_slice(spool_idx, track)
            .unwrap()
            .expect("slice should exist");

        assert_eq!(retrieved.symbols, data.symbols);
        assert_eq!(retrieved.padding_len, 0);
    }

    #[test]
    fn test_recovery_slice_roundtrip() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;
        let data = RecoverySliceData::new(vec![0xCD; 1024], 0);

        service.put_recovery_slice(spool_idx, track, data.clone()).unwrap();

        let retrieved = service
            .get_recovery_slice(spool_idx, track)
            .unwrap()
            .expect("slice should exist");

        assert_eq!(retrieved.symbols, data.symbols);
    }

    #[test]
    fn test_get_nonexistent_slice() {
        let service = create_test_service();
        let result = service.get_primary_slice(0, Pubkey::new_unique()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_primary_slice() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;

        service
            .put_primary_slice(spool_idx, track, PrimarySliceData::new(vec![0xAB; 100], 0))
            .unwrap();

        assert!(service.get_primary_slice(spool_idx, track).unwrap().is_some());

        service.delete_primary_slice(spool_idx, track).unwrap();

        assert!(service.get_primary_slice(spool_idx, track).unwrap().is_none());
    }

    #[test]
    fn test_put_delete_slices_atomic() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx: SpoolIndex = 42;

        let primary = PrimarySliceData::new(vec![0xAB; 100], 0);
        let recovery = RecoverySliceData::new(vec![0xCD; 100], 0);

        service.put_slices(spool_idx, track, primary, recovery).unwrap();

        assert!(service.get_primary_slice(spool_idx, track).unwrap().is_some());
        assert!(service.get_recovery_slice(spool_idx, track).unwrap().is_some());

        service.delete_slices(spool_idx, track).unwrap();

        assert!(service.get_primary_slice(spool_idx, track).unwrap().is_none());
        assert!(service.get_recovery_slice(spool_idx, track).unwrap().is_none());
    }
}
