//! Storage service for managing slice storage and retrieval.

use std::path::Path;
use std::sync::Arc;

use solana_pubkey::Pubkey;
use store::Store;
use store_rocks::RocksStore;
use tape_store::types::Pubkey as StorePubkey;
use tape_store::TapeStore;

// Re-export types from tape_store for use by routes
pub use tape_store::ops::{Compression, SliceMeta, SliceOps, MERKLE_HEIGHT};

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
    /// The underlying tape store.
    store: TapeStore<S>,
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

    /// Store a slice with its metadata.
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index (same as slice index)
    /// * `track_address` - The track's on-chain address
    /// * `data` - The slice data
    /// * `meta` - The slice metadata including merkle proof
    pub fn put_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
        data: Vec<u8>,
        meta: SliceMeta,
    ) -> Result<(), StorageError> {
        let data_len = data.len();

        // Convert solana_pubkey::Pubkey to tape_store::types::Pubkey
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        self.store.put_slice(spool_idx, track_pubkey, data, meta)?;

        if let Some(metrics) = &self.metrics {
            metrics.slices_stored_total.inc();
            metrics.bytes_stored_total.add(data_len as i64);
        }

        Ok(())
    }

    /// Retrieve a slice and its metadata.
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index (same as slice index)
    /// * `track_address` - The track's on-chain address
    ///
    /// # Returns
    /// Tuple of (data, metadata) if found, None otherwise.
    pub fn get_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<Option<(Vec<u8>, SliceMeta)>, StorageError> {
        // Convert solana_pubkey::Pubkey to tape_store::types::Pubkey
        let track_pubkey = StorePubkey::new(track_address.to_bytes());

        let result = self.store.get_slice(spool_idx, track_pubkey)?;

        if let Some((ref data, _)) = result {
            if let Some(metrics) = &self.metrics {
                metrics.slices_retrieved_total.inc();
                metrics.bytes_retrieved_total.add(data.len() as i64);
            }
        }

        Ok(result)
    }

    /// Delete a slice.
    ///
    /// # Arguments
    /// * `spool_idx` - The spool index
    /// * `track_address` - The track's on-chain address
    pub fn delete_slice(
        &self,
        spool_idx: u16,
        track_address: Pubkey,
    ) -> Result<(), StorageError> {
        let track_pubkey = StorePubkey::new(track_address.to_bytes());
        self.store.delete_slice(spool_idx, track_pubkey)?;
        Ok(())
    }

    /// Shutdown storage gracefully.
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        tracing::info!("Storage service shutting down");
        // RocksDB handles cleanup on drop
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_crypto::Hash;

    fn create_test_service() -> StorageService<MemoryStore> {
        StorageService::new(TapeStore::new(MemoryStore::new()))
    }

    fn create_test_meta() -> SliceMeta {
        SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::None,
            received_at: 123456789,
        }
    }

    #[test]
    fn test_put_get_slice() {
        let service = create_test_service();
        let track = Pubkey::new_unique();
        let spool_idx = 42u16;
        let data = vec![0xAB; 1024];
        let meta = create_test_meta();

        service
            .put_slice(spool_idx, track, data.clone(), meta.clone())
            .unwrap();

        let (retrieved_data, retrieved_meta) = service
            .get_slice(spool_idx, track)
            .unwrap()
            .expect("slice should exist");

        assert_eq!(retrieved_data, data);
        assert_eq!(retrieved_meta.len, meta.len);
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
        let spool_idx = 42u16;

        service
            .put_slice(spool_idx, track, vec![0xAB; 100], create_test_meta())
            .unwrap();

        assert!(service.get_slice(spool_idx, track).unwrap().is_some());

        service.delete_slice(spool_idx, track).unwrap();

        assert!(service.get_slice(spool_idx, track).unwrap().is_none());
    }
}
