//! Storage service for managing slice storage and retrieval.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use solana_pubkey::Pubkey;
use store::Store;
use store_rocks::RocksStore;
use tape_store::types::Pubkey as StorePubkey;
use tape_store::TapeStore;

// Re-export types from tape_store for use by routes
pub use tape_store::ops::{Compression, SliceMeta, SliceOps, MERKLE_HEIGHT};

use crate::config::NodeConfig;
use crate::metrics::NodeMetrics;

/// Error type for storage operations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage initialization failed: {0}")]
    InitFailed(String),

    #[error("storage path does not exist: {0}")]
    PathNotFound(PathBuf),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {0}")]
    Database(#[from] store::Error),

    #[error("tape store error: {0}")]
    TapeStore(#[from] tape_store::error::TapeStoreError),
}

/// Storage service for managing slice data.
///
/// Wraps `TapeStore<RocksStore>` and provides methods for storing
/// and retrieving slices with metrics tracking.
pub struct StorageService<S: Store = RocksStore> {
    /// The underlying tape store.
    store: TapeStore<S>,
    /// Path to the storage directory.
    storage_path: PathBuf,
    /// Storage capacity in bytes.
    capacity: u64,
    /// Reference to node metrics.
    metrics: Arc<NodeMetrics>,
}

impl StorageService<RocksStore> {
    /// Create a new storage service with RocksDB backend.
    ///
    /// # Arguments
    /// * `config` - Node configuration containing storage settings
    /// * `metrics` - Node metrics for tracking storage operations
    pub fn new(config: &NodeConfig, metrics: Arc<NodeMetrics>) -> Result<Self, StorageError> {
        let storage_path = config.storage_path.clone();

        // Create storage directory if it doesn't exist
        if !storage_path.exists() {
            std::fs::create_dir_all(&storage_path)?;
            tracing::info!(path = %storage_path.display(), "Created storage directory");
        }

        // Open the TapeStore with RocksDB
        let store = TapeStore::open_primary(&storage_path)
            .map_err(|e| StorageError::InitFailed(e.to_string()))?;

        tracing::info!(
            path = %storage_path.display(),
            capacity = config.storage_capacity,
            "Storage service initialized with RocksDB"
        );

        Ok(Self {
            store,
            storage_path,
            capacity: config.storage_capacity,
            metrics,
        })
    }
}

impl<S: Store> StorageService<S> {
    /// Create a storage service with a custom store backend.
    ///
    /// This is primarily used for testing with MemoryStore.
    pub fn with_store(
        store: TapeStore<S>,
        storage_path: PathBuf,
        capacity: u64,
        metrics: Arc<NodeMetrics>,
    ) -> Self {
        Self {
            store,
            storage_path,
            capacity,
            metrics,
        }
    }

    /// Get the storage path.
    pub fn storage_path(&self) -> &Path {
        &self.storage_path
    }

    /// Get the storage capacity.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Check if storage is healthy.
    pub fn is_healthy(&self) -> bool {
        self.storage_path.exists() && self.storage_path.is_dir()
    }

    /// Initialize storage (placeholder for any startup tasks).
    pub async fn initialize(&self) -> Result<(), StorageError> {
        tracing::info!(
            path = %self.storage_path.display(),
            capacity = self.capacity,
            "Storage service initialized"
        );
        Ok(())
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

        self.metrics.slices_stored_total.inc();
        self.metrics.bytes_stored_total.add(data_len as i64);

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
            self.metrics.slices_retrieved_total.inc();
            self.metrics.bytes_retrieved_total.add(data.len() as i64);
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
    use tape_metrics::MetricsRegistry;

    fn create_test_metrics() -> Arc<NodeMetrics> {
        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        Arc::new(NodeMetrics::new(registry.prometheus_registry()))
    }

    fn create_test_store() -> StorageService<MemoryStore> {
        let metrics = create_test_metrics();
        let store = TapeStore::new(MemoryStore::new());
        StorageService::with_store(
            store,
            PathBuf::from("/tmp/test"),
            1_000_000,
            metrics,
        )
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
        let service = create_test_store();
        let track = Pubkey::new_unique();
        let spool_idx = 42u16;
        let data = vec![0xAB; 1024];
        let meta = create_test_meta();

        // Put slice
        service
            .put_slice(spool_idx, track, data.clone(), meta.clone())
            .unwrap();

        // Get slice
        let (retrieved_data, retrieved_meta) = service
            .get_slice(spool_idx, track)
            .unwrap()
            .expect("slice should exist");

        assert_eq!(retrieved_data, data);
        assert_eq!(retrieved_meta.len, meta.len);
    }

    #[test]
    fn test_get_nonexistent_slice() {
        let service = create_test_store();
        let track = Pubkey::new_unique();

        let result = service.get_slice(0, track).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_slice() {
        let service = create_test_store();
        let track = Pubkey::new_unique();
        let spool_idx = 42u16;

        // Put slice
        service
            .put_slice(spool_idx, track, vec![0xAB; 100], create_test_meta())
            .unwrap();

        // Verify it exists
        assert!(service.get_slice(spool_idx, track).unwrap().is_some());

        // Delete slice
        service.delete_slice(spool_idx, track).unwrap();

        // Verify it's gone
        assert!(service.get_slice(spool_idx, track).unwrap().is_none());
    }

    #[test]
    fn test_is_healthy() {
        let service = create_test_store();
        // For MemoryStore with fake path, is_healthy checks path existence
        // In real usage with RocksDB, the path would exist
        assert!(!service.is_healthy()); // /tmp/test doesn't exist in this test
    }
}
