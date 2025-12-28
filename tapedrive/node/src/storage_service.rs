//! Storage service for managing slice storage and retrieval.

use std::path::{Path, PathBuf};
use std::sync::Arc;

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
}

/// Storage service for managing slice data.
///
/// This is a placeholder implementation that will be replaced
/// with actual RocksDB storage in a future phase.
pub struct StorageService {
    /// Path to the storage directory.
    storage_path: PathBuf,
    /// Storage capacity in bytes.
    capacity: u64,
    /// Reference to node metrics.
    metrics: Arc<NodeMetrics>,
}

impl StorageService {
    /// Create a new storage service.
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

        Ok(Self {
            storage_path,
            capacity: config.storage_capacity,
            metrics,
        })
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

    /// Initialize storage (placeholder for RocksDB initialization).
    pub async fn initialize(&self) -> Result<(), StorageError> {
        // In future phases, this will initialize RocksDB
        tracing::info!(
            path = %self.storage_path.display(),
            capacity = self.capacity,
            "Storage service initialized"
        );

        // Update metrics
        self.metrics.storage_bytes_used.set(0);

        Ok(())
    }

    /// Shutdown storage gracefully.
    pub async fn shutdown(&self) -> Result<(), StorageError> {
        tracing::info!("Storage service shutting down");
        // In future phases, this will flush and close RocksDB
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_metrics::MetricsRegistry;

    fn create_test_config(storage_path: PathBuf) -> NodeConfig {
        use solana_program::pubkey::Pubkey;
        use std::net::SocketAddr;

        NodeConfig {
            name: "test-node".to_string(),
            protocol_keypair: PathBuf::from("/tmp/test"),
            network_keypair: PathBuf::from("/tmp/test"),
            bls_keypair: PathBuf::from("/tmp/test"),
            bind_address: "127.0.0.1:8080".parse::<SocketAddr>().unwrap(),
            public_host: "localhost".to_string(),
            public_port: 8080,
            tls: crate::config::TlsConfig::default(),
            storage_path,
            storage_capacity: 1_000_000,
            solana_rpc_url: "http://localhost:8899".to_string(),
            node_authority: Pubkey::new_from_array([0; 32]),
        }
    }

    #[test]
    fn test_storage_service_creation() {
        let temp_dir = std::env::temp_dir().join("tape_test_storage");
        let config = create_test_config(temp_dir.clone());

        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = Arc::new(NodeMetrics::new(registry.prometheus_registry()));

        let service = StorageService::new(&config, metrics).unwrap();

        assert_eq!(service.storage_path(), temp_dir);
        assert_eq!(service.capacity(), 1_000_000);
        assert!(service.is_healthy());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[tokio::test]
    async fn test_storage_initialize() {
        let temp_dir = std::env::temp_dir().join("tape_test_storage_init");
        let config = create_test_config(temp_dir.clone());

        let registry = match MetricsRegistry::get() {
            Some(r) => r,
            None => MetricsRegistry::init(),
        };
        let metrics = Arc::new(NodeMetrics::new(registry.prometheus_registry()));

        let service = StorageService::new(&config, metrics).unwrap();
        service.initialize().await.unwrap();

        assert!(service.is_healthy());

        // Cleanup
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
