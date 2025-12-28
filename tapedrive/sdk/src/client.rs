//! High-level tape client for blob operations.

use crate::communication::NodeCommunicationFactory;
use crate::downloader::ParallelDownloader;
use crate::error::{DownloadError, UploadError};
use crate::uploader::DistributedUploader;

/// High-level client for tapedrive blob operations.
///
/// Provides simple upload/download methods that handle:
/// - Erasure coding (slicing)
/// - Distributed upload to storage nodes
/// - Parallel download with recovery
/// - Merkle verification
pub struct TapeClient {
    /// Factory for creating node clients.
    node_factory: NodeCommunicationFactory,

    /// List of known storage node addresses.
    /// In production, this would be fetched from Solana committee state.
    node_addresses: Vec<String>,
}

impl TapeClient {
    /// Create a new tape client.
    ///
    /// # Arguments
    /// * `node_addresses` - List of storage node addresses
    pub fn new(node_addresses: Vec<String>) -> Self {
        Self {
            node_factory: NodeCommunicationFactory::new(),
            node_addresses,
        }
    }

    /// Create a new tape client with a custom factory.
    pub fn with_factory(node_addresses: Vec<String>, factory: NodeCommunicationFactory) -> Self {
        Self {
            node_factory: factory,
            node_addresses,
        }
    }

    /// Upload raw slices to the network.
    ///
    /// This is a lower-level method that uploads pre-encoded slices.
    /// For full blob upload with encoding, use a higher-level method
    /// that integrates with the slicer.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slices` - Pre-encoded slices (should be 1024)
    pub async fn upload_slices(
        &self,
        track_id: &str,
        slices: Vec<Vec<u8>>,
    ) -> Result<(), UploadError> {
        let uploader = DistributedUploader::new(
            track_id.to_string(),
            slices,
            self.node_addresses.clone(),
            self.node_factory.clone(),
        );

        uploader.upload_all().await
    }

    /// Download slices from the network.
    ///
    /// This is a lower-level method that downloads raw slices.
    /// For full blob download with decoding, use a higher-level method
    /// that integrates with the slicer.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    pub async fn download_slices(
        &self,
        track_id: &str,
    ) -> Result<Vec<(u16, Vec<u8>)>, DownloadError> {
        let downloader = ParallelDownloader::new(
            track_id.to_string(),
            self.node_addresses.clone(),
            self.node_factory.clone(),
        );

        downloader.download_enough_slices().await
    }

    /// Get the list of node addresses.
    pub fn node_addresses(&self) -> &[String] {
        &self.node_addresses
    }

    /// Update the list of node addresses.
    ///
    /// Call this when the committee changes.
    pub fn set_node_addresses(&mut self, addresses: Vec<String>) {
        self.node_addresses = addresses;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let nodes = vec!["localhost:8080".to_string(), "localhost:8081".to_string()];
        let client = TapeClient::new(nodes.clone());

        assert_eq!(client.node_addresses(), nodes.as_slice());
    }

    #[test]
    fn test_client_with_factory() {
        let nodes = vec!["localhost:8080".to_string()];
        let factory =
            NodeCommunicationFactory::new().with_connect_timeout(std::time::Duration::from_secs(10));

        let client = TapeClient::with_factory(nodes.clone(), factory);

        assert_eq!(client.node_addresses(), nodes.as_slice());
    }

    #[test]
    fn test_update_addresses() {
        let mut client = TapeClient::new(vec!["localhost:8080".to_string()]);

        let new_nodes = vec!["node1:8080".to_string(), "node2:8080".to_string()];
        client.set_node_addresses(new_nodes.clone());

        assert_eq!(client.node_addresses(), new_nodes.as_slice());
    }
}
