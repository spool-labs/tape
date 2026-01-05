//! High-level tape client for blob operations.

use tape_slicer::BlobMerkleRoot;

use crate::communication::NodeCommunicationFactory;
use crate::decoder::BlobDecoder;
use crate::downloader::ParallelDownloader;
use crate::encoder::BlobEncoder;
use crate::error::{ClientError, DownloadError, UploadError};
use crate::uploader::{DistributedUploader, SliceWithProof};

/// Default max slice size (1 MiB) - matches production settings.
pub const DEFAULT_MAX_SLICE_BYTES: usize = 1 << 20;

/// High-level client for tapedrive blob operations.
///
/// Provides simple upload/download methods that handle:
/// - Erasure coding (slicing)
/// - Distributed upload to storage nodes
/// - Parallel download with recovery
/// - Merkle verification
///
/// # Example
///
/// ```rust,ignore
/// // Production client (1 MiB slices, ~1 GB max blob)
/// let client = TapeClient::new(node_addresses);
///
/// // Test client (4 KB slices, ~2.7 MB max blob, low memory)
/// let client = TapeClient::builder()
///     .node_addresses(node_addresses)
///     .max_slice_bytes(4 * 1024)
///     .build();
/// ```
pub struct TapeClient {
    /// Factory for creating node clients.
    node_factory: NodeCommunicationFactory,

    /// List of known storage node addresses.
    node_addresses: Vec<String>,

    /// Maximum slice size in bytes for encoding.
    /// Smaller values use less memory but limit max blob size.
    max_slice_bytes: usize,
}

impl TapeClient {
    /// Create a new tape client with default settings.
    ///
    /// Uses 1 MiB slice size (production default).
    ///
    /// # Arguments
    /// * `node_addresses` - List of storage node addresses
    pub fn new(node_addresses: Vec<String>) -> Self {
        Self {
            node_factory: NodeCommunicationFactory::new(),
            node_addresses,
            max_slice_bytes: DEFAULT_MAX_SLICE_BYTES,
        }
    }

    /// Create a builder for more configuration options.
    pub fn builder() -> TapeClientBuilder {
        TapeClientBuilder::default()
    }

    /// Create a new tape client with a custom factory.
    pub fn with_factory(node_addresses: Vec<String>, factory: NodeCommunicationFactory) -> Self {
        Self {
            node_factory: factory,
            node_addresses,
            max_slice_bytes: DEFAULT_MAX_SLICE_BYTES,
        }
    }

    /// Upload slices with proofs to the network.
    ///
    /// This is a lower-level method that uploads pre-encoded slices.
    /// For full blob upload with encoding, use `upload_blob()` instead.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slices` - Pre-encoded slices with merkle proofs (should be 1024)
    pub async fn upload_slices(
        &self,
        track_id: &str,
        slices: Vec<SliceWithProof>,
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

    /// Check if a specific node is healthy.
    ///
    /// # Arguments
    /// * `node_address` - The node address to check
    pub async fn health_check(&self, node_address: &str) -> Result<bool, ClientError> {
        let client = self
            .node_factory
            .client_for_address(node_address)
            .map_err(|e| ClientError::Encoding(e.to_string()))?;
        client
            .health_check()
            .await
            .map_err(|e| ClientError::Encoding(e.to_string()))
    }

    /// Update the list of node addresses.
    ///
    /// Call this when the committee changes.
    pub fn set_node_addresses(&mut self, addresses: Vec<String>) {
        self.node_addresses = addresses;
    }

    // =========================================================================
    // High-level blob operations (encode + upload, download + decode)
    // =========================================================================

    /// Get the configured max slice size.
    pub fn max_slice_bytes(&self) -> usize {
        self.max_slice_bytes
    }

    /// Upload a blob to the network.
    ///
    /// This is the primary method for storing data. It:
    /// 1. Encodes the blob into SLICE_COUNT slices using Reed-Solomon
    /// 2. Computes the Merkle root commitment and proofs for each slice
    /// 3. Uploads all slices with their proofs to storage nodes
    ///
    /// # Arguments
    /// * `track_id` - The track identifier for this blob
    /// * `data` - Raw blob data to upload
    ///
    /// # Returns
    /// The Merkle root (commitment hash) for the uploaded blob.
    /// This should be stored on-chain for verification during download.
    ///
    /// # Note
    /// This method does NOT register the track on-chain. The caller must:
    /// 1. Call this method to upload data
    /// 2. Use the returned commitment to register the track on Solana
    /// 3. Collect BLS certifications from nodes
    pub async fn upload_blob(
        &self,
        track_id: &str,
        data: Vec<u8>,
    ) -> Result<BlobMerkleRoot, ClientError> {
        // Encode blob into slices with merkle proofs
        let mut encoder = BlobEncoder::with_max_slice_bytes(self.max_slice_bytes);
        let (slices_with_proofs, commitment) = encoder
            .encode_with_proofs(data)
            .map_err(ClientError::Upload)?;

        // Upload all slices with their proofs
        let uploader = DistributedUploader::new(
            track_id.to_string(),
            slices_with_proofs,
            self.node_addresses.clone(),
            self.node_factory.clone(),
        );

        uploader.upload_all().await.map_err(ClientError::Upload)?;

        Ok(commitment)
    }

    /// Probe the slice size for a track by downloading a single slice.
    ///
    /// This is useful for determining the correct decoder buffer size
    /// before downloading all slices.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    ///
    /// # Returns
    /// The size in bytes of slices for this track.
    pub async fn probe_slice_size(&self, track_id: &str) -> Result<usize, DownloadError> {
        let downloader = ParallelDownloader::new(
            track_id.to_string(),
            self.node_addresses.clone(),
            self.node_factory.clone(),
        );

        // Download slice 0 to determine size
        let slice_data = downloader.download_slice(0).await?;
        Ok(slice_data.len())
    }

    /// Download and decode a blob from the network.
    ///
    /// This is the primary method for retrieving data. It:
    /// 1. Probes a single slice to determine the slice size
    /// 2. Downloads at least DATA_SLICES slices from storage nodes
    /// 3. Decodes the slices back into the original blob using the detected size
    ///
    /// # Arguments
    /// * `track_id` - The track identifier for the blob
    ///
    /// # Returns
    /// The original blob data.
    ///
    /// # Note
    /// This method does NOT verify the data against on-chain commitment.
    /// For full verification, the caller should:
    /// 1. Fetch the track's commitment_hash from Solana
    /// 2. Re-encode the downloaded data and compare merkle roots
    /// Or use `download_blob_verified()` which does this automatically.
    pub async fn download_blob(&self, track_id: &str) -> Result<Vec<u8>, ClientError> {
        // Probe slice size first to allocate correct decoder buffers
        let slice_size = self
            .probe_slice_size(track_id)
            .await
            .map_err(ClientError::Download)?;

        // Download enough slices
        let slices = self
            .download_slices(track_id)
            .await
            .map_err(ClientError::Download)?;

        // Decode slices using detected slice size
        let mut decoder = BlobDecoder::with_max_slice_bytes(slice_size);
        let data = decoder
            .decode(slices)
            .map_err(ClientError::Download)?;

        Ok(data)
    }

    /// Download a blob and verify against the expected commitment.
    ///
    /// Same as `download_blob()` but also verifies the reconstructed data
    /// matches the expected Merkle root commitment.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier for the blob
    /// * `expected_commitment` - The Merkle root from on-chain track data
    ///
    /// # Returns
    /// The verified original blob data.
    ///
    /// # Errors
    /// Returns `ClientError::CommitmentMismatch` if verification fails.
    pub async fn download_blob_verified(
        &self,
        track_id: &str,
        expected_commitment: &BlobMerkleRoot,
    ) -> Result<Vec<u8>, ClientError> {
        // Download and decode
        let data = self.download_blob(track_id).await?;

        // Re-encode to verify commitment
        let mut encoder = BlobEncoder::with_max_slice_bytes(self.max_slice_bytes);
        let (_, actual_commitment) = encoder
            .encode_to_vec_with_root(data.clone())
            .map_err(|e| ClientError::Encoding(e.to_string()))?;

        if &actual_commitment != expected_commitment {
            return Err(ClientError::CommitmentMismatch);
        }

        Ok(data)
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Builder for creating a `TapeClient` with custom configuration.
///
/// # Example
///
/// ```rust,ignore
/// let client = TapeClient::builder()
///     .node_addresses(vec!["node1:8080".into(), "node2:8080".into()])
///     .max_slice_bytes(4 * 1024)  // 4 KB slices for testing
///     .build();
/// ```
#[derive(Default)]
pub struct TapeClientBuilder {
    node_addresses: Vec<String>,
    node_factory: Option<NodeCommunicationFactory>,
    max_slice_bytes: Option<usize>,
}

impl TapeClientBuilder {
    /// Set the storage node addresses.
    pub fn node_addresses(mut self, addresses: Vec<String>) -> Self {
        self.node_addresses = addresses;
        self
    }

    /// Add a single node address.
    pub fn add_node(mut self, address: impl Into<String>) -> Self {
        self.node_addresses.push(address.into());
        self
    }

    /// Set a custom node communication factory.
    pub fn node_factory(mut self, factory: NodeCommunicationFactory) -> Self {
        self.node_factory = Some(factory);
        self
    }

    /// Set the maximum slice size in bytes.
    ///
    /// - Production default: 1 MiB (1 << 20) - supports blobs up to ~683 MiB
    /// - Testing: 4 KiB (4 * 1024) - supports blobs up to ~2.7 MiB, uses ~6 MB RAM
    ///
    /// Smaller values use less memory but limit the maximum blob size.
    pub fn max_slice_bytes(mut self, size: usize) -> Self {
        self.max_slice_bytes = Some(size);
        self
    }

    /// Build the `TapeClient`.
    pub fn build(self) -> TapeClient {
        TapeClient {
            node_addresses: self.node_addresses,
            node_factory: self.node_factory.unwrap_or_default(),
            max_slice_bytes: self.max_slice_bytes.unwrap_or(DEFAULT_MAX_SLICE_BYTES),
        }
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

    #[test]
    fn test_builder_default() {
        let client = TapeClient::builder()
            .node_addresses(vec!["node1:8080".into()])
            .build();

        assert_eq!(client.node_addresses(), &["node1:8080"]);
        assert_eq!(client.max_slice_bytes(), DEFAULT_MAX_SLICE_BYTES);
    }

    #[test]
    fn test_builder_custom_slice_size() {
        let client = TapeClient::builder()
            .node_addresses(vec!["node1:8080".into()])
            .max_slice_bytes(4 * 1024)
            .build();

        assert_eq!(client.max_slice_bytes(), 4 * 1024);
    }

    #[test]
    fn test_builder_add_node() {
        let client = TapeClient::builder()
            .add_node("node1:8080")
            .add_node("node2:8080")
            .build();

        assert_eq!(client.node_addresses().len(), 2);
    }
}
