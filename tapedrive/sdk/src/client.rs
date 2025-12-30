//! High-level tape client for blob operations.

use tape_slicer::BlobMerkleRoot;

use crate::communication::NodeCommunicationFactory;
use crate::decoder::BlobDecoder;
use crate::downloader::ParallelDownloader;
use crate::encoder::BlobEncoder;
use crate::error::{ClientError, DownloadError, UploadError};
use crate::uploader::{DistributedUploader, SliceWithProof};

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

    /// Update the list of node addresses.
    ///
    /// Call this when the committee changes.
    pub fn set_node_addresses(&mut self, addresses: Vec<String>) {
        self.node_addresses = addresses;
    }

    // =========================================================================
    // High-level blob operations (encode + upload, download + decode)
    // =========================================================================

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
        let mut encoder = BlobEncoder::new();
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

    /// Download and decode a blob from the network.
    ///
    /// This is the primary method for retrieving data. It:
    /// 1. Downloads at least DATA_SLICES slices from storage nodes
    /// 2. Decodes the slices back into the original blob
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
        // Download enough slices
        let slices = self
            .download_slices(track_id)
            .await
            .map_err(ClientError::Download)?;

        // Decode slices back to blob
        let mut decoder = BlobDecoder::new();
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
        let mut encoder = BlobEncoder::new();
        let (_, actual_commitment) = encoder
            .encode_to_vec_with_root(data.clone())
            .map_err(|e| ClientError::Encoding(e.to_string()))?;

        if &actual_commitment != expected_commitment {
            return Err(ClientError::CommitmentMismatch);
        }

        Ok(data)
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
