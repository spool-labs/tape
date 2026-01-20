//! High-level blob client for upload/download operations.
//!
//! Uses proper spool-based routing from on-chain committee state to send
//! each slice to the correct storage node.

pub use tape_api::program::MEMBER_COUNT;
use tape_core::erasure::SLICE_COUNT;
use tape_core::spooler::{SpoolAssignment, SpoolIndex};
use tape_core::system::Committee;
use tape_core::types::NetworkAddress;
use tape_slicer::BlobMerkleRoot;

use crate::communication::NodeCommunicationFactory;
use crate::decoder::BlobDecoder;
use crate::downloader::ParallelDownloader;
use crate::encoder::BlobEncoder;
use crate::error::{ClientError, DownloadError, UploadError};
use crate::routing::SliceRouter;
use crate::uploader::{DistributedUploader, SliceWithProof};

/// High-level client for tapedrive blob operations.
///
/// Provides simple upload/download methods that handle:
/// - Erasure coding (slicing) using RotatedSlicer for fair load distribution
/// - Distributed upload to storage nodes using proper spool-based routing
/// - Parallel download with recovery
/// - Merkle verification
///
/// # Example
///
/// ```rust,ignore
/// // Build client from on-chain system state
/// let system = rpc_client.get_system().await?;
/// let node_addresses = fetch_node_addresses(&system.committee).await?;
/// let client = TapeClient::from_system(
///     system.committee.clone(),
///     system.spools.clone(),
///     node_addresses,
/// );
///
/// // Or use builder for more control
/// let client = TapeClient::builder()
///     .committee(system.committee)
///     .spool_assignment(system.spools)
///     .node_addresses(node_addresses)
///     .build();
/// ```
pub struct TapeClient {
    /// Factory for creating node clients.
    node_factory: NodeCommunicationFactory,

    /// Router for slice → node mapping based on spool assignments.
    router: SliceRouter<MEMBER_COUNT>,
}

impl TapeClient {
    /// Create a new tape client from on-chain system state.
    ///
    /// # Arguments
    /// * `committee` - The active committee from System account
    /// * `spool_assignment` - Spool assignments from System account
    /// * `node_addresses` - List of (member_index, NetworkAddress) pairs
    pub fn from_system(
        committee: Committee<MEMBER_COUNT>,
        spool_assignment: SpoolAssignment<SLICE_COUNT>,
        node_addresses: impl IntoIterator<Item = (usize, NetworkAddress)>,
    ) -> Self {
        let mut router = SliceRouter::new(spool_assignment, committee);
        router.set_addresses(node_addresses);

        Self {
            node_factory: NodeCommunicationFactory::new(),
            router,
        }
    }

    /// Create a builder for more configuration options.
    pub fn builder() -> TapeClientBuilder {
        TapeClientBuilder::default()
    }

    /// Upload slices with proofs to the network.
    ///
    /// This is a lower-level method that uploads pre-encoded slices.
    /// For full blob upload with encoding, use `upload_blob()` instead.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slices` - Pre-encoded slices with merkle proofs (should be SLICE_COUNT)
    pub async fn upload_slices(
        &self,
        track_id: &str,
        slices: Vec<SliceWithProof>,
    ) -> Result<(), UploadError> {
        let uploader = DistributedUploader::new(
            track_id.to_string(),
            slices,
            self.router.clone(),
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
    ) -> Result<Vec<(SpoolIndex, Vec<u8>)>, DownloadError> {
        use std::collections::HashMap;

        // Build slice_index → address mapping using proper spool-based routing
        let mut slice_to_address: HashMap<SpoolIndex, String> = HashMap::new();

        for slice_idx in 0..SLICE_COUNT as SpoolIndex {
            if let Ok(sock) = self.router.socket_addr_for_slice(slice_idx) {
                slice_to_address.insert(slice_idx, format!("http://{}", sock));
            }
        }

        let downloader = ParallelDownloader::new(
            track_id.to_string(),
            slice_to_address,
            self.node_factory.clone(),
        );

        downloader.download_enough_slices().await
    }

    /// Get the committee size.
    pub fn committee_size(&self) -> usize {
        self.router.committee_size()
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

    /// Update the router with new committee and spool assignments.
    ///
    /// Call this when the committee changes (epoch transition).
    pub fn update_router(
        &mut self,
        committee: Committee<MEMBER_COUNT>,
        spool_assignment: SpoolAssignment<SLICE_COUNT>,
        node_addresses: impl IntoIterator<Item = (usize, NetworkAddress)>,
    ) {
        let mut router = SliceRouter::new(spool_assignment, committee);
        router.set_addresses(node_addresses);
        self.router = router;
    }

    /// Get a reference to the internal router.
    pub fn router(&self) -> &SliceRouter<MEMBER_COUNT> {
        &self.router
    }

    // =========================================================================
    // High-level blob operations (encode + upload, download + decode)
    // =========================================================================

    /// Upload a blob to the network.
    ///
    /// This is the primary method for storing data. It:
    /// 1. Encodes the blob into SLICE_COUNT slices using Reed-Solomon
    /// 2. Computes the Merkle root commitment and proofs for each slice
    /// 3. Uploads all slices with their proofs to the correct storage nodes
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
        // Encode blob into slices with merkle proofs using RotatedSlicer
        let mut encoder = BlobEncoder::new();
        let (slices_with_proofs, commitment) = encoder
            .encode_with_proofs(data)
            .map_err(ClientError::Upload)?;

        // Upload all slices with their proofs using spool-based routing
        let uploader = DistributedUploader::new(
            track_id.to_string(),
            slices_with_proofs,
            self.router.clone(),
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
    ///
    /// # Fault Tolerance
    /// Tries random slices from different nodes until one responds.
    /// Randomized order ensures load is spread across nodes.
    pub async fn probe_slice_size(&self, track_id: &str) -> Result<usize, DownloadError> {
        use rand::seq::SliceRandom;
        use std::collections::HashMap;

        // Build slice_index → address mapping using proper spool-based routing
        let mut slice_to_address: HashMap<SpoolIndex, String> = HashMap::new();

        for slice_idx in 0..SLICE_COUNT as SpoolIndex {
            if let Ok(sock) = self.router.socket_addr_for_slice(slice_idx) {
                slice_to_address.insert(slice_idx, format!("http://{}", sock));
            }
        }

        let downloader = ParallelDownloader::new(
            track_id.to_string(),
            slice_to_address,
            self.node_factory.clone(),
        );

        // Generate random slice indices to spread load across nodes
        let mut indices: Vec<SpoolIndex> = (0..SLICE_COUNT as SpoolIndex).collect();
        indices.shuffle(&mut rand::thread_rng());

        // Try slices in random order until one responds
        // With 1024 slices across N nodes, this will try every node
        for &slice_idx in &indices {
            if let Ok(slice_data) = downloader.download_slice(slice_idx).await {
                return Ok(slice_data.len());
            }
        }

        Err(DownloadError::NoNodesAvailable)
    }

    /// Download and decode a blob from the network.
    ///
    /// This is the primary method for retrieving data. It:
    /// 1. Downloads at least DATA_SLICES slices from storage nodes (fault-tolerant)
    /// 2. Infers slice size from the collected slices
    /// 3. Decodes the slices back into the original blob
    ///
    /// # Arguments
    /// * `track_id` - The track identifier for the blob
    ///
    /// # Returns
    /// The original blob data.
    ///
    /// # Fault Tolerance
    /// This method is resilient to node failures. It will continue fetching
    /// from available nodes until it has enough slices (2f+1 of 3f+1) to
    /// reconstruct the original data.
    ///
    /// # Note
    /// This method does NOT verify the data against on-chain commitment.
    /// For full verification, the caller should:
    /// 1. Fetch the track's commitment_hash from Solana
    /// 2. Re-encode the downloaded data and compare merkle roots
    /// Or use `download_blob_verified()` which does this automatically.
    pub async fn download_blob(&self, track_id: &str) -> Result<Vec<u8>, ClientError> {
        // Download enough slices (fault-tolerant - continues on node failures)
        let slices = self
            .download_slices(track_id)
            .await
            .map_err(ClientError::Download)?;

        // Decode slices using RotatedSlicer (default)
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

        // Re-encode to verify commitment using RotatedSlicer
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

// ============================================================================
// Builder
// ============================================================================

/// Builder for creating a `TapeClient` with custom configuration.
///
/// # Example
///
/// ```rust,ignore
/// let client = TapeClient::builder()
///     .committee(system.committee)
///     .spool_assignment(system.spools)
///     .node_addresses(addresses)
///     .build();
/// ```
#[derive(Default)]
pub struct TapeClientBuilder {
    committee: Option<Committee<MEMBER_COUNT>>,
    spool_assignment: Option<SpoolAssignment<SLICE_COUNT>>,
    node_addresses: Vec<(usize, NetworkAddress)>,
    node_factory: Option<NodeCommunicationFactory>,
}

impl TapeClientBuilder {
    /// Set the committee from on-chain System state.
    pub fn committee(mut self, committee: Committee<MEMBER_COUNT>) -> Self {
        self.committee = Some(committee);
        self
    }

    /// Set the spool assignment from on-chain System state.
    pub fn spool_assignment(mut self, assignment: SpoolAssignment<SLICE_COUNT>) -> Self {
        self.spool_assignment = Some(assignment);
        self
    }

    /// Set the node addresses (member_index, NetworkAddress pairs).
    pub fn node_addresses(
        mut self,
        addresses: impl IntoIterator<Item = (usize, NetworkAddress)>,
    ) -> Self {
        self.node_addresses = addresses.into_iter().collect();
        self
    }

    /// Add a single node address.
    pub fn add_node(mut self, member_index: usize, address: NetworkAddress) -> Self {
        self.node_addresses.push((member_index, address));
        self
    }

    /// Set a custom node communication factory.
    pub fn node_factory(mut self, factory: NodeCommunicationFactory) -> Self {
        self.node_factory = Some(factory);
        self
    }

    /// Build the `TapeClient`.
    ///
    /// # Panics
    /// Panics if committee or spool_assignment are not set.
    pub fn build(self) -> TapeClient {
        let committee = self.committee.expect("committee is required");
        let spool_assignment = self.spool_assignment.expect("spool_assignment is required");

        let mut router = SliceRouter::new(spool_assignment, committee);
        router.set_addresses(self.node_addresses);

        TapeClient {
            router,
            node_factory: self.node_factory.unwrap_or_default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::system::CommitteeMember;
    use tape_core::types::{Coin, NodeId, TAPE};

    fn make_test_committee(count: usize) -> Committee<MEMBER_COUNT> {
        let mut committee = Committee::new();
        for i in 0..count.min(MEMBER_COUNT) {
            let member = CommitteeMember::new(
                NodeId::new(i as u64 + 1),
                Coin::<TAPE>::new(1000 - i as u64),
            );
            let _ = committee.try_join(&member);
        }
        committee
    }

    fn make_uniform_assignment(member_count: usize) -> SpoolAssignment<SLICE_COUNT> {
        let mut spools = [0u8; SLICE_COUNT];
        for i in 0..SLICE_COUNT {
            spools[i] = (i % member_count) as u8;
        }
        SpoolAssignment::new(spools)
    }

    #[test]
    fn test_client_from_system() {
        let committee = make_test_committee(2);
        let assignment = make_uniform_assignment(2);
        let addresses = vec![
            (0, NetworkAddress::from("127.0.0.1:8080").unwrap()),
            (1, NetworkAddress::from("127.0.0.1:8081").unwrap()),
        ];

        let client = TapeClient::from_system(committee, assignment, addresses);

        assert_eq!(client.committee_size(), 2);
    }

    #[test]
    fn test_builder_with_committee() {
        let committee = make_test_committee(2);
        let assignment = make_uniform_assignment(2);
        let addresses = vec![
            (0, NetworkAddress::from("127.0.0.1:8080").unwrap()),
            (1, NetworkAddress::from("127.0.0.1:8081").unwrap()),
        ];

        let client = TapeClient::builder()
            .committee(committee)
            .spool_assignment(assignment)
            .node_addresses(addresses)
            .build();

        assert_eq!(client.committee_size(), 2);
    }

    #[test]
    fn test_builder_add_node() {
        let committee = make_test_committee(2);
        let assignment = make_uniform_assignment(2);

        let client = TapeClient::builder()
            .committee(committee)
            .spool_assignment(assignment)
            .add_node(0, NetworkAddress::from("127.0.0.1:8080").unwrap())
            .add_node(1, NetworkAddress::from("127.0.0.1:8081").unwrap())
            .build();

        assert_eq!(client.committee_size(), 2);
    }

    #[test]
    fn test_update_router() {
        let committee = make_test_committee(2);
        let assignment = make_uniform_assignment(2);
        let addresses = vec![
            (0, NetworkAddress::from("127.0.0.1:8080").unwrap()),
            (1, NetworkAddress::from("127.0.0.1:8081").unwrap()),
        ];

        let mut client = TapeClient::from_system(committee, assignment, addresses);
        assert_eq!(client.committee_size(), 2);

        // Update to a new committee with 3 members
        let new_committee = make_test_committee(3);
        let new_assignment = make_uniform_assignment(3);
        let new_addresses = vec![
            (0, NetworkAddress::from("127.0.0.1:9080").unwrap()),
            (1, NetworkAddress::from("127.0.0.1:9081").unwrap()),
            (2, NetworkAddress::from("127.0.0.1:9082").unwrap()),
        ];

        client.update_router(new_committee, new_assignment, new_addresses);
        assert_eq!(client.committee_size(), 3);
    }
}
