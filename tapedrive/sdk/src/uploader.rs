//! Distributed uploader for parallel slice uploads.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tape_crypto::Hash;
use tape_node_api::SlicePayload;
use tokio::sync::Semaphore;

use crate::communication::NodeCommunicationFactory;
use crate::encoder::SliceMerkleProof;
use crate::error::UploadError;

// Re-export erasure coding constants from tape-core
pub use tape_core::erasure::{DATA_SLICES, PARITY_SLICES, SLICE_COUNT};

/// Default concurrency limit for uploads.
const DEFAULT_CONCURRENCY: usize = 32;

/// A slice with its merkle proof, ready for upload.
#[derive(Clone)]
pub struct SliceWithProof {
    pub index: u16,
    pub data: Vec<u8>,
    pub leaf_hash: Hash,
    pub merkle_proof: SliceMerkleProof,
}

impl SliceWithProof {
    /// Create a new slice with proof.
    pub fn new(index: u16, data: Vec<u8>, leaf_hash: Hash, merkle_proof: SliceMerkleProof) -> Self {
        Self { index, data, leaf_hash, merkle_proof }
    }

    /// Convert to SlicePayload for network transmission.
    pub fn to_payload(&self) -> SlicePayload {
        SlicePayload::new(self.data.clone(), self.leaf_hash, self.merkle_proof)
    }
}

/// Distributed uploader for parallel slice uploads to storage nodes.
pub struct DistributedUploader {
    track_id: String,
    slices: Vec<SliceWithProof>,
    node_addresses: Vec<String>,
    factory: NodeCommunicationFactory,
    concurrency_limit: Arc<Semaphore>,
}

impl DistributedUploader {
    /// Create a new uploader.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slices` - The encoded slices with merkle proofs (should be 1024)
    /// * `node_addresses` - List of node addresses for the committee
    /// * `factory` - Factory for creating node clients
    pub fn new(
        track_id: String,
        slices: Vec<SliceWithProof>,
        node_addresses: Vec<String>,
        factory: NodeCommunicationFactory,
    ) -> Self {
        Self {
            track_id,
            slices,
            node_addresses,
            factory,
            concurrency_limit: Arc::new(Semaphore::new(DEFAULT_CONCURRENCY)),
        }
    }

    /// Set the concurrency limit.
    pub fn with_concurrency(mut self, limit: usize) -> Self {
        self.concurrency_limit = Arc::new(Semaphore::new(limit));
        self
    }

    /// Upload all slices to the network.
    ///
    /// Returns when a quorum (2/3 + 1) of nodes have acknowledged.
    pub async fn upload_all(&self) -> Result<(), UploadError> {
        if self.node_addresses.is_empty() {
            return Err(UploadError::NoNodesAvailable);
        }

        let num_nodes = self.node_addresses.len();

        // Simple round-robin distribution of slices to nodes
        // In production, this would use spool assignments from the committee
        let mut slices_per_node: HashMap<usize, Vec<SliceWithProof>> = HashMap::new();

        for slice in &self.slices {
            let node_idx = slice.index as usize % num_nodes;
            slices_per_node
                .entry(node_idx)
                .or_default()
                .push(slice.clone());
        }

        // Upload to each node in parallel
        let upload_futures: Vec<_> = slices_per_node
            .into_iter()
            .map(|(node_idx, slices)| {
                let factory = self.factory.clone();
                let track_id = self.track_id.clone();
                let address = self.node_addresses[node_idx].clone();
                let permit = self.concurrency_limit.clone();

                async move {
                    let _permit = permit
                        .acquire()
                        .await
                        .map_err(|_| UploadError::Semaphore)?;

                    let client = factory.client_for_address(&address)?;

                    for slice in slices {
                        let payload = slice.to_payload();
                        client.put_slice(&track_id, slice.index, &payload).await?;
                    }

                    Ok::<_, UploadError>(node_idx)
                }
            })
            .collect();

        // Wait for all uploads
        let results: Vec<Result<usize, UploadError>> = stream::iter(upload_futures)
            .buffer_unordered(DEFAULT_CONCURRENCY)
            .collect()
            .await;

        // Check quorum
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let required = (num_nodes * 2 / 3) + 1;

        if successful < required {
            return Err(UploadError::InsufficientQuorum {
                got: successful,
                need: required,
            });
        }

        Ok(())
    }

    /// Get the number of slices.
    pub fn slice_count(&self) -> usize {
        self.slices.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_slicer::MERKLE_HEIGHT;

    fn make_test_slices(count: usize) -> Vec<SliceWithProof> {
        (0..count)
            .map(|i| SliceWithProof {
                index: i as u16,
                data: vec![i as u8; 100],
                leaf_hash: Hash::default(),
                merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            })
            .collect()
    }

    #[test]
    fn test_constants() {
        assert_eq!(SLICE_COUNT, 1024);
        assert_eq!(DATA_SLICES, 683);
        assert_eq!(PARITY_SLICES, 341);
        assert_eq!(DATA_SLICES + PARITY_SLICES, SLICE_COUNT);
    }

    #[test]
    fn test_uploader_creation() {
        let factory = NodeCommunicationFactory::new();
        let slices = make_test_slices(10);
        let nodes = vec!["localhost:8080".to_string(), "localhost:8081".to_string()];

        let uploader =
            DistributedUploader::new("track_123".to_string(), slices, nodes, factory);

        assert_eq!(uploader.slice_count(), 10);
    }

    #[test]
    fn test_slice_with_proof_to_payload() {
        let slice = SliceWithProof {
            index: 42,
            data: vec![0xAB; 500],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        };

        let payload = slice.to_payload();

        assert_eq!(payload.data, slice.data);
        assert_eq!(payload.leaf_hash, slice.leaf_hash);
        assert_eq!(payload.merkle_proof, slice.merkle_proof);
    }
}
