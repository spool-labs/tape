//! Distributed uploader for parallel slice uploads.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;

use crate::communication::NodeCommunicationFactory;
use crate::error::UploadError;

// Re-export erasure coding constants from tape-core
pub use tape_core::erasure::{DATA_SLICES, PARITY_SLICES, TOTAL_SLICES};

/// Default concurrency limit for uploads.
const DEFAULT_CONCURRENCY: usize = 32;

/// Distributed uploader for parallel slice uploads to storage nodes.
pub struct DistributedUploader {
    track_id: String,
    slices: Vec<Vec<u8>>,
    node_addresses: Vec<String>,
    factory: NodeCommunicationFactory,
    concurrency_limit: Arc<Semaphore>,
}

impl DistributedUploader {
    /// Create a new uploader.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slices` - The encoded slices (should be 1024)
    /// * `node_addresses` - List of node addresses for the committee
    /// * `factory` - Factory for creating node clients
    pub fn new(
        track_id: String,
        slices: Vec<Vec<u8>>,
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
        let mut slices_per_node: HashMap<usize, Vec<(u16, Vec<u8>)>> = HashMap::new();

        for (slice_idx, slice_data) in self.slices.iter().enumerate() {
            let node_idx = slice_idx % num_nodes;
            slices_per_node
                .entry(node_idx)
                .or_default()
                .push((slice_idx as u16, slice_data.clone()));
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

                    for (slice_idx, data) in slices {
                        client.put_slice(&track_id, slice_idx, data).await?;
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

    #[test]
    fn test_constants() {
        assert_eq!(TOTAL_SLICES, 1024);
        assert_eq!(DATA_SLICES, 683);
        assert_eq!(PARITY_SLICES, 341);
        assert_eq!(DATA_SLICES + PARITY_SLICES, TOTAL_SLICES);
    }

    #[test]
    fn test_uploader_creation() {
        let factory = NodeCommunicationFactory::new();
        let slices: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; 100]).collect();
        let nodes = vec!["localhost:8080".to_string(), "localhost:8081".to_string()];

        let uploader =
            DistributedUploader::new("track_123".to_string(), slices.clone(), nodes, factory);

        assert_eq!(uploader.slice_count(), 10);
    }
}
