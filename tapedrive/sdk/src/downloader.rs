//! Parallel downloader for slice retrieval.

use futures::stream::{FuturesUnordered, StreamExt};

use crate::communication::NodeCommunicationFactory;
use crate::error::DownloadError;
use crate::uploader::{DATA_SLICES, TOTAL_SLICES};

/// Parallel downloader for retrieving slices from storage nodes.
pub struct ParallelDownloader {
    track_id: String,
    node_addresses: Vec<String>,
    factory: NodeCommunicationFactory,
}

impl ParallelDownloader {
    /// Create a new downloader.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `node_addresses` - List of node addresses for the committee
    /// * `factory` - Factory for creating node clients
    pub fn new(
        track_id: String,
        node_addresses: Vec<String>,
        factory: NodeCommunicationFactory,
    ) -> Self {
        Self {
            track_id,
            node_addresses,
            factory,
        }
    }

    /// Download at least DATA_SLICES (683) valid slices.
    ///
    /// Requests slices in parallel and returns as soon as enough are collected.
    pub async fn download_enough_slices(&self) -> Result<Vec<(u16, Vec<u8>)>, DownloadError> {
        if self.node_addresses.is_empty() {
            return Err(DownloadError::NoNodesAvailable);
        }

        let num_nodes = self.node_addresses.len();
        let mut collected_slices = Vec::with_capacity(DATA_SLICES);
        let mut futures = FuturesUnordered::new();

        // Request all slices in parallel
        for slice_idx in 0..TOTAL_SLICES as u16 {
            let node_idx = slice_idx as usize % num_nodes;
            let address = self.node_addresses[node_idx].clone();
            let factory = self.factory.clone();
            let track_id = self.track_id.clone();

            futures.push(async move {
                let client = factory.client_for_address(&address)?;
                let result = client.get_slice(&track_id, slice_idx).await;
                Ok::<_, DownloadError>((slice_idx, result))
            });
        }

        // Collect until we have enough
        while let Some(result) = futures.next().await {
            match result {
                Ok((slice_idx, Ok(data))) => {
                    collected_slices.push((slice_idx, data));

                    if collected_slices.len() >= DATA_SLICES {
                        break;
                    }
                }
                Ok((_, Err(_))) => {
                    // Slice fetch failed, continue with others
                }
                Err(_) => {
                    // Client creation failed, continue with others
                }
            }
        }

        if collected_slices.len() < DATA_SLICES {
            return Err(DownloadError::InsufficientSlices {
                got: collected_slices.len(),
                need: DATA_SLICES,
            });
        }

        Ok(collected_slices)
    }

    /// Download a specific slice.
    pub async fn download_slice(&self, slice_idx: u16) -> Result<Vec<u8>, DownloadError> {
        if self.node_addresses.is_empty() {
            return Err(DownloadError::NoNodesAvailable);
        }

        let num_nodes = self.node_addresses.len();
        let node_idx = slice_idx as usize % num_nodes;
        let address = &self.node_addresses[node_idx];

        let client = self.factory.client_for_address(address)?;
        let data = client.get_slice(&self.track_id, slice_idx).await?;

        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downloader_creation() {
        let factory = NodeCommunicationFactory::new();
        let nodes = vec!["localhost:8080".to_string(), "localhost:8081".to_string()];

        let downloader = ParallelDownloader::new("track_123".to_string(), nodes, factory);

        // Just verify it creates without panic
        assert_eq!(downloader.track_id, "track_123");
    }
}
