//! Parallel downloader for slice retrieval.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use tape_core::spooler::SpoolIndex;
use tokio::sync::Semaphore;

use crate::communication::NodeCommunicationFactory;
use crate::error::DownloadError;
use crate::uploader::{DATA_SLICES, SPOOL_COUNT};

/// Default concurrency limit for parallel downloads.
/// This limits how many HTTP requests are in flight at once.
const DEFAULT_CONCURRENCY: usize = 64;

/// Parallel downloader for retrieving slices from storage nodes.
pub struct ParallelDownloader {
    track_id: String,
    /// Maps slice_index → node address (for proper spool-based routing)
    slice_to_address: HashMap<SpoolIndex, String>,
    factory: NodeCommunicationFactory,
    concurrency: usize,
    /// Slice indices to exclude from downloads (e.g., for recovery).
    exclude_slices: HashSet<SpoolIndex>,
}

impl ParallelDownloader {
    /// Create a new downloader with spool-based routing.
    ///
    /// # Arguments
    /// * `track_id` - The track identifier
    /// * `slice_to_address` - Map of slice_index → node address (from spool assignment)
    /// * `factory` - Factory for creating node clients
    pub fn new(
        track_id: String,
        slice_to_address: HashMap<SpoolIndex, String>,
        factory: NodeCommunicationFactory,
    ) -> Self {
        Self {
            track_id,
            slice_to_address,
            factory,
            concurrency: DEFAULT_CONCURRENCY,
            exclude_slices: HashSet::new(),
        }
    }

    /// Create a new downloader with custom concurrency limit.
    pub fn with_concurrency(
        track_id: String,
        slice_to_address: HashMap<SpoolIndex, String>,
        factory: NodeCommunicationFactory,
        concurrency: usize,
    ) -> Self {
        Self {
            track_id,
            slice_to_address,
            factory,
            concurrency,
            exclude_slices: HashSet::new(),
        }
    }

    /// Set slices to exclude from downloads.
    ///
    /// This is useful for recovery scenarios where you need to reconstruct
    /// a specific slice and want to avoid requesting it from nodes that
    /// don't have it.
    pub fn with_excluded_slices(mut self, exclude: impl IntoIterator<Item = SpoolIndex>) -> Self {
        self.exclude_slices = exclude.into_iter().collect();
        self
    }

    /// Exclude a single slice from downloads.
    pub fn exclude_slice(mut self, slice_idx: SpoolIndex) -> Self {
        self.exclude_slices.insert(slice_idx);
        self
    }

    /// Download at least DATA_SLICES (2f+1) valid slices.
    ///
    /// Requests slices in parallel (up to concurrency limit) and returns
    /// as soon as enough are collected. Respects any excluded slices set
    /// via [`with_excluded_slices`] or [`exclude_slice`].
    pub async fn download_enough_slices(&self) -> Result<Vec<(SpoolIndex, Vec<u8>)>, DownloadError> {
        if self.slice_to_address.is_empty() {
            return Err(DownloadError::NoNodesAvailable);
        }

        let mut collected_slices = Vec::with_capacity(DATA_SLICES);
        let mut futures = FuturesUnordered::new();

        // Semaphore to limit concurrency
        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        // Request all slices in parallel (bounded by semaphore), skipping excluded
        for slice_idx in 0..SPOOL_COUNT as SpoolIndex {
            if self.exclude_slices.contains(&slice_idx) {
                continue;
            }

            // Use spool-based routing: look up the correct node for this slice
            let address = match self.slice_to_address.get(&slice_idx) {
                Some(addr) => addr.clone(),
                None => continue, // Skip slices without a known node
            };
            let factory = self.factory.clone();
            let track_id = self.track_id.clone();
            let sem = semaphore.clone();

            futures.push(async move {
                // Acquire permit before making request
                let _permit = sem.acquire().await.expect("semaphore closed");
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
    pub async fn download_slice(&self, slice_idx: SpoolIndex) -> Result<Vec<u8>, DownloadError> {
        let address = self.slice_to_address.get(&slice_idx)
            .ok_or(DownloadError::InvalidSliceIndex(slice_idx))?;

        let client = self.factory.client_for_address(address)?;
        let data = client.get_slice(&self.track_id, slice_idx).await?;

        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_slice_map(addrs: &[&str]) -> HashMap<SpoolIndex, String> {
        addrs.iter().enumerate()
            .map(|(i, addr)| (i as SpoolIndex, addr.to_string()))
            .collect()
    }

    #[test]
    fn test_downloader_creation() {
        let factory = NodeCommunicationFactory::new();
        let slice_map = make_slice_map(&["localhost:8080", "localhost:8081"]);

        let downloader = ParallelDownloader::new("track_123".to_string(), slice_map, factory);

        // Just verify it creates without panic
        assert_eq!(downloader.track_id, "track_123");
        assert!(downloader.exclude_slices.is_empty());
    }

    #[test]
    fn test_downloader_with_exclusions() {
        let factory = NodeCommunicationFactory::new();
        let slice_map = make_slice_map(&["localhost:8080"]);

        let downloader = ParallelDownloader::new("track_123".to_string(), slice_map, factory)
            .exclude_slice(42)
            .exclude_slice(100);

        assert_eq!(downloader.exclude_slices.len(), 2);
        assert!(downloader.exclude_slices.contains(&42));
        assert!(downloader.exclude_slices.contains(&100));
    }

    #[test]
    fn test_downloader_with_excluded_slices_iter() {
        let factory = NodeCommunicationFactory::new();
        let slice_map = make_slice_map(&["localhost:8080"]);
        let excludes: Vec<SpoolIndex> = vec![10, 20, 30];

        let downloader = ParallelDownloader::new("track_123".to_string(), slice_map, factory)
            .with_excluded_slices(excludes);

        assert_eq!(downloader.exclude_slices.len(), 3);
        assert!(downloader.exclude_slices.contains(&10));
        assert!(downloader.exclude_slices.contains(&20));
        assert!(downloader.exclude_slices.contains(&30));
    }
}
