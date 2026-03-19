//! Parallel downloader for slice retrieval.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::stream::{FuturesUnordered, StreamExt};
use tape_core::spooler::SpoolIndex;
use tape_core::types::NodeId;
use tape_protocol::api::{Api, GetSliceReq};
use solana_sdk::pubkey::Pubkey;
use tape_retry::{retry_if, RetryConfig, Retryable};
use tokio::sync::Semaphore;

use crate::error::DownloadError;

/// Default concurrency limit for parallel downloads.
/// This limits how many HTTP requests are in flight at once.
const DEFAULT_CONCURRENCY: usize = 64;

/// Parallel downloader for retrieving slices from storage nodes.
pub struct ParallelDownloader {
    track: Pubkey,
    /// Maps slice_index → NodeId (for proper spool-based routing)
    slice_to_node: HashMap<SpoolIndex, NodeId>,
    concurrency: usize,
    /// Minimum slices needed for reconstruction (k from track's encoding profile).
    min_slices: usize,
    /// Slice indices to exclude from downloads (e.g., for recovery).
    exclude_slices: HashSet<SpoolIndex>,
}

impl ParallelDownloader {
    /// Create a new downloader with spool-based routing.
    pub fn new(
        track: Pubkey,
        slice_to_node: HashMap<SpoolIndex, NodeId>,
        min_slices: usize,
    ) -> Self {
        Self {
            track,
            slice_to_node,
            concurrency: DEFAULT_CONCURRENCY,
            min_slices,
            exclude_slices: HashSet::new(),
        }
    }

    /// Create a new downloader with custom concurrency limit.
    pub fn with_concurrency(
        track: Pubkey,
        slice_to_node: HashMap<SpoolIndex, NodeId>,
        min_slices: usize,
        concurrency: usize,
    ) -> Self {
        Self {
            track,
            slice_to_node,
            concurrency,
            min_slices,
            exclude_slices: HashSet::new(),
        }
    }

    /// Set slices to exclude from downloads.
    pub fn with_excluded_slices(mut self, exclude: impl IntoIterator<Item = SpoolIndex>) -> Self {
        self.exclude_slices = exclude.into_iter().collect();
        self
    }

    /// Exclude a single slice from downloads.
    pub fn exclude_slice(mut self, slice_idx: SpoolIndex) -> Self {
        self.exclude_slices.insert(slice_idx);
        self
    }

    /// Download at least min_slices (k) valid slices via the Api trait.
    ///
    /// Requests slices in parallel (up to concurrency limit) and returns
    /// as soon as enough are collected.
    pub async fn download_enough_slices<P: Api>(&self, peer_client: &P) -> Result<Vec<(SpoolIndex, Vec<u8>)>, DownloadError> {
        if self.slice_to_node.is_empty() {
            return Err(DownloadError::NoNodesAvailable);
        }

        let mut collected_slices = Vec::with_capacity(self.min_slices);
        let mut futures = FuturesUnordered::new();

        let semaphore = Arc::new(Semaphore::new(self.concurrency));

        for (&slice_idx, &node_id) in &self.slice_to_node {
            if self.exclude_slices.contains(&slice_idx) {
                continue;
            }

            let track = self.track;
            let sem = semaphore.clone();

            futures.push(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");
                let req = GetSliceReq {
                    track: track.into(),
                    spool: slice_idx,
                };
                let result = retry_if(
                    RetryConfig::ten(),
                    None,
                    || peer_client.get_slice(node_id, &req),
                    Retryable::is_retryable,
                ).await;
                (slice_idx, result)
            });
        }

        while let Some((slice_idx, result)) = futures.next().await {
            match result {
                Ok(res) => {
                    collected_slices.push((slice_idx, res.data));
                    if collected_slices.len() >= self.min_slices {
                        break;
                    }
                }
                Err(_) => {
                    // Slice fetch failed, continue with others
                }
            }
        }

        if collected_slices.len() < self.min_slices {
            return Err(DownloadError::InsufficientSlices {
                got: collected_slices.len(),
                need: self.min_slices,
            });
        }

        Ok(collected_slices)
    }

    /// Download a specific slice via the Api trait.
    pub async fn download_slice<P: Api>(&self, peer_client: &P, slice_idx: SpoolIndex) -> Result<Vec<u8>, DownloadError> {
        let &node_id = self.slice_to_node.get(&slice_idx)
            .ok_or(DownloadError::InvalidSliceIndex(slice_idx))?;

        let req = GetSliceReq {
            track: self.track.into(),
            spool: slice_idx,
        };

        let res = retry_if(
            RetryConfig::ten(),
            None,
            || peer_client.get_slice(node_id, &req),
            tape_retry::Retryable::is_retryable,
        ).await.map_err(|e| DownloadError::Node(e.to_string()))?;

        Ok(res.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_slice_map(count: usize) -> HashMap<SpoolIndex, NodeId> {
        (0..count)
            .map(|i| (i as SpoolIndex, NodeId::new(i as u64 + 1)))
            .collect()
    }

    #[test]
    fn downloader_creation() {
        let slice_map = make_slice_map(2);
        let min_slices = 10;

        let track = Pubkey::new_unique();
        let downloader = ParallelDownloader::new(track, slice_map, min_slices);

        assert_eq!(downloader.track, track);
        assert!(downloader.exclude_slices.is_empty());
        assert_eq!(downloader.min_slices, min_slices);
    }

    #[test]
    fn downloader_with_exclusions() {
        let slice_map = make_slice_map(1);
        let min_slices = 6;

        let downloader = ParallelDownloader::new(Pubkey::new_unique(), slice_map, min_slices)
            .exclude_slice(42)
            .exclude_slice(100);

        assert_eq!(downloader.exclude_slices.len(), 2);
        assert!(downloader.exclude_slices.contains(&42));
        assert!(downloader.exclude_slices.contains(&100));
    }

    #[test]
    fn downloader_with_excluded_slices_iter() {
        let slice_map = make_slice_map(1);
        let min_slices = 10;
        let excludes: Vec<SpoolIndex> = vec![10, 20, 30];

        let downloader = ParallelDownloader::new(Pubkey::new_unique(), slice_map, min_slices)
            .with_excluded_slices(excludes);

        assert_eq!(downloader.exclude_slices.len(), 3);
        assert!(downloader.exclude_slices.contains(&10));
        assert!(downloader.exclude_slices.contains(&20));
        assert!(downloader.exclude_slices.contains(&30));
    }
}
