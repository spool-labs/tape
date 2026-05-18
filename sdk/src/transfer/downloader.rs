//! Parallel downloader for slice retrieval.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use futures::stream::{FuturesUnordered, StreamExt};
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_protocol::api::{Api, ApiError, GetSliceReq, GetSliceRes};
use tape_retry::{Backoff, RetryConfig, Retryable};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::warn;

use crate::error::DownloadError;

/// Default concurrency limit for parallel downloads.
/// This limits how many HTTP requests are in flight at once.
const DEFAULT_CONCURRENCY: usize = 8;

/// Parallel downloader for retrieving slices from storage nodes.
pub struct ParallelDownloader {
    track: Address,
    slice_to_node: HashMap<SpoolIndex, Address>,
    concurrency: usize,
    min_slices: usize,
    exclude_slices: HashSet<SpoolIndex>,
}

impl ParallelDownloader {
    /// Create a new downloader with spool-based routing.
    pub fn new(
        track: Address,
        slice_to_node: HashMap<SpoolIndex, Address>,
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
        track: Address,
        slice_to_node: HashMap<SpoolIndex, Address>,
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

        let sem = Arc::new(Semaphore::new(self.concurrency));

        for (&slice_idx, &node) in &self.slice_to_node {
            if self.exclude_slices.contains(&slice_idx) {
                continue;
            }

            let track = self.track;
            let sem = sem.clone();

            futures.push(async move {
                let _permit = sem.acquire().await.expect("semaphore closed");
                let result = download_slice_with_retry(peer_client, track, node, slice_idx).await;
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
                Err(error) => {
                    warn!(
                        slice = %slice_idx,
                        error = %error,
                        "slice fetch failed, continuing with others"
                    );
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
        let &node = self.slice_to_node.get(&slice_idx)
            .ok_or(DownloadError::InvalidSliceIndex(slice_idx))?;

        let res = download_slice_with_retry(peer_client, self.track, node, slice_idx)
            .await
            .map_err(|e| DownloadError::Node(e.to_string()))?;

        Ok(res.data)
    }
}

async fn download_slice_with_retry<P: Api>(
    peer_client: &P,
    track: Address,
    node: Address,
    slice_idx: SpoolIndex,
) -> Result<GetSliceRes, ApiError> {
    let req = GetSliceReq {
        track: track.into(),
        spool: slice_idx,
    };
    let mut backoff = Backoff::new(RetryConfig::ten());

    loop {
        let started = Instant::now();
        match peer_client.get_slice(node, &req).await {
            Ok(res) => return Ok(res),
            Err(error) if !error.is_retryable() => {
                warn!(
                    track = %track,
                    node = %node,
                    slice = %slice_idx,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    error = %error,
                    "slice fetch failed with non-retryable error"
                );
                return Err(error);
            }
            Err(error) => {
                let elapsed_ms = started.elapsed().as_millis() as u64;
                let Some(delay) = backoff.next_delay() else {
                    warn!(
                        track = %track,
                        node = %node,
                        slice = %slice_idx,
                        attempt = backoff.attempt(),
                        elapsed_ms,
                        error = %error,
                        "slice fetch exhausted retries"
                    );
                    return Err(error);
                };

                warn!(
                    track = %track,
                    node = %node,
                    slice = %slice_idx,
                    attempt = backoff.attempt(),
                    delay_ms = delay.as_millis() as u64,
                    elapsed_ms,
                    error = %error,
                    "slice fetch failed, retrying after backoff"
                );

                sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_crypto::address::Address;

    fn make_slice_map(count: usize) -> HashMap<SpoolIndex, Address> {
        (0..count)
            .map(|i| (SpoolIndex::from(i as u64), Address::new_unique()))
            .collect()
    }

    #[test]
    fn downloader_creation() {
        let slice_map = make_slice_map(2);
        let min_slices = 10;

        let track = Address::new_unique();
        let downloader = ParallelDownloader::new(track, slice_map, min_slices);

        assert_eq!(downloader.track, track);
        assert!(downloader.exclude_slices.is_empty());
        assert_eq!(downloader.min_slices, min_slices);
    }

    #[test]
    fn downloader_with_exclusions() {
        let slice_map = make_slice_map(1);
        let min_slices = 6;
        let first = SpoolIndex::from(42);
        let second = SpoolIndex::from(100);

        let downloader = ParallelDownloader::new(Address::new_unique(), slice_map, min_slices)
            .exclude_slice(first)
            .exclude_slice(second);

        assert_eq!(downloader.exclude_slices.len(), 2);
        assert!(downloader.exclude_slices.contains(&first));
        assert!(downloader.exclude_slices.contains(&second));
    }

    #[test]
    fn downloader_with_excluded_slices_iter() {
        let slice_map = make_slice_map(1);
        let min_slices = 10;
        let excludes: Vec<SpoolIndex> = vec![
            SpoolIndex::from(10),
            SpoolIndex::from(20),
            SpoolIndex::from(30),
        ];

        let downloader = ParallelDownloader::new(Address::new_unique(), slice_map, min_slices)
            .with_excluded_slices(excludes);

        assert_eq!(downloader.exclude_slices.len(), 3);
        assert!(downloader.exclude_slices.contains(&SpoolIndex::from(10)));
        assert!(downloader.exclude_slices.contains(&SpoolIndex::from(20)));
        assert!(downloader.exclude_slices.contains(&SpoolIndex::from(30)));
    }
}
