//! In-memory cache for snapshot chunks between build and on-chain confirmation.
//!
//! The build pipeline produces encoded slices for local spool groups well
//! before the chunk's on-chain track address is known (`track_number` is
//! assigned by the `WriteSnapshot` program). The cache holds the slices
//! (keyed by `(epoch, group, chunk_index)`) until the matching
//! `SnapshotWritten` event arrives, at which point the write driver
//! flushes slices into `SliceCol` under the now-known track address and
//! drops the entry.
//!
//! Two consumers besides the driver read from the cache:
//! - the `snapshot_write` HTTP handler, which compares a peer's requested
//!   `value_hash` against the local build's `BlobInfo::get_hash()`;
//! - the `snapshot_finalize` HTTP handler, which checks that every chunk
//!   built locally for a group has been posted on-chain.

use std::collections::HashMap;
use std::sync::RwLock;

use tape_core::erasure::SPOOL_GROUP_SIZE;
use tape_core::spooler::SpoolGroup;
use tape_core::track::blob::BlobInfo;
use tape_core::types::{ChunkNumber, EpochNumber};
use tape_crypto::address::Address;
use tape_crypto::hash::Hash;

/// Per-chunk cache key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ChunkKey {
    pub epoch: EpochNumber,
    pub group: SpoolGroup,
    pub chunk_index: ChunkNumber,
}

impl ChunkKey {
    pub const fn new(epoch: EpochNumber, group: SpoolGroup, chunk_index: ChunkNumber) -> Self {
        Self {
            epoch,
            group,
            chunk_index,
        }
    }
}

/// One cached chunk: the BlobInfo needed for signature matching, the slice
/// payload held until persistence, and an optional on-chain track address.
pub struct ChunkEntry {
    pub blob: BlobInfo,
    pub slices: [Vec<u8>; SPOOL_GROUP_SIZE],
    /// Set once the chunk's `SnapshotWritten` event has been observed.
    pub posted_track: Option<Address>,
}

/// Snapshot progress counters for one `(epoch, group)`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GroupProgress {
    pub built: usize,
    pub posted: usize,
}

impl GroupProgress {
    pub fn is_empty(self) -> bool {
        self.built == 0
    }

    pub fn is_complete(self) -> bool {
        self.built > 0 && self.built == self.posted
    }
}

/// In-memory snapshot build cache. Thread-safe via a single `RwLock`.
#[derive(Default)]
pub struct SnapshotBuildCache {
    chunks: RwLock<HashMap<ChunkKey, ChunkEntry>>,
}

impl SnapshotBuildCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a freshly built chunk. Overwrites any prior entry for the same key.
    pub fn insert(
        &self,
        key: ChunkKey,
        blob: BlobInfo,
        slices: [Vec<u8>; SPOOL_GROUP_SIZE],
    ) {
        let mut chunks = self.chunks.write().expect("snapshot build cache poisoned");
        chunks.insert(
            key,
            ChunkEntry {
                blob,
                slices,
                posted_track: None,
            },
        );
    }

    /// The `value_hash` the local build committed to for this chunk, or `None`
    /// if the chunk has never been built on this node.
    pub fn value_hash(&self, key: &ChunkKey) -> Option<Hash> {
        let chunks = self.chunks.read().expect("snapshot build cache poisoned");
        chunks.get(key).map(|entry| entry.blob.get_hash())
    }

    /// Record the on-chain track address for a chunk and return its slice
    /// payload so the caller can flush it into `SliceCol`. Returns `None` if
    /// the chunk is unknown to this node.
    pub fn mark_posted(
        &self,
        key: &ChunkKey,
        track_address: Address,
    ) -> Option<[Vec<u8>; SPOOL_GROUP_SIZE]> {
        let mut chunks = self.chunks.write().expect("snapshot build cache poisoned");
        let entry = chunks.get_mut(key)?;
        entry.posted_track = Some(track_address);
        Some(core::array::from_fn(|i| std::mem::take(&mut entry.slices[i])))
    }

    /// Drop a single chunk entry (typically after slices have been flushed).
    pub fn drop_chunk(&self, key: &ChunkKey) {
        let mut chunks = self.chunks.write().expect("snapshot build cache poisoned");
        chunks.remove(key);
    }

    /// Drop every chunk for an epoch (e.g., after the snapshot is `Finalized`).
    pub fn drop_epoch(&self, epoch: EpochNumber) {
        let mut chunks = self.chunks.write().expect("snapshot build cache poisoned");
        chunks.retain(|k, _| k.epoch != epoch);
    }

    /// Progress for `(epoch, group)`: how many chunks are built locally and
    /// how many of those have been observed on-chain.
    pub fn group_progress(&self, epoch: EpochNumber, group: SpoolGroup) -> GroupProgress {
        let chunks = self.chunks.read().expect("snapshot build cache poisoned");
        let mut progress = GroupProgress::default();
        for (k, v) in chunks.iter() {
            if k.epoch == epoch && k.group == group {
                progress.built += 1;
                if v.posted_track.is_some() {
                    progress.posted += 1;
                }
            }
        }
        progress
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::types::{StorageUnits, StripeCount};

    fn sample_blob(commitment: Hash) -> BlobInfo {
        BlobInfo {
            size: StorageUnits::from_bytes(2_048),
            commitment,
            profile: EncodingProfile::basic_default(),
            stripe_size: StorageUnits::from_bytes(512),
            stripe_count: StripeCount(4),
            leaves: [Hash::from([0x44; 32]); SPOOL_GROUP_SIZE],
        }
    }

    fn empty_slices() -> [Vec<u8>; SPOOL_GROUP_SIZE] {
        core::array::from_fn(|_| Vec::new())
    }

    fn key(epoch: u64, group: u64, chunk_index: u64) -> ChunkKey {
        ChunkKey::new(
            EpochNumber(epoch),
            SpoolGroup(group),
            ChunkNumber(chunk_index),
        )
    }

    #[test]
    fn insert_and_value_hash() {
        let cache = SnapshotBuildCache::new();
        let k = key(1, 2, 3);
        let blob = sample_blob(Hash::from([0x11; 32]));
        let expected = blob.get_hash();

        assert!(cache.value_hash(&k).is_none());
        cache.insert(k, blob, empty_slices());
        assert_eq!(cache.value_hash(&k), Some(expected));
    }

    #[test]
    fn mark_posted_returns_slices() {
        let cache = SnapshotBuildCache::new();
        let k = key(5, 0, 0);
        let slices: [Vec<u8>; SPOOL_GROUP_SIZE] =
            core::array::from_fn(|i| vec![i as u8; 4]);

        cache.insert(k, sample_blob(Hash::from([0x22; 32])), slices.clone());

        let taken = cache
            .mark_posted(&k, Address::from([0xAA; 32]))
            .expect("entry present");
        assert_eq!(taken, slices);
        assert_eq!(
            cache.group_progress(EpochNumber(5), SpoolGroup(0)),
            GroupProgress { built: 1, posted: 1 }
        );
    }

    #[test]
    fn drop_epoch_removes_all_matching() {
        let cache = SnapshotBuildCache::new();
        let blob = sample_blob(Hash::from([0x33; 32]));
        cache.insert(key(1, 0, 0), blob.clone(), empty_slices());
        cache.insert(key(1, 0, 1), blob.clone(), empty_slices());
        cache.insert(key(2, 0, 0), blob, empty_slices());

        cache.drop_epoch(EpochNumber(1));

        assert!(cache.value_hash(&key(1, 0, 0)).is_none());
        assert!(cache.value_hash(&key(1, 0, 1)).is_none());
        assert!(cache.value_hash(&key(2, 0, 0)).is_some());
    }

    #[test]
    fn group_progress_reflects_completion() {
        let cache = SnapshotBuildCache::new();
        let blob = sample_blob(Hash::from([0x44; 32]));
        cache.insert(key(1, 3, 0), blob.clone(), empty_slices());
        cache.insert(key(1, 3, 1), blob, empty_slices());

        assert_eq!(
            cache.group_progress(EpochNumber(1), SpoolGroup(3)),
            GroupProgress { built: 2, posted: 0 }
        );

        cache.mark_posted(&key(1, 3, 0), Address::from([0x01; 32]));
        assert!(!cache
            .group_progress(EpochNumber(1), SpoolGroup(3))
            .is_complete());

        cache.mark_posted(&key(1, 3, 1), Address::from([0x02; 32]));
        let progress = cache.group_progress(EpochNumber(1), SpoolGroup(3));
        assert!(progress.is_complete());
        assert_eq!(progress.built, 2);
    }
}
