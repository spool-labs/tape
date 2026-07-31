//! Read-after-write staging for the S3 surface.
//!
//! A freshly written object is not resolvable through the S3 read path until the
//! block ingestor tails its slot and the track certifies (seconds later).
//! ClickHouse S3 disks require read-after-write, so the write path stashes each
//! object here and the read path serves it until the ingestor catches up.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;

/// How long an object is served from staging before the ingestor is expected to
/// have made it resolvable on-chain.
const STAGING_TTL: Duration = Duration::from_secs(60);
/// Cap on the bytes held in staging; the oldest entries are evicted first.
const STAGING_MAX_BYTES: usize = 256 * 1024 * 1024;

/// A staged object plus the metadata a GET/HEAD needs to answer without the
/// on-chain index.
#[derive(Clone)]
pub struct StagedObject {
    /// Object bytes exactly as written.
    pub bytes: Bytes,
    /// Content type reported on GET and HEAD.
    pub content_type: ContentType,
    /// ETag the client was given at write time.
    pub etag: Hash,
    /// Last-modified time in unix seconds.
    pub block_time: i64,
    /// Insertion time, driving the TTL and the eviction order.
    inserted: Instant,
}

impl StagedObject {
    /// Capture an object with the instant it entered staging.
    pub fn new(bytes: Bytes, content_type: ContentType, etag: Hash, block_time: i64) -> Self {
        Self {
            bytes,
            content_type,
            etag,
            block_time,
            inserted: Instant::now(),
        }
    }
}

/// Freshly written objects keyed by `(bucket tape, object key)`, with a running
/// byte total so the budget never needs a full scan.
#[derive(Default)]
struct StagingEntries {
    objects: HashMap<(Address, String), StagedObject>,
    total_bytes: usize,
}

impl StagingEntries {
    /// Remove one entry, keeping the byte total in step.
    fn remove(&mut self, map_key: &(Address, String)) -> Option<StagedObject> {
        let removed = self.objects.remove(map_key)?;
        self.total_bytes -= removed.bytes.len();
        Some(removed)
    }

    /// Insert one entry (replacing any same-key copy), keeping the byte total
    /// in step.
    fn insert(&mut self, map_key: (Address, String), object: StagedObject) {
        self.total_bytes += object.bytes.len();
        if let Some(replaced) = self.objects.insert(map_key, object) {
            self.total_bytes -= replaced.bytes.len();
        }
    }

    /// Drop every expired entry.
    fn prune_expired(&mut self) {
        let total_bytes = &mut self.total_bytes;
        self.objects.retain(|_, staged| {
            let is_fresh = staged.inserted.elapsed() < STAGING_TTL;
            if !is_fresh {
                *total_bytes -= staged.bytes.len();
            }
            is_fresh
        });
    }
}

/// Freshly written objects served until the ingestor catches up.
#[derive(Default)]
pub struct StagingStore {
    entries: Mutex<StagingEntries>,
}

impl StagingStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stash a just-written object, pruning expired entries and evicting the
    /// oldest until the total fits the byte budget.
    pub fn put(&self, tape: Address, key: String, object: StagedObject) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        entries.prune_expired();
        while entries.total_bytes + object.bytes.len() > STAGING_MAX_BYTES {
            let Some(oldest) = entries
                .objects
                .iter()
                .min_by_key(|(_, staged)| staged.inserted)
                .map(|(map_key, _)| map_key.clone())
            else {
                break;
            };
            entries.remove(&oldest);
        }
        entries.insert((tape, key), object);
    }

    /// Serve a staged object if present and still fresh; a stale entry is dropped.
    pub fn get(&self, tape: Address, key: &str) -> Option<StagedObject> {
        let mut entries = self.entries.lock().ok()?;
        let map_key = (tape, key.to_string());
        match entries.objects.get(&map_key) {
            Some(staged) if staged.inserted.elapsed() < STAGING_TTL => Some(staged.clone()),
            Some(_) => {
                entries.remove(&map_key);
                None
            }
            None => None,
        }
    }

    /// Staged keys for one bucket, in lexicographic order.
    ///
    /// Staged keys are listed too, or a client does not see its own write.
    /// `start` is the inclusive name to begin at, matching the on-chain index.
    pub fn staged_from(
        &self,
        tape: Address,
        prefix: &[u8],
        start: &[u8],
    ) -> Vec<(Vec<u8>, StagedObject)> {
        let Ok(mut entries) = self.entries.lock() else {
            return Vec::new();
        };
        entries.prune_expired();

        // Filtered before anything is copied, so a bucket with a large staging
        // set does not allocate a name and clone an object per entry only to
        // drop most of them.
        let mut staged: Vec<(Vec<u8>, StagedObject)> = entries
            .objects
            .iter()
            .filter(|((bucket, key), _)| {
                *bucket == tape
                    && key.as_bytes().starts_with(prefix)
                    && key.as_bytes() >= start
            })
            .map(|((_, key), object)| (key.as_bytes().to_vec(), object.clone()))
            .collect();
        staged.sort_by(|left, right| left.0.cmp(&right.0));
        staged
    }

    /// Drop a staged object so a staged copy never outlives a delete of its key.
    pub fn remove(&self, tape: Address, key: &str) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&(tape, key.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn staged(byte: u8, len: usize) -> StagedObject {
        StagedObject::new(
            Bytes::from(vec![byte; len]),
            ContentType::Unknown,
            Hash::default(),
            1_700_000_000,
        )
    }

    // a staged object reads back by its bucket+key, and a delete drops it
    #[test]
    fn put_get_remove() {
        let store = StagingStore::new();
        let tape = Address::new([7u8; 32]);
        assert!(store.get(tape, "a/b.bin").is_none());

        store.put(tape, "a/b.bin".to_string(), staged(0xAB, 16));
        let hit = store.get(tape, "a/b.bin").expect("staged object present");
        assert_eq!(hit.bytes.as_ref(), &[0xAB; 16]);

        store.remove(tape, "a/b.bin");
        assert!(store.get(tape, "a/b.bin").is_none());
    }

    // a different bucket with the same key does not collide
    #[test]
    fn bucket_scoped() {
        let store = StagingStore::new();
        let one = Address::new([1u8; 32]);
        let two = Address::new([2u8; 32]);
        store.put(one, "k".to_string(), staged(0x11, 8));
        assert!(store.get(one, "k").is_some());
        assert!(store.get(two, "k").is_none());
    }

    // replacing a key does not double-count its bytes against the budget
    #[test]
    fn replace_accounting() {
        let store = StagingStore::new();
        let tape = Address::new([3u8; 32]);
        let half = STAGING_MAX_BYTES / 2;
        store.put(tape, "a".to_string(), staged(0x01, half));
        store.put(tape, "a".to_string(), staged(0x02, half));

        // With correct accounting the total is one half, so another half fits
        // without evicting anything.
        store.put(tape, "b".to_string(), staged(0x03, half));

        assert!(store.get(tape, "a").is_some());
        assert!(store.get(tape, "b").is_some());
    }

    // the byte budget evicts the oldest entry to admit a new one
    #[test]
    fn evicts_oldest() {
        let store = StagingStore::new();
        let tape = Address::new([9u8; 32]);
        // Two entries that together exceed the budget force an eviction.
        store.put(tape, "old".to_string(), staged(0x01, STAGING_MAX_BYTES));
        store.put(tape, "new".to_string(), staged(0x02, 1));
        assert!(store.get(tape, "old").is_none(), "oldest evicted");
        assert!(store.get(tape, "new").is_some(), "newest retained");
    }
}
