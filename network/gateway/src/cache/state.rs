use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::types::SliceKey;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SliceCacheKey {
    pub spool_id: SpoolIndex,
    pub track_address: Address,
}

impl SliceCacheKey {
    pub fn new(spool_id: SpoolIndex, track_address: Address) -> Self {
        Self {
            spool_id,
            track_address,
        }
    }
}

impl From<SliceKey> for SliceCacheKey {
    fn from(key: SliceKey) -> Self {
        Self::new(key.spool_id, key.track_address)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheSource {
    Hit,
    Miss,
}

#[derive(Debug)]
pub struct CacheRead {
    pub data: Bytes,
    pub source: CacheSource,
    pub is_verified: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: u64,
    pub inflight: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct CacheEntry {
    pub size: u64,
    pub last_access: u64,
    pub is_verified: bool,
}

#[derive(Debug, Default)]
pub struct CacheState {
    pub entries: HashMap<SliceCacheKey, CacheEntry>,
    pub total_bytes: u64,
    clock: u64,
    by_access: BTreeMap<u64, SliceCacheKey>,
}

impl CacheState {
    fn next_access(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    pub fn upsert(&mut self, key: SliceCacheKey, size: u64, is_verified: bool) {
        let last_access = self.next_access();
        let entry = CacheEntry {
            size,
            last_access,
            is_verified,
        };
        if let Some(previous) = self.entries.insert(key, entry) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.size);
            self.by_access.remove(&previous.last_access);
        }
        self.by_access.insert(last_access, key);
        self.total_bytes = self.total_bytes.saturating_add(size);
    }

    /// Mark an entry used and answer whether its bytes were checked since they reached the store
    pub fn touch(&mut self, key: SliceCacheKey, size: u64) -> bool {
        let last_access = self.next_access();
        self.by_access.insert(last_access, key);
        match self.entries.get_mut(&key) {
            Some(entry) => {
                if entry.size != size {
                    self.total_bytes = self.total_bytes.saturating_sub(entry.size);
                    self.total_bytes = self.total_bytes.saturating_add(size);
                    entry.size = size;
                    entry.is_verified = false;
                }
                self.by_access.remove(&entry.last_access);
                entry.last_access = last_access;
                entry.is_verified
            }
            None => {
                let entry = CacheEntry {
                    size,
                    last_access,
                    is_verified: false,
                };
                self.entries.insert(key, entry);
                self.total_bytes = self.total_bytes.saturating_add(size);
                false
            }
        }
    }

    pub fn mark_verified(&mut self, key: SliceCacheKey) {
        if let Some(entry) = self.entries.get_mut(&key) {
            entry.is_verified = true;
        }
    }

    pub fn remove(&mut self, key: SliceCacheKey) -> Option<CacheEntry> {
        let removed = self.entries.remove(&key)?;
        self.by_access.remove(&removed.last_access);
        self.total_bytes = self.total_bytes.saturating_sub(removed.size);
        Some(removed)
    }

    /// The entry used longest ago
    pub fn oldest(&self) -> Option<SliceCacheKey> {
        self.by_access.first_key_value().map(|(_, key)| *key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(spool: u64) -> SliceCacheKey {
        SliceCacheKey::new(SpoolIndex(spool), Address::new_unique())
    }

    // the oldest entry follows every touch, upsert and removal
    #[test]
    fn access_order() {
        let mut state = CacheState::default();
        let (first, second, third) = (key(1), key(2), key(3));
        state.upsert(first, 1, true);
        state.upsert(second, 1, true);
        state.upsert(third, 1, true);

        state.touch(first, 1);
        let after_touch = state.oldest();
        state.upsert(second, 2, true);
        let after_upsert = state.oldest();
        state.remove(third);
        let after_remove = state.oldest();

        assert_eq!(after_touch, Some(second));
        assert_eq!(after_upsert, Some(third));
        assert_eq!(after_remove, Some(first));
        assert_eq!(state.total_bytes, 3);
    }
}
