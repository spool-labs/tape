use std::collections::HashMap;

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
    pub data: Vec<u8>,
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
        }
        self.total_bytes = self.total_bytes.saturating_add(size);
    }

    /// Mark an entry used and answer whether its bytes were checked since they reached the store
    pub fn touch(&mut self, key: SliceCacheKey, size: u64) -> bool {
        let last_access = self.next_access();
        match self.entries.get_mut(&key) {
            Some(entry) => {
                if entry.size != size {
                    self.total_bytes = self.total_bytes.saturating_sub(entry.size);
                    self.total_bytes = self.total_bytes.saturating_add(size);
                    entry.size = size;
                    entry.is_verified = false;
                }
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
        self.total_bytes = self.total_bytes.saturating_sub(removed.size);
        Some(removed)
    }
}
