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
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: u64,
    pub inflight: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CacheEntry {
    pub(super) size: u64,
    pub(super) last_access: u64,
}

#[derive(Debug, Default)]
pub(super) struct CacheState {
    pub(super) entries: HashMap<SliceCacheKey, CacheEntry>,
    pub(super) total_bytes: u64,
    clock: u64,
}

impl CacheState {
    fn next_access(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    pub(super) fn upsert(&mut self, key: SliceCacheKey, size: u64) {
        let last_access = self.next_access();
        if let Some(previous) = self.entries.insert(key, CacheEntry { size, last_access }) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.size);
        }
        self.total_bytes = self.total_bytes.saturating_add(size);
    }

    pub(super) fn touch(&mut self, key: SliceCacheKey, size: u64) {
        let last_access = self.next_access();
        match self.entries.get_mut(&key) {
            Some(entry) => {
                if entry.size != size {
                    self.total_bytes = self.total_bytes.saturating_sub(entry.size);
                    self.total_bytes = self.total_bytes.saturating_add(size);
                    entry.size = size;
                }
                entry.last_access = last_access;
            }
            None => {
                self.entries.insert(key, CacheEntry { size, last_access });
                self.total_bytes = self.total_bytes.saturating_add(size);
            }
        }
    }

    pub(super) fn remove(&mut self, key: SliceCacheKey) -> Option<CacheEntry> {
        let removed = self.entries.remove(&key)?;
        self.total_bytes = self.total_bytes.saturating_sub(removed.size);
        Some(removed)
    }
}
