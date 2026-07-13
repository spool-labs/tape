//! Core storage trait defining the key-value store interface

use crate::{Result, WriteBatch};

/// Iterator direction for scanning (lexicographic order)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Ascending order (smallest to largest)
    Asc,
    /// Descending order (largest to smallest)
    Desc,
}

/// Key-value pair type returned by iterators
pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Boxed iterator type for store operations
pub type StoreIter<'a> = Box<dyn Iterator<Item = KeyValue> + 'a>;

/// Role of a physical storage volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreVolume {
    /// The metadata/index volume, or the whole store when not split.
    Primary,
    /// The bulk volume for large payloads.
    Bulk,
}

/// Best-effort disk usage for one physical storage volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskVolume {
    pub volume: StoreVolume,
    pub used_bytes: u64,
    pub free_bytes: Option<u64>,
}

/// Best-effort on-disk usage for one column family, tagged with its volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CfDiskUsage {
    /// Column family name.
    pub cf: String,
    /// Physical volume the column lives on.
    pub volume: StoreVolume,
    /// Bytes held in SST files.
    pub sst_bytes: u64,
    /// Bytes held in blob files, zero for columns that store values inline.
    pub blob_bytes: u64,
    /// Estimated live key count.
    pub num_keys: u64,
}

impl CfDiskUsage {
    /// Total on-disk bytes for the column family (SST plus blob files).
    pub fn total_bytes(&self) -> u64 {
        self.sst_bytes.saturating_add(self.blob_bytes)
    }
}

/// Trait for key-value storage with column family support
///
/// All implementations must be thread-safe (Send + Sync).
/// Column families are namespaces for keys - each CF has its own key space.
pub trait Store: Send + Sync {
    /// Get a value by key from the specified column family.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair into the specified column family.
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key from the specified column family.
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Check if a key exists in the specified column family.
    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool>;

    /// Apply a batch of write operations atomically.
    ///
    /// Atomicity holds only within a single backend. A backend split across
    /// independent instances may write a cross-instance batch non-atomically.
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Delete every key in the range `[start, end)` from the column family.
    /// Backends can override with a native range tombstone; the default collects
    /// the keys in range and deletes them in one batch.
    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        let keys: Vec<Vec<u8>> = self.iter_range(cf, start, end)?.map(|(k, _)| k).collect();
        if keys.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::new();
        for key in &keys {
            batch.delete(cf, key);
        }
        self.write_batch(batch)
    }

    /// Iterate over all entries in lexicographic key order.
    fn iter(&self, cf: &str) -> Result<StoreIter<'_>>;

    /// Iterate over entries matching the key prefix in lexicographic order.
    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>>;

    /// Collect the keys under `prefix` WITHOUT reading their values. Backends can
    /// override to skip value (e.g. blob-file) reads when only keys are needed.
    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self.iter_prefix(cf, prefix)?.map(|(k, _)| k).collect())
    }

    /// Iterate from the start key (inclusive) in the specified direction.
    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>>;

    /// Iterate over entries in the key range [start, end) in lexicographic order.
    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>>;

    /// Best-effort total backend size in bytes.
    ///
    /// Persistent stores should include DB overhead such as SSTs, WALs, indexes,
    /// metadata, and similar files. In-memory stores can return an approximate
    /// resident footprint.
    fn actual_size_bytes(&self) -> Result<u64> {
        Ok(0)
    }

    /// Best-effort free disk space available to the backend.
    ///
    /// Backends without a filesystem can return `None`.
    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Cheap on-disk footprint of persisted live data, safe to poll on every
    /// scrape. Must not walk the filesystem; backends that cannot answer
    /// cheaply return nothing. Required so delegating stores cannot silently
    /// inherit a no-answer default.
    fn live_data_size_bytes(&self) -> Result<Option<u64>>;

    /// Cheap approximate key count for a named column family, safe to poll.
    /// Backends that cannot estimate cheaply return nothing. Required for the
    /// same reason.
    fn key_count_estimate(&self, cf: &str) -> Result<Option<u64>>;

    /// Best-effort on-disk usage per column family.
    ///
    /// Persistent backends report SST and blob-file bytes and an estimated key
    /// count for each column family, tagged with its volume. Backends that
    /// cannot introspect cheaply return an empty vec.
    fn cf_disk_usage(&self) -> Result<Vec<CfDiskUsage>> {
        Ok(Vec::new())
    }

    /// Best-effort background space reclamation.
    ///
    /// Persistent stores can use this to compact tombstoned data and release
    /// backend space. Backends that do not support reclamation should no-op.
    fn reclaim_space(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort disk usage per physical volume.
    ///
    /// Backends split across devices report one entry per volume. The default
    /// is a single primary volume covering the whole store.
    fn disk_volumes(&self) -> Result<Vec<DiskVolume>> {
        Ok(vec![DiskVolume {
            volume: StoreVolume::Primary,
            used_bytes: self.actual_size_bytes()?,
            free_bytes: self.available_disk_bytes()?,
        }])
    }
}
