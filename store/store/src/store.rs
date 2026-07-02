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

    /// Best-effort background space reclamation.
    ///
    /// Persistent stores can use this to compact tombstoned data and release
    /// backend space. Backends that do not support reclamation should no-op.
    fn reclaim_space(&self) -> Result<()> {
        Ok(())
    }
}
