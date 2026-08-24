//! Core storage trait defining the key-value store interface

use std::future::Future;

use crate::{Result, Value, WriteBatch};

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
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>>;

    /// Get several values from one column family, answered in the order asked.
    ///
    /// The default asks one at a time. A backend overrides to put them all in
    /// front of its device at once.
    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(cf, key)?);
        }
        Ok(values)
    }

    /// Get a value by key, awaited rather than waited for on the calling thread.
    ///
    /// Not dispatchable through `dyn Store`, since the future's type is the
    /// backend's own. The default answers from the blocking call.
    fn get_wait(&self, cf: &str, key: &[u8]) -> impl Future<Output = Result<Option<Value>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get(cf, key))
    }

    /// Get several values from one column family, awaited, answered in order.
    fn get_many_wait(
        &self,
        cf: &str,
        keys: &[&[u8]],
    ) -> impl Future<Output = Result<Vec<Option<Value>>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get_many(cf, keys))
    }

    /// Get part of one value, from `offset` for `len` bytes.
    ///
    /// Clamped the way a `pread` is, and a missing key answers nothing at all.
    fn get_range(&self, cf: &str, key: &[u8], offset: u64, len: usize) -> Result<Option<Value>> {
        Ok(self.get(cf, key)?.map(|value| range_of(value, offset, len)))
    }

    /// Get part of one value, awaited rather than waited for on the calling thread.
    fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Option<Value>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get_range(cf, key, offset, len))
    }

    /// Put a key-value pair into the specified column family.
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Put a key-value pair, awaited rather than waited for on the calling thread.
    ///
    /// Not dispatchable through `dyn Store`, for the same reason `get_wait` is not.
    fn put_wait(
        &self,
        cf: &str,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.put(cf, key, value))
    }

    /// Delete a key from the specified column family.
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Check if a key exists in the specified column family.
    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool>;

    /// Apply a batch of write operations atomically.
    ///
    /// Atomicity holds only within a single backend. A backend split across
    /// independent instances may write a cross-instance batch non-atomically.
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Apply a batch of write operations atomically, awaited.
    ///
    /// One durability point for the whole batch, however many keys it carries.
    fn write_batch_wait(&self, batch: WriteBatch) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.write_batch(batch))
    }

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

    /// One page of a column family, resumable by an opaque mark.
    ///
    /// Promises only that a full sweep hands out every live key at least once,
    /// in whatever order the backend keeps. `None` back means the family is
    /// done. A mark is the backend's to read, so hand back whatever the last
    /// page answered and nothing else.
    fn sweep(
        &self,
        cf: &str,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<Vec<u8>>)> {
        // The mark is inclusive: it is the first key this page did not return,
        // so the next page starts on it. Skipping it would drop one key per page
        // boundary.
        let start = from.unwrap_or(&[]);
        let mut rows = Vec::with_capacity(limit);
        let mut next = None;
        for (key, value) in self.iter_from(cf, start, Direction::Asc)? {
            if rows.len() == limit {
                next = Some(key);
                break;
            }
            rows.push((key, value));
        }
        Ok((rows, next))
    }

    /// One page of the rows under a prefix, resumable by an opaque mark.
    ///
    /// Same promise as `sweep`, narrowed to a prefix. A backend whose keys have
    /// no order serves this only where the prefix selects a whole shard of its
    /// own, so a caller cannot turn a prefix walk into a family scan by
    /// accident.
    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<Vec<u8>>)> {
        // Inclusive, as in `sweep`: the mark is the first key not returned.
        let start = from.unwrap_or(prefix);
        let mut rows = Vec::with_capacity(limit);
        let mut next = None;
        for (key, value) in self.iter_from(cf, start, Direction::Asc)? {
            if !key.starts_with(prefix) {
                break;
            }
            if rows.len() == limit {
                next = Some(key);
                break;
            }
            rows.push((key, value));
        }
        Ok((rows, next))
    }

    /// One page of the keys under a prefix, resumable by an opaque mark.
    ///
    /// Same promise as `sweep_prefix` for a caller that wants the keys and none of
    /// the values. The default takes the values and drops them. A backend that
    /// can page keys on their own overrides it.
    fn sweep_keys_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        let (rows, next) = self.sweep_prefix(cf, prefix, from, limit)?;
        Ok((rows.into_iter().map(|(key, _)| key).collect(), next))
    }

    /// Exact count of the keys under `prefix`, without materializing them.
    ///
    /// The default collects the keys and takes the length. Backends override to
    /// count in place.
    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        Ok(self.iter_keys_prefix(cf, prefix)?.len() as u64)
    }

    /// Stored value bytes under `prefix`, without reading any of them.
    ///
    /// Stored bytes rather than what the caller put in: whatever the backend
    /// holds under those keys, however it accounts for its own framing. Nothing
    /// comes back from a backend that could only answer by reading the payloads.
    /// Has no default, so a delegating store cannot inherit a no-answer
    /// silently.
    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Option<u64>>;

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

/// The window of a value a ranged read asks for, clamped rather than refused
pub fn range_of(value: Value, offset: u64, len: usize) -> Value {
    reel_core::store::range_of(value, offset, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    // a window inside the value comes back whole
    #[test]
    fn window_inside() {
        assert_eq!(range_of(Value::new(b"abcdefgh".to_vec()), 2, 3).as_ref() as &[u8], b"cde");
        assert_eq!(range_of(Value::new(b"abcdefgh".to_vec()), 0, 8).as_ref() as &[u8], b"abcdefgh");
    }

    // a window running past the end stops at the end
    #[test]
    fn window_over() {
        assert_eq!(range_of(Value::new(b"abcd".to_vec()), 2, 99).as_ref() as &[u8], b"cd");
        assert_eq!(range_of(Value::new(b"abcd".to_vec()), 0, usize::MAX).as_ref() as &[u8], b"abcd");
    }

    // an offset at or past the end answers no bytes
    #[test]
    fn window_beyond() {
        assert_eq!(range_of(Value::new(b"abcd".to_vec()), 4, 2).as_ref() as &[u8], b"");
        assert_eq!(range_of(Value::new(b"abcd".to_vec()), u64::MAX, 2).as_ref() as &[u8], b"");
        assert_eq!(range_of(Value::new(Vec::new()), 0, 2).as_ref() as &[u8], b"");
    }
}
