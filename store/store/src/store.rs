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
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Iterate over all entries in lexicographic key order.
    fn iter(&self, cf: &str) -> Result<StoreIter<'_>>;

    /// Iterate over entries matching the key prefix in lexicographic order.
    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>>;

    /// Iterate from the start key (inclusive) in the specified direction.
    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>>;

    /// Iterate over entries in the key range [start, end) in lexicographic order.
    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>>;
}
