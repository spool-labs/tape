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

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    #[test]
    fn basic_ops() {
        let store = MemoryStore::new();

        // Test put and get
        store.put("test", b"key1", b"value1").unwrap();
        let result = store.get("test", b"key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Test non-existent key
        let result = store.get("test", b"nonexistent").unwrap();
        assert_eq!(result, None);

        // Test contains
        assert!(store.contains("test", b"key1").unwrap());
        assert!(!store.contains("test", b"nonexistent").unwrap());

        // Test delete
        store.delete("test", b"key1").unwrap();
        assert!(!store.contains("test", b"key1").unwrap());
        let result = store.get("test", b"key1").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn column_families() {
        let store = MemoryStore::new();

        // Different column families have separate key spaces
        store.put("cf1", b"key", b"value1").unwrap();
        store.put("cf2", b"key", b"value2").unwrap();

        let result1 = store.get("cf1", b"key").unwrap();
        let result2 = store.get("cf2", b"key").unwrap();

        assert_eq!(result1, Some(b"value1".to_vec()));
        assert_eq!(result2, Some(b"value2".to_vec()));
    }

    #[test]
    fn write_batch() {
        let store = MemoryStore::new();

        // Prepare batch
        let mut batch = WriteBatch::new();
        batch.put("test", b"key1", b"value1");
        batch.put("test", b"key2", b"value2");
        batch.delete("test", b"key3");

        // Pre-populate key3
        store.put("test", b"key3", b"old_value").unwrap();
        assert!(store.contains("test", b"key3").unwrap());

        // Apply batch
        store.write_batch(batch).unwrap();

        // Verify results
        assert_eq!(store.get("test", b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("test", b"key2").unwrap(), Some(b"value2".to_vec()));
        assert!(!store.contains("test", b"key3").unwrap());
    }

    #[test]
    fn batch_multi_cf() {
        let store = MemoryStore::new();

        let mut batch = WriteBatch::new();
        batch.put("cf1", b"key", b"value1");
        batch.put("cf2", b"key", b"value2");
        batch.delete("cf3", b"key");

        store.write_batch(batch).unwrap();

        assert_eq!(store.get("cf1", b"key").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("cf2", b"key").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get("cf3", b"key").unwrap(), None);
    }

    #[test]
    fn empty_batch() {
        let store = MemoryStore::new();
        let batch = WriteBatch::new();

        // Empty batch should succeed without error
        store.write_batch(batch).unwrap();
    }

    #[test]
    fn iter() {
        let store = MemoryStore::new();

        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();

        let entries: Vec<_> = store.iter("test").unwrap().collect();
        assert_eq!(entries.len(), 3);
        // Should be sorted
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn iter_prefix() {
        let store = MemoryStore::new();

        store.put("test", b"user:1", b"alice").unwrap();
        store.put("test", b"user:2", b"bob").unwrap();
        store.put("test", b"post:1", b"hello").unwrap();
        store.put("test", b"user:3", b"charlie").unwrap();

        let users: Vec<_> = store.iter_prefix("test", b"user:").unwrap().collect();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].1, b"alice".to_vec());
        assert_eq!(users[1].1, b"bob".to_vec());
        assert_eq!(users[2].1, b"charlie".to_vec());

        let posts: Vec<_> = store.iter_prefix("test", b"post:").unwrap().collect();
        assert_eq!(posts.len(), 1);
    }

    #[test]
    fn iter_from() {
        let store = MemoryStore::new();

        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();
        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"d", b"4").unwrap();

        // Ascending from "b"
        let asc: Vec<_> = store.iter_from("test", b"b", Direction::Asc).unwrap().collect();
        assert_eq!(asc.len(), 3);
        assert_eq!(asc[0].0, b"b".to_vec());
        assert_eq!(asc[1].0, b"c".to_vec());
        assert_eq!(asc[2].0, b"d".to_vec());

        // Descending from "c"
        let desc: Vec<_> = store.iter_from("test", b"c", Direction::Desc).unwrap().collect();
        assert_eq!(desc.len(), 3);
        assert_eq!(desc[0].0, b"c".to_vec());
        assert_eq!(desc[1].0, b"b".to_vec());
        assert_eq!(desc[2].0, b"a".to_vec());
    }

    #[test]
    fn iter_range() {
        let store = MemoryStore::new();

        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();
        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"d", b"4").unwrap();

        // Range [b, d) should return b and c
        let range: Vec<_> = store.iter_range("test", b"b", b"d").unwrap().collect();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, b"b".to_vec());
        assert_eq!(range[1].0, b"c".to_vec());
    }
}
