use store::{Store, WriteBatch};
use store_memory::MemoryStore;

#[test]
fn basic_crud() {
    let store = MemoryStore::new();

    // Test put
    store.put("users", b"alice", b"admin").unwrap();
    store.put("users", b"bob", b"user").unwrap();

    // Test get
    assert_eq!(store.get("users", b"alice").unwrap(), Some(b"admin".to_vec()));
    assert_eq!(store.get("users", b"bob").unwrap(), Some(b"user".to_vec()));
    assert_eq!(store.get("users", b"charlie").unwrap(), None);

    // Test contains
    assert!(store.contains("users", b"alice").unwrap());
    assert!(store.contains("users", b"bob").unwrap());
    assert!(!store.contains("users", b"charlie").unwrap());

    // Test delete
    store.delete("users", b"alice").unwrap();
    assert!(!store.contains("users", b"alice").unwrap());
    assert_eq!(store.get("users", b"alice").unwrap(), None);

    // Bob should still exist
    assert!(store.contains("users", b"bob").unwrap());
}

#[test]
fn column_families() {
    let store = MemoryStore::new();

    // Write to different column families
    store.put("users", b"key1", b"user_value").unwrap();
    store.put("posts", b"key1", b"post_value").unwrap();
    store.put("comments", b"key1", b"comment_value").unwrap();

    // Each CF should have independent key spaces
    assert_eq!(store.get("users", b"key1").unwrap(), Some(b"user_value".to_vec()));
    assert_eq!(store.get("posts", b"key1").unwrap(), Some(b"post_value".to_vec()));
    assert_eq!(store.get("comments", b"key1").unwrap(), Some(b"comment_value".to_vec()));

    // Delete from one CF shouldn't affect others
    store.delete("posts", b"key1").unwrap();
    assert_eq!(store.get("posts", b"key1").unwrap(), None);
    assert_eq!(store.get("users", b"key1").unwrap(), Some(b"user_value".to_vec()));
    assert_eq!(store.get("comments", b"key1").unwrap(), Some(b"comment_value".to_vec()));
}

#[test]
fn write_batch() {
    let store = MemoryStore::new();

    // Pre-populate with some data
    store.put("test", b"existing", b"old_value").unwrap();

    // Create batch with multiple operations
    let mut batch = WriteBatch::new();
    batch.put("test", b"key1", b"value1");
    batch.put("test", b"key2", b"value2");
    batch.put("test", b"key3", b"value3");
    batch.delete("test", b"existing");

    // Apply batch
    store.write_batch(batch).unwrap();

    // Verify all operations were applied
    assert_eq!(store.get("test", b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(store.get("test", b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(store.get("test", b"key3").unwrap(), Some(b"value3".to_vec()));
    assert_eq!(store.get("test", b"existing").unwrap(), None);
}

#[test]
fn batch_multi_cf() {
    let store = MemoryStore::new();

    let mut batch = WriteBatch::new();
    batch.put("cf1", b"key", b"value1");
    batch.put("cf2", b"key", b"value2");
    batch.put("cf3", b"key", b"value3");
    batch.delete("cf4", b"key");

    store.write_batch(batch).unwrap();

    assert_eq!(store.get("cf1", b"key").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(store.get("cf2", b"key").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(store.get("cf3", b"key").unwrap(), Some(b"value3".to_vec()));
    assert_eq!(store.get("cf4", b"key").unwrap(), None);
}

#[test]
fn overwrite() {
    let store = MemoryStore::new();

    store.put("test", b"key", b"value1").unwrap();
    assert_eq!(store.get("test", b"key").unwrap(), Some(b"value1".to_vec()));

    store.put("test", b"key", b"value2").unwrap();
    assert_eq!(store.get("test", b"key").unwrap(), Some(b"value2".to_vec()));

    store.put("test", b"key", b"value3").unwrap();
    assert_eq!(store.get("test", b"key").unwrap(), Some(b"value3".to_vec()));
}

#[test]
fn empty_data() {
    let store = MemoryStore::new();

    // Empty key
    store.put("test", b"", b"value").unwrap();
    assert_eq!(store.get("test", b"").unwrap(), Some(b"value".to_vec()));

    // Empty value
    store.put("test", b"key", b"").unwrap();
    assert_eq!(store.get("test", b"key").unwrap(), Some(b"".to_vec()));

    // Both empty
    store.put("test2", b"", b"").unwrap();
    assert_eq!(store.get("test2", b"").unwrap(), Some(b"".to_vec()));
}

#[test]
fn binary_data() {
    let store = MemoryStore::new();

    let binary_key = vec![0u8, 1, 2, 3, 255, 254, 253];
    let binary_value = vec![100u8, 200, 150, 0, 1, 2, 255];

    store.put("binary", &binary_key, &binary_value).unwrap();
    assert_eq!(store.get("binary", &binary_key).unwrap(), Some(binary_value.clone()));
    assert!(store.contains("binary", &binary_key).unwrap());

    store.delete("binary", &binary_key).unwrap();
    assert!(!store.contains("binary", &binary_key).unwrap());
}

#[test]
fn batch_builder() {
    let store = MemoryStore::new();

    let mut batch = WriteBatch::new();
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);

    batch.put("cf1", b"key1", b"value1");
    assert!(!batch.is_empty());
    assert_eq!(batch.len(), 1);

    batch.put("cf1", b"key2", b"value2");
    batch.delete("cf2", b"key3");
    batch.put("cf3", b"key4", b"value4");
    assert_eq!(batch.len(), 4);

    store.write_batch(batch).unwrap();

    assert_eq!(store.get("cf1", b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(store.get("cf1", b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(store.get("cf2", b"key3").unwrap(), None);
    assert_eq!(store.get("cf3", b"key4").unwrap(), Some(b"value4".to_vec()));
}

#[test]
fn empty_batch() {
    let store = MemoryStore::new();

    let batch = WriteBatch::new();
    assert!(batch.is_empty());

    // Empty batch should succeed
    store.write_batch(batch).unwrap();
}

#[test]
fn delete_nonexistent() {
    let store = MemoryStore::new();

    // Deleting non-existent key should not error
    store.delete("test", b"nonexistent").unwrap();

    // Deleting from non-existent CF should not error
    store.delete("nonexistent_cf", b"key").unwrap();
}

#[test]
fn large_batch() {
    let store = MemoryStore::new();

    let mut batch = WriteBatch::new();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        batch.put("test", key.as_bytes(), value.as_bytes());
    }

    assert_eq!(batch.len(), 1000);
    store.write_batch(batch).unwrap();

    // Verify all keys were written
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        assert_eq!(
            store.get("test", key.as_bytes()).unwrap(),
            Some(expected_value.as_bytes().to_vec())
        );
    }
}

#[test]
fn batch_mixed() {
    let store = MemoryStore::new();

    // Initial data
    store.put("test", b"key1", b"initial1").unwrap();
    store.put("test", b"key2", b"initial2").unwrap();
    store.put("test", b"key3", b"initial3").unwrap();

    // Batch that updates some and deletes others
    let mut batch = WriteBatch::new();
    batch.put("test", b"key1", b"updated1"); // Update
    batch.delete("test", b"key2"); // Delete
    batch.put("test", b"key4", b"new4"); // New
    // key3 unchanged

    store.write_batch(batch).unwrap();

    assert_eq!(store.get("test", b"key1").unwrap(), Some(b"updated1".to_vec()));
    assert_eq!(store.get("test", b"key2").unwrap(), None);
    assert_eq!(store.get("test", b"key3").unwrap(), Some(b"initial3".to_vec()));
    assert_eq!(store.get("test", b"key4").unwrap(), Some(b"new4".to_vec()));
}
