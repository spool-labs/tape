//! Typed wrapper around the Store trait for type-safe operations

use crate::{Column, Error, Result, Store};

/// A typed wrapper around a Store implementation
///
/// TypedStore provides type-safe operations by using the Column trait
/// to define the key and value types for each column family.
/// Serialization is handled via wincode.
///
/// # Example
///
/// ```
/// use store::{Column, TypedStore, MemoryStore};
///
/// // Define a column with primitive types
/// struct Users;
/// impl Column for Users {
///     const CF_NAME: &'static str = "users";
///     type Key = u64;
///     type Value = String;
/// }
///
/// let store = TypedStore::new(MemoryStore::new());
/// store.put::<Users>(&1, &"Alice:30".to_string()).unwrap();
/// let user = store.get::<Users>(&1).unwrap();
/// assert_eq!(user, Some("Alice:30".to_string()));
/// ```
pub struct TypedStore<S: Store> {
    inner: S,
}

impl<S: Store> TypedStore<S> {
    /// Create a new TypedStore wrapping the given Store implementation
    pub fn new(store: S) -> Self {
        Self { inner: store }
    }

    /// Get the underlying store
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a value by key from the column.
    pub fn get<C: Column>(&self, key: &C::Key) -> Result<Option<C::Value>> {
        let key_bytes = wincode::serialize(key)
            .map_err(|e| Error::Serialization(format!("failed to serialize key: {}", e)))?;

        match self.inner.get(C::CF_NAME, &key_bytes)? {
            Some(value_bytes) => {
                let value = wincode::deserialize(&value_bytes)
                    .map_err(|e| Error::Serialization(format!("failed to deserialize value: {}", e)))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Put a key-value pair into the column.
    pub fn put<C: Column>(&self, key: &C::Key, value: &C::Value) -> Result<()> {
        let key_bytes = wincode::serialize(key)
            .map_err(|e| Error::Serialization(format!("failed to serialize key: {}", e)))?;
        let value_bytes = wincode::serialize(value)
            .map_err(|e| Error::Serialization(format!("failed to serialize value: {}", e)))?;

        self.inner.put(C::CF_NAME, &key_bytes, &value_bytes)
    }

    /// Delete a key from the column.
    pub fn delete<C: Column>(&self, key: &C::Key) -> Result<()> {
        let key_bytes = wincode::serialize(key)
            .map_err(|e| Error::Serialization(format!("failed to serialize key: {}", e)))?;

        self.inner.delete(C::CF_NAME, &key_bytes)
    }

    /// Check if a key exists in the column.
    pub fn contains<C: Column>(&self, key: &C::Key) -> Result<bool> {
        let key_bytes = wincode::serialize(key)
            .map_err(|e| Error::Serialization(format!("failed to serialize key: {}", e)))?;

        self.inner.contains(C::CF_NAME, &key_bytes)
    }

    /// Iterate over all entries, returning (key, value) pairs in lexicographic order.
    pub fn iter<C: Column>(&self) -> Result<Vec<(C::Key, C::Value)>> {
        let iter = self.inner.iter(C::CF_NAME)?;
        let mut results = Vec::new();

        for (key_bytes, value_bytes) in iter {
            let key = wincode::deserialize(&key_bytes)
                .map_err(|e| Error::Serialization(format!("failed to deserialize key: {}", e)))?;
            let value = wincode::deserialize(&value_bytes)
                .map_err(|e| Error::Serialization(format!("failed to deserialize value: {}", e)))?;
            results.push((key, value));
        }

        Ok(results)
    }

    /// Iterate over entries matching the key prefix in lexicographic order.
    pub fn iter_prefix<C: Column>(&self, prefix: &C::Key) -> Result<Vec<(C::Key, C::Value)>> {
        let prefix_bytes = wincode::serialize(prefix)
            .map_err(|e| Error::Serialization(format!("failed to serialize prefix: {}", e)))?;

        let iter = self.inner.iter_prefix(C::CF_NAME, &prefix_bytes)?;
        let mut results = Vec::new();

        for (key_bytes, value_bytes) in iter {
            let key = wincode::deserialize(&key_bytes)
                .map_err(|e| Error::Serialization(format!("failed to deserialize key: {}", e)))?;
            let value = wincode::deserialize(&value_bytes)
                .map_err(|e| Error::Serialization(format!("failed to deserialize value: {}", e)))?;
            results.push((key, value));
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    // Use primitive types that wincode already supports natively
    // For production code with structs, implement SchemaRead/SchemaWrite manually
    // or use the derive macros if available in the wincode version

    struct Users;
    impl Column for Users {
        const CF_NAME: &'static str = "users";
        type Key = u64;
        type Value = String; // Use String instead of custom struct for testing
    }

    struct Posts;
    impl Column for Posts {
        const CF_NAME: &'static str = "posts";
        type Key = String;
        type Value = String;
    }

    struct Scores;
    impl Column for Scores {
        const CF_NAME: &'static str = "scores";
        type Key = u64;
        type Value = u64;
    }

    #[test]
    fn put_get() {
        let store = TypedStore::new(MemoryStore::new());

        store.put::<Users>(&1, &"Alice:30".to_string()).unwrap();

        let retrieved = store.get::<Users>(&1).unwrap();
        assert_eq!(retrieved, Some("Alice:30".to_string()));
    }

    #[test]
    fn get_nonexistent() {
        let store = TypedStore::new(MemoryStore::new());

        let result = store.get::<Users>(&999).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn delete() {
        let store = TypedStore::new(MemoryStore::new());

        store.put::<Users>(&1, &"Bob:25".to_string()).unwrap();
        assert!(store.contains::<Users>(&1).unwrap());

        store.delete::<Users>(&1).unwrap();
        assert!(!store.contains::<Users>(&1).unwrap());
        assert_eq!(store.get::<Users>(&1).unwrap(), None);
    }

    #[test]
    fn contains() {
        let store = TypedStore::new(MemoryStore::new());

        assert!(!store.contains::<Users>(&1).unwrap());

        store.put::<Users>(&1, &"Charlie:35".to_string()).unwrap();

        assert!(store.contains::<Users>(&1).unwrap());
    }

    #[test]
    fn multi_columns() {
        let store = TypedStore::new(MemoryStore::new());

        // Use different column families
        store.put::<Users>(&1, &"Dave:40".to_string()).unwrap();
        store.put::<Posts>(&"post1".to_string(), &"Hello World".to_string()).unwrap();

        // Verify both exist in separate namespaces
        assert_eq!(store.get::<Users>(&1).unwrap(), Some("Dave:40".to_string()));
        assert_eq!(
            store.get::<Posts>(&"post1".to_string()).unwrap(),
            Some("Hello World".to_string())
        );
    }

    #[test]
    fn iter() {
        let store = TypedStore::new(MemoryStore::new());

        // Insert multiple users
        let users = vec![
            (1u64, "Alice:30".to_string()),
            (2u64, "Bob:25".to_string()),
            (3u64, "Charlie:35".to_string()),
        ];

        for (id, user) in &users {
            store.put::<Users>(id, user).unwrap();
        }

        // Iterate and collect
        let results = store.iter::<Users>().unwrap();

        assert_eq!(results.len(), 3);
        // Results should be sorted by key
        for (i, (key, value)) in results.iter().enumerate() {
            assert_eq!(*key, users[i].0);
            assert_eq!(*value, users[i].1);
        }
    }

    #[test]
    fn iter_empty() {
        let store = TypedStore::new(MemoryStore::new());

        let results = store.iter::<Users>().unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn overwrite() {
        let store = TypedStore::new(MemoryStore::new());

        store.put::<Users>(&1, &"Original:20".to_string()).unwrap();
        store.put::<Users>(&1, &"Updated:21".to_string()).unwrap();

        let result = store.get::<Users>(&1).unwrap();
        assert_eq!(result, Some("Updated:21".to_string()));
    }

    #[test]
    fn iter_prefix() {
        let store = TypedStore::new(MemoryStore::new());

        // Insert users with sequential IDs
        for i in 1..=5u64 {
            store.put::<Users>(&i, &format!("User{}:{}", i, 20 + i)).unwrap();
        }

        // Note: prefix scanning with u64 keys works because wincode serialization
        // preserves lexicographic ordering for fixed-size integers
        let results = store.iter_prefix::<Users>(&1).unwrap();

        // Should get user 1
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 1);
    }

    #[test]
    fn numeric_values() {
        let store = TypedStore::new(MemoryStore::new());

        store.put::<Scores>(&1, &100).unwrap();
        store.put::<Scores>(&2, &200).unwrap();

        assert_eq!(store.get::<Scores>(&1).unwrap(), Some(100));
        assert_eq!(store.get::<Scores>(&2).unwrap(), Some(200));
    }
}
