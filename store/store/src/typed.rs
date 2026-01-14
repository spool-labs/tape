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
/// ```ignore
/// use store::{Column, TypedStore};
/// use store_memory::MemoryStore;
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
