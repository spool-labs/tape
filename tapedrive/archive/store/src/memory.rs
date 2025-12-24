//! In-memory implementation of the Store trait for testing

use std::collections::HashMap;
use std::sync::RwLock;

use crate::store::{Direction, StoreIter};
use crate::{batch::BatchOp, Result, Store, WriteBatch};

#[cfg(feature = "metrics")]
use crate::metrics::{get_metrics, OperationTimer};

/// In-memory key-value store using HashMap
///
/// This implementation is thread-safe using RwLock and supports
/// dynamic column family creation on first write.
///
/// Useful for testing and development where persistence is not required.
pub struct MemoryStore {
    // Map: column_family -> (key -> value)
    data: RwLock<HashMap<String, HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStore {
    /// Create a new empty in-memory store
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for MemoryStore {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let result = Ok(data
            .get(cf)
            .and_then(|cf_data| cf_data.get(key))
            .map(|v| v.clone()));

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let found = result.as_ref().map(|opt| opt.is_some()).unwrap_or(false);
            let status = if result.is_ok() { "success" } else { "error" };

            metrics
                .get_duration
                .with_label_values(&[cf, &found.to_string()])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "get", status])
                .inc();

            metrics
                .key_bytes
                .with_label_values(&[cf, "get"])
                .observe(key.len() as f64);

            if let Ok(Some(ref value)) = result {
                metrics
                    .value_bytes
                    .with_label_values(&[cf, "get"])
                    .observe(value.len() as f64);
                metrics
                    .bytes_read_total
                    .with_label_values(&[cf])
                    .inc_by(value.len() as u64);
            }
        }

        result
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let mut data = self.data.write().unwrap();
        data.entry(cf.to_string())
            .or_insert_with(HashMap::new)
            .insert(key.to_vec(), value.to_vec());
        let result = Ok(());

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let status = if result.is_ok() { "success" } else { "error" };

            metrics
                .put_duration
                .with_label_values(&[cf])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "put", status])
                .inc();

            metrics
                .key_bytes
                .with_label_values(&[cf, "put"])
                .observe(key.len() as f64);

            metrics
                .value_bytes
                .with_label_values(&[cf, "put"])
                .observe(value.len() as f64);

            metrics
                .bytes_written_total
                .with_label_values(&[cf])
                .inc_by((key.len() + value.len()) as u64);
        }

        result
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let mut data = self.data.write().unwrap();
        if let Some(cf_data) = data.get_mut(cf) {
            cf_data.remove(key);
        }
        let result = Ok(());

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let status = if result.is_ok() { "success" } else { "error" };

            metrics
                .delete_duration
                .with_label_values(&[cf])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "delete", status])
                .inc();

            metrics
                .key_bytes
                .with_label_values(&[cf, "delete"])
                .observe(key.len() as f64);
        }

        result
    }

    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let result = Ok(data
            .get(cf)
            .map(|cf_data| cf_data.contains_key(key))
            .unwrap_or(false));

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let found = result.as_ref().map(|b| *b).unwrap_or(false);
            let status = if result.is_ok() { "success" } else { "error" };

            metrics
                .contains_duration
                .with_label_values(&[cf, &found.to_string()])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "contains", status])
                .inc();

            metrics
                .key_bytes
                .with_label_values(&[cf, "contains"])
                .observe(key.len() as f64);
        }

        result
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        #[cfg(feature = "metrics")]
        let batch_len = batch.len();

        // Apply all operations atomically under a single write lock
        let mut data = self.data.write().unwrap();

        #[cfg(feature = "metrics")]
        let mut bytes_written = 0u64;
        #[cfg(feature = "metrics")]
        let mut cf_name = String::new();

        for op in batch.iter() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    #[cfg(feature = "metrics")]
                    {
                        cf_name = cf.clone();
                        bytes_written += (key.len() + value.len()) as u64;
                    }

                    data.entry(cf.clone())
                        .or_insert_with(HashMap::new)
                        .insert(key.clone(), value.clone());
                }
                BatchOp::Delete { cf, key } => {
                    #[cfg(feature = "metrics")]
                    {
                        cf_name = cf.clone();
                    }

                    if let Some(cf_data) = data.get_mut(cf) {
                        cf_data.remove(key);
                    }
                }
            }
        }

        let result = Ok(());

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let status = if result.is_ok() { "success" } else { "error" };
            let cf = if cf_name.is_empty() {
                "default"
            } else {
                &cf_name
            };

            metrics
                .batch_duration
                .with_label_values(&[cf])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "write_batch", status])
                .inc();

            metrics
                .batch_items
                .with_label_values(&[cf])
                .observe(batch_len as f64);

            if bytes_written > 0 {
                metrics
                    .bytes_written_total
                    .with_label_values(&[cf])
                    .inc_by(bytes_written);
            }
        }

        result
    }

    fn iter(&self, cf: &str) -> Result<StoreIter<'_>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let result = Ok(Box::new(entries.into_iter()) as StoreIter<'_>);

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            metrics
                .iter_duration
                .with_label_values(&[cf, "full"])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "iter", "success"])
                .inc();
        }

        result
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let prefix = prefix.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| k.starts_with(&prefix))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let result = Ok(Box::new(entries.into_iter()) as StoreIter<'_>);

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            metrics
                .iter_duration
                .with_label_values(&[cf, "prefix"])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "iter_prefix", "success"])
                .inc();
        }

        result
    }

    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let start = start.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| match direction {
                        Direction::Asc => k.as_slice() >= start.as_slice(),
                        Direction::Desc => k.as_slice() <= start.as_slice(),
                    })
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        match direction {
            Direction::Asc => entries.sort_by(|a, b| a.0.cmp(&b.0)),
            Direction::Desc => entries.sort_by(|a, b| b.0.cmp(&a.0)),
        }
        let result = Ok(Box::new(entries.into_iter()) as StoreIter<'_>);

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            metrics
                .iter_duration
                .with_label_values(&[cf, "from"])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "iter_from", "success"])
                .inc();
        }

        result
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let data = self.data.read().unwrap();
        let start = start.to_vec();
        let end = end.to_vec();
        let mut entries: Vec<_> = data
            .get(cf)
            .map(|cf_data| {
                cf_data
                    .iter()
                    .filter(|(k, _)| k.as_slice() >= start.as_slice() && k.as_slice() < end.as_slice())
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let result = Ok(Box::new(entries.into_iter()) as StoreIter<'_>);

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            metrics
                .iter_duration
                .with_label_values(&[cf, "range"])
                .observe(timer.elapsed_secs());

            metrics
                .operations_total
                .with_label_values(&[cf, "iter_range", "success"])
                .inc();
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_empty() {
        let store = MemoryStore::new();
        assert!(store.get("test", b"key").unwrap().is_none());
        assert!(!store.contains("test", b"key").unwrap());
    }

    #[test]
    fn put_get() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value").unwrap();

        let result = store.get("test", b"key").unwrap();
        assert_eq!(result, Some(b"value".to_vec()));
    }

    #[test]
    fn put_overwrites() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value1").unwrap();
        store.put("test", b"key", b"value2").unwrap();

        let result = store.get("test", b"key").unwrap();
        assert_eq!(result, Some(b"value2".to_vec()));
    }

    #[test]
    fn delete() {
        let store = MemoryStore::new();

        store.put("test", b"key", b"value").unwrap();
        assert!(store.contains("test", b"key").unwrap());

        store.delete("test", b"key").unwrap();
        assert!(!store.contains("test", b"key").unwrap());
        assert_eq!(store.get("test", b"key").unwrap(), None);
    }

    #[test]
    fn delete_nonexistent() {
        let store = MemoryStore::new();

        // Deleting non-existent key should not error
        store.delete("test", b"nonexistent").unwrap();
    }

    #[test]
    fn multi_cf() {
        let store = MemoryStore::new();

        store.put("cf1", b"key", b"value1").unwrap();
        store.put("cf2", b"key", b"value2").unwrap();
        store.put("cf3", b"key", b"value3").unwrap();

        assert_eq!(store.get("cf1", b"key").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("cf2", b"key").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get("cf3", b"key").unwrap(), Some(b"value3".to_vec()));

        // Delete from one CF doesn't affect others
        store.delete("cf2", b"key").unwrap();
        assert_eq!(store.get("cf1", b"key").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("cf2", b"key").unwrap(), None);
        assert_eq!(store.get("cf3", b"key").unwrap(), Some(b"value3".to_vec()));
    }

    #[test]
    fn binary_data() {
        let store = MemoryStore::new();

        let key = vec![0u8, 1, 2, 255, 254];
        let value = vec![10u8, 20, 30, 200, 100];

        store.put("test", &key, &value).unwrap();
        assert_eq!(store.get("test", &key).unwrap(), Some(value));
    }

    #[test]
    fn batch_atomic() {
        let store = MemoryStore::new();

        // Pre-populate some data
        store.put("test", b"key1", b"old1").unwrap();
        store.put("test", b"key2", b"old2").unwrap();

        // Create batch
        let mut batch = WriteBatch::new();
        batch.put("test", b"key1", b"new1");
        batch.put("test", b"key3", b"new3");
        batch.delete("test", b"key2");

        store.write_batch(batch).unwrap();

        // Verify all operations applied
        assert_eq!(store.get("test", b"key1").unwrap(), Some(b"new1".to_vec()));
        assert_eq!(store.get("test", b"key2").unwrap(), None);
        assert_eq!(store.get("test", b"key3").unwrap(), Some(b"new3".to_vec()));
    }

    #[test]
    fn concurrent() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(MemoryStore::new());
        let mut handles = vec![];

        // Spawn multiple threads writing to different keys
        for i in 0..10 {
            let store_clone = Arc::clone(&store);
            let handle = thread::spawn(move || {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                store_clone.put("test", key.as_bytes(), value.as_bytes()).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all writes succeeded
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            assert_eq!(
                store.get("test", key.as_bytes()).unwrap(),
                Some(expected_value.as_bytes().to_vec())
            );
        }
    }
}
