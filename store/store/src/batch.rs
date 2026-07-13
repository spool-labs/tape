//! Write batch operations for atomic writes across column families

/// A batch of write operations (Put/Delete) to be applied atomically
#[derive(Debug, Clone, Default)]
pub struct WriteBatch {
    ops: Vec<BatchOp>,
}

#[derive(Debug, Clone)]
pub enum BatchOp {
    Put {
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: String,
        key: Vec<u8>,
    },
}

impl BatchOp {
    /// Column family this operation targets
    pub fn cf(&self) -> &str {
        match self {
            BatchOp::Put { cf, .. } => cf,
            BatchOp::Delete { cf, .. } => cf,
        }
    }
}

impl WriteBatch {
    /// Create a new empty write batch
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Add a Put operation to the batch
    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        self.put_owned(cf, key.to_vec(), value.to_vec());
    }

    /// Add a Put operation, taking ownership of key and value
    ///
    /// Callers holding freshly serialized bytes should prefer this: staging a
    /// large slice payload through the borrowing form copies the whole value.
    pub fn put_owned(&mut self, cf: &str, key: Vec<u8>, value: Vec<u8>) {
        self.ops.push(BatchOp::Put {
            cf: cf.to_string(),
            key,
            value,
        });
    }

    /// Add a Delete operation to the batch
    pub fn delete(&mut self, cf: &str, key: &[u8]) {
        self.delete_owned(cf, key.to_vec());
    }

    /// Add a Delete operation, taking ownership of the key
    pub fn delete_owned(&mut self, cf: &str, key: Vec<u8>) {
        self.ops.push(BatchOp::Delete { cf: cf.to_string(), key });
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Get the number of operations in the batch
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Get an iterator over the operations
    pub fn iter(&self) -> impl Iterator<Item = &BatchOp> {
        self.ops.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_ops() {
        let mut batch = WriteBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.put("cf1", b"key1", b"value1");
        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        batch.delete("cf2", b"key2");
        assert_eq!(batch.len(), 2);

        batch.put("cf1", b"key3", b"value3");
        assert_eq!(batch.len(), 3);
    }
}
