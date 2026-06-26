//! RocksDB implementation of the Store trait

use std::fs;
use std::path::{Path, PathBuf};

use fs2::available_space;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options,
    WriteBatch as RocksWriteBatch,
};

use store::{BatchOp, Direction, Error, Result, Store, StoreIter, WriteBatch};

#[cfg(feature = "metrics")]
use store::get_metrics;
#[cfg(feature = "metrics")]
use tape_metrics::OperationTimer;

/// RocksDB-based persistent key-value store
///
/// Uses multi-threaded column family access for concurrent operations.
pub struct RocksStore {
    db: DBWithThreadMode<MultiThreaded>,
    path: PathBuf,
    column_families: Vec<String>,
}

impl RocksStore {
    /// Open a RocksDB database at the specified path.
    pub fn open<P: AsRef<Path>>(path: P, column_families: &[&str]) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let column_families = column_families
            .iter()
            .map(|column_family| (*column_family).to_string())
            .collect::<Vec<_>>();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // If no column families specified, just open with default
        let db = if column_families.is_empty() {
            DBWithThreadMode::open(&db_opts, &path)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf(&db_opts, &path, column_families.clone())
                .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path,
            column_families,
        })
    }

    /// Open a RocksDB database with custom options.
    pub fn open_with_opts<P: AsRef<Path>>(
        path: P,
        db_opts: Options,
        column_families: &[&str],
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let column_families = column_families
            .iter()
            .map(|column_family| (*column_family).to_string())
            .collect::<Vec<_>>();
        let db = if column_families.is_empty() {
            DBWithThreadMode::open(&db_opts, &path)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf(&db_opts, &path, column_families.clone())
                .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path,
            column_families,
        })
    }

    /// Open a RocksDB database with custom per-column-family configuration.
    pub fn open_with_cf_config<P: AsRef<Path>>(
        path: P,
        db_opts: Options,
        cf_configs: Vec<ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let column_families = cf_configs
            .iter()
            .map(|config| config.name().to_string())
            .collect::<Vec<_>>();
        let db = if cf_configs.is_empty() {
            DBWithThreadMode::open(&db_opts, &path)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf_descriptors(&db_opts, &path, cf_configs)
                .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path,
            column_families,
        })
    }

    /// Create a new column family.
    pub fn create_cf(&self, name: &str) -> Result<()> {
        let opts = Options::default();
        self.db
            .create_cf(name, &opts)
            .map_err(|e| Error::Database(e.to_string()))
    }

    /// Drop a column family.
    pub fn drop_cf(&self, name: &str) -> Result<()> {
        self.db
            .drop_cf(name)
            .map_err(|e| Error::Database(e.to_string()))
    }

    /// Open database in read-only mode.
    pub fn open_read_only<P: AsRef<Path>>(
        path: P,
        column_families: &[&str],
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let column_families = column_families
            .iter()
            .map(|column_family| (*column_family).to_string())
            .collect::<Vec<_>>();
        let db_opts = Options::default();

        let db = if column_families.is_empty() {
            DBWithThreadMode::open_for_read_only(&db_opts, &path, false)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf_for_read_only(
                &db_opts,
                &path,
                column_families.clone(),
                false,
            )
                .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path,
            column_families,
        })
    }

    /// Open database in read-only mode with custom column family configuration.
    pub fn open_read_only_with_cf_config<P: AsRef<Path>>(
        path: P,
        db_opts: Options,
        cf_configs: Vec<ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let column_families = cf_configs
            .iter()
            .map(|config| config.name().to_string())
            .collect::<Vec<_>>();
        let db = if cf_configs.is_empty() {
            DBWithThreadMode::open_for_read_only(&db_opts, &path, false)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf_descriptors_read_only(&db_opts, &path, cf_configs, false)
                .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path,
            column_families,
        })
    }

    /// Open as secondary instance that can catch up with primary.
    pub fn open_secondary<P: AsRef<Path>>(
        primary_path: P,
        secondary_path: P,
        column_families: &[&str],
    ) -> Result<Self> {
        let primary_path = primary_path.as_ref().to_path_buf();
        let secondary_path = secondary_path.as_ref().to_path_buf();
        let column_families = column_families
            .iter()
            .map(|column_family| (*column_family).to_string())
            .collect::<Vec<_>>();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let db = if column_families.is_empty() {
            DBWithThreadMode::open_as_secondary(&db_opts, &primary_path, &secondary_path)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf_as_secondary(
                &db_opts,
                &primary_path,
                &secondary_path,
                column_families.clone(),
            )
            .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path: secondary_path,
            column_families,
        })
    }

    /// Open as secondary instance with custom column family configuration.
    pub fn open_secondary_with_cf_config<P: AsRef<Path>>(
        primary_path: P,
        secondary_path: P,
        db_opts: Options,
        cf_configs: Vec<ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let primary_path = primary_path.as_ref().to_path_buf();
        let secondary_path = secondary_path.as_ref().to_path_buf();
        let column_families = cf_configs
            .iter()
            .map(|config| config.name().to_string())
            .collect::<Vec<_>>();
        let db = if cf_configs.is_empty() {
            DBWithThreadMode::open_as_secondary(&db_opts, &primary_path, &secondary_path)
                .map_err(|e| Error::Database(e.to_string()))?
        } else {
            DBWithThreadMode::open_cf_descriptors_as_secondary(
                &db_opts,
                &primary_path,
                &secondary_path,
                cf_configs,
            )
            .map_err(|e| Error::Database(e.to_string()))?
        };

        Ok(Self {
            db,
            path: secondary_path,
            column_families,
        })
    }

    /// Sync secondary instance with primary database.
    pub fn catch_up_with_primary(&self) -> Result<()> {
        self.db
            .try_catch_up_with_primary()
            .map_err(|e| Error::Database(e.to_string()))
    }

    /// Get the underlying RocksDB instance for advanced operations
    pub fn inner(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.db
    }

    /// Flush all memtables to disk, across the default and every named
    /// column family.
    pub fn flush(&self) -> Result<()> {
        self.db
            .flush()
            .map_err(|e| Error::Database(e.to_string()))?;

        for column_family in &self.column_families {
            let Some(handle) = self.db.cf_handle(column_family) else {
                continue;
            };
            self.db
                .flush_cf(&handle)
                .map_err(|e| Error::Database(e.to_string()))?;
        }

        Ok(())
    }

    fn compact_column_family(&self, column_family: &str) {
        let Some(column_family_handle) = self.db.cf_handle(column_family) else {
            return;
        };

        self.db
            .compact_range_cf(&column_family_handle, None::<&[u8]>, None::<&[u8]>);
    }
}

impl Store for RocksStore {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = self
            .db
            .get_cf(&cf_handle, key)
            .map_err(|e| Error::Database(e.to_string()));

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

            if result.is_err() {
                metrics
                    .errors_total
                    .with_label_values(&[cf, "get", "database"])
                    .inc();
            }
        }

        result
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = self
            .db
            .put_cf(&cf_handle, key, value)
            .map_err(|e| Error::Database(e.to_string()));

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

            if result.is_err() {
                metrics
                    .errors_total
                    .with_label_values(&[cf, "put", "database"])
                    .inc();
            }
        }

        result
    }

    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = self
            .db
            .delete_range_cf(&cf_handle, start, end)
            .map_err(|e| Error::Database(e.to_string()));

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            let status = if result.is_ok() { "success" } else { "error" };
            metrics
                .delete_duration
                .with_label_values(&[cf])
                .observe(timer.elapsed_secs());
            metrics
                .operations_total
                .with_label_values(&[cf, "delete_range", status])
                .inc();
        }

        result
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = self
            .db
            .delete_cf(&cf_handle, key)
            .map_err(|e| Error::Database(e.to_string()));

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

            if result.is_err() {
                metrics
                    .errors_total
                    .with_label_values(&[cf, "delete", "database"])
                    .inc();
            }
        }

        result
    }

    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = self
            .db
            .get_pinned_cf(&cf_handle, key)
            .map(|opt| opt.is_some())
            .map_err(|e| Error::Database(e.to_string()));

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

            if result.is_err() {
                metrics
                    .errors_total
                    .with_label_values(&[cf, "contains", "database"])
                    .inc();
            }
        }

        result
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        #[cfg(feature = "metrics")]
        let batch_len = batch.len();

        let mut rocks_batch = RocksWriteBatch::default();

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

                    let cf_handle = self
                        .db
                        .cf_handle(cf)
                        .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;
                    rocks_batch.put_cf(&cf_handle, key, value);
                }
                BatchOp::Delete { cf, key } => {
                    #[cfg(feature = "metrics")]
                    {
                        cf_name = cf.clone();
                    }

                    let cf_handle = self
                        .db
                        .cf_handle(cf)
                        .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;
                    rocks_batch.delete_cf(&cf_handle, key);
                }
            }
        }

        let result = self
            .db
            .write(rocks_batch)
            .map_err(|e| Error::Database(e.to_string()));

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

            if result.is_err() {
                metrics
                    .errors_total
                    .with_label_values(&[cf, "write_batch", "database"])
                    .inc();
            }
        }

        result
    }

    fn iter(&self, cf: &str) -> Result<StoreIter<'_>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let iter = self
            .db
            .iterator_cf(&cf_handle, IteratorMode::Start)
            .map(|item| {
                let (k, v) = item.expect("iterator error");
                (k.to_vec(), v.to_vec())
            });

        let result = Ok(Box::new(iter) as StoreIter<'_>);

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

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let prefix = prefix.to_vec();
        let iter = self
            .db
            .prefix_iterator_cf(&cf_handle, &prefix)
            .map(|item| {
                let (k, v) = item.expect("iterator error");
                (k.to_vec(), v.to_vec())
            })
            .take_while(move |(k, _)| k.starts_with(&prefix));

        let result = Ok(Box::new(iter) as StoreIter<'_>);

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

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let mode = match direction {
            Direction::Asc => IteratorMode::From(start, rocksdb::Direction::Forward),
            Direction::Desc => IteratorMode::From(start, rocksdb::Direction::Reverse),
        };

        let iter = self
            .db
            .iterator_cf(&cf_handle, mode)
            .map(|item| {
                let (k, v) = item.expect("iterator error");
                (k.to_vec(), v.to_vec())
            });

        let result = Ok(Box::new(iter) as StoreIter<'_>);

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

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let end = end.to_vec();
        let iter = self
            .db
            .iterator_cf(&cf_handle, IteratorMode::From(start, rocksdb::Direction::Forward))
            .map(|item| {
                let (k, v) = item.expect("iterator error");
                (k.to_vec(), v.to_vec())
            })
            .take_while(move |(k, _)| k.as_slice() < end.as_slice());

        let result = Ok(Box::new(iter) as StoreIter<'_>);

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

    fn actual_size_bytes(&self) -> Result<u64> {
        directory_size_bytes(&self.path)
    }

    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        available_space(&self.path)
            .map(Some)
            .map_err(Error::Io)
    }

    fn reclaim_space(&self) -> Result<()> {
        self.flush()?;
        self.compact_column_family("default");

        for column_family in &self.column_families {
            self.compact_column_family(column_family);
        }

        Ok(())
    }
}

fn directory_size_bytes(path: &Path) -> Result<u64> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(Error::Io(error)),
    };

    let mut total = 0u64;
    for entry in entries {
        let entry = entry.map_err(Error::Io)?;
        let file_type = entry.file_type().map_err(Error::Io)?;
        if file_type.is_dir() {
            total = total.saturating_add(directory_size_bytes(&entry.path())?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata().map_err(Error::Io)?.len());
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn open_basic_ops() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        // Put and get
        store.put("test", b"key1", b"value1").unwrap();
        let result = store.get("test", b"key1").unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Contains
        assert!(store.contains("test", b"key1").unwrap());
        assert!(!store.contains("test", b"nonexistent").unwrap());

        // Delete
        store.delete("test", b"key1").unwrap();
        assert!(!store.contains("test", b"key1").unwrap());
    }

    #[test]
    fn multi_cf() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["cf1", "cf2"]).unwrap();

        store.put("cf1", b"key", b"value1").unwrap();
        store.put("cf2", b"key", b"value2").unwrap();

        assert_eq!(store.get("cf1", b"key").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("cf2", b"key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn write_batch() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        store.put("test", b"to_delete", b"old").unwrap();

        let mut batch = WriteBatch::new();
        batch.put("test", b"key1", b"value1");
        batch.put("test", b"key2", b"value2");
        batch.delete("test", b"to_delete");

        store.write_batch(batch).unwrap();

        assert_eq!(store.get("test", b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get("test", b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.get("test", b"to_delete").unwrap(), None);
    }

    #[test]
    fn cf_not_found() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        let result = store.get("nonexistent", b"key");
        assert!(matches!(result, Err(Error::ColumnFamilyNotFound(_))));
    }

    #[test]
    fn create_cf_dynamic() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &[]).unwrap();

        // Create a new column family
        store.create_cf("dynamic").unwrap();

        // Now we can use it
        store.put("dynamic", b"key", b"value").unwrap();
        assert_eq!(store.get("dynamic", b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Write data
        {
            let store = RocksStore::open(&path, &["test"]).unwrap();
            store.put("test", b"key", b"value").unwrap();
            store.flush().unwrap();
        }

        // Reopen and verify
        {
            let store = RocksStore::open(&path, &["test"]).unwrap();
            assert_eq!(store.get("test", b"key").unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn binary_data() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        let key = vec![0u8, 1, 2, 255, 254];
        let value = vec![100u8, 200, 0, 1, 255];

        store.put("test", &key, &value).unwrap();
        assert_eq!(store.get("test", &key).unwrap(), Some(value));
    }

    #[test]
    fn iter() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();

        let entries: Vec<_> = store.iter("test").unwrap().collect();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], (b"a".to_vec(), b"1".to_vec()));
        assert_eq!(entries[1], (b"b".to_vec(), b"2".to_vec()));
        assert_eq!(entries[2], (b"c".to_vec(), b"3".to_vec()));
    }

    #[test]
    fn iter_prefix() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        store.put("test", b"user:1", b"alice").unwrap();
        store.put("test", b"user:2", b"bob").unwrap();
        store.put("test", b"post:1", b"hello").unwrap();
        store.put("test", b"user:3", b"charlie").unwrap();

        let users: Vec<_> = store.iter_prefix("test", b"user:").unwrap().collect();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].1, b"alice".to_vec());
        assert_eq!(users[1].1, b"bob".to_vec());
        assert_eq!(users[2].1, b"charlie".to_vec());
    }

    #[test]
    fn iter_from() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

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
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

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

    #[test]
    fn delete_range() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        store.put("test", b"a", b"1").unwrap();
        store.put("test", b"b", b"2").unwrap();
        store.put("test", b"c", b"3").unwrap();
        store.put("test", b"d", b"4").unwrap();

        // [b, d) drops b and c; a and d (the exclusive end) survive.
        store.delete_range("test", b"b", b"d").unwrap();

        assert!(store.get("test", b"a").unwrap().is_some());
        assert!(store.get("test", b"b").unwrap().is_none());
        assert!(store.get("test", b"c").unwrap().is_none());
        assert!(store.get("test", b"d").unwrap().is_some());
    }

    #[test]
    fn cf_config() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cf_configs = vec![
            ColumnFamilyConfig::new("fixed").with_block_based().build(),
            ColumnFamilyConfig::new("block").with_block_based().build(),
        ];

        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        // Test operations on configured column families
        store.put("fixed", &1u64.to_be_bytes(), b"value1").unwrap();
        store.put("block", b"key2", b"value2").unwrap();

        assert_eq!(
            store.get("fixed", &1u64.to_be_bytes()).unwrap(),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            store.get("block", b"key2").unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[test]
    fn cf_blob_db() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cf_configs = vec![ColumnFamilyConfig::new("blobs")
            .with_blob_db(1024) // 1 KiB threshold
            .with_prefix_extractor(8)
            .build()];

        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        // Write a large value that should go into blob storage
        let large_value = vec![0u8; 10 * 1024]; // 10 KiB
        store.put("blobs", b"large_key", &large_value).unwrap();

        let result = store.get("blobs", b"large_key").unwrap();
        assert_eq!(result, Some(large_value));
    }

    #[test]
    fn cf_prefix() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cf_configs = vec![ColumnFamilyConfig::new("prefixed")
            .with_prefix_extractor(4)
            .build()];

        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        // Keys with same prefix
        store.put("prefixed", b"user:alice", b"data1").unwrap();
        store.put("prefixed", b"user:bob", b"data2").unwrap();
        store.put("prefixed", b"post:1234", b"data3").unwrap();

        // Prefix scan should find only "user:" entries
        let users: Vec<_> = store.iter_prefix("prefixed", b"user").unwrap().collect();
        assert_eq!(users.len(), 2);
    }

    #[test]
    fn cf_custom_opts() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cf_configs = vec![ColumnFamilyConfig::new("custom")
            .with_options(|opts| {
                opts.set_write_buffer_size(32 * 1024 * 1024); // 32 MiB
            })
            .build()];

        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        store.put("custom", b"key", b"value").unwrap();
        assert_eq!(store.get("custom", b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn cf_empty() {
        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Empty config should work and create a database
        // When opened with empty config, only the default CF exists (no named CFs)
        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, vec![]).unwrap();

        // Since we opened with empty config, we can create a CF dynamically
        store.create_cf("test").unwrap();
        store.put("test", b"key", b"value").unwrap();
        assert_eq!(store.get("test", b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn read_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create and populate database
        {
            let store = RocksStore::open(&path, &["test"]).unwrap();
            store.put("test", b"key1", b"value1").unwrap();
            store.put("test", b"key2", b"value2").unwrap();
            store.flush().unwrap();
        }

        // Open in read-only mode
        {
            let ro_store = RocksStore::open_read_only(&path, &["test"]).unwrap();

            // Can read
            assert_eq!(ro_store.get("test", b"key1").unwrap(), Some(b"value1".to_vec()));
            assert_eq!(ro_store.get("test", b"key2").unwrap(), Some(b"value2".to_vec()));
            assert!(ro_store.contains("test", b"key1").unwrap());

            // Can iterate
            let entries: Vec<_> = ro_store.iter("test").unwrap().collect();
            assert_eq!(entries.len(), 2);

            // Write operations will fail (RocksDB enforces this)
            // We don't test this as it would require catching a panic or error
        }
    }

    #[test]
    fn secondary() {
        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create primary database
        {
            let primary = RocksStore::open(&primary_path, &["test"]).unwrap();
            primary.put("test", b"key1", b"initial").unwrap();
            primary.flush().unwrap();
        }

        // Open secondary instance
        {
            let secondary = RocksStore::open_secondary(
                &primary_path,
                &secondary_path,
                &["test"]
            ).unwrap();

            // Sync with primary
            secondary.catch_up_with_primary().unwrap();

            // Can read initial data
            assert_eq!(secondary.get("test", b"key1").unwrap(), Some(b"initial".to_vec()));
        }
    }

    #[test]
    fn secondary_catchup() {
        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create and keep primary open
        let primary = RocksStore::open(&primary_path, &["test"]).unwrap();
        primary.put("test", b"key1", b"v1").unwrap();
        primary.flush().unwrap();

        // Open secondary
        let secondary = RocksStore::open_secondary(
            &primary_path,
            &secondary_path,
            &["test"]
        ).unwrap();

        // Initial sync
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.get("test", b"key1").unwrap(), Some(b"v1".to_vec()));

        // Write more data to primary
        primary.put("test", b"key2", b"v2").unwrap();
        primary.flush().unwrap();

        // Before catch-up, secondary might not see new data
        // After catch-up, it should see it
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.get("test", b"key2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn multi_readonly() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create database
        {
            let store = RocksStore::open(&path, &["test"]).unwrap();
            store.put("test", b"shared", b"data").unwrap();
            store.flush().unwrap();
        }

        // Open multiple read-only instances simultaneously
        let ro1 = RocksStore::open_read_only(&path, &["test"]).unwrap();
        let ro2 = RocksStore::open_read_only(&path, &["test"]).unwrap();

        // Both can read the same data
        assert_eq!(ro1.get("test", b"shared").unwrap(), Some(b"data".to_vec()));
        assert_eq!(ro2.get("test", b"shared").unwrap(), Some(b"data".to_vec()));
    }

    #[test]
    fn readonly_cf_config() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create database with custom config
        {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cf_configs = vec![
                ColumnFamilyConfig::new("fixed").with_block_based().build(),
            ];

            let store = RocksStore::open_with_cf_config(&path, db_opts, cf_configs).unwrap();
            store.put("fixed", &1u64.to_be_bytes(), b"value1").unwrap();
            store.flush().unwrap();
        }

        // Open read-only with same config
        {
            let db_opts = Options::default();
            let cf_configs = vec![
                ColumnFamilyConfig::new("fixed").with_block_based().build(),
            ];

            let ro_store = RocksStore::open_read_only_with_cf_config(&path, db_opts, cf_configs).unwrap();
            assert_eq!(
                ro_store.get("fixed", &1u64.to_be_bytes()).unwrap(),
                Some(b"value1".to_vec())
            );
        }
    }

    #[test]
    fn secondary_cf_config() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");

        // Create primary with custom config
        {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cf_configs = vec![
                ColumnFamilyConfig::new("block").with_block_based().build(),
            ];

            let primary = RocksStore::open_with_cf_config(&primary_path, db_opts, cf_configs).unwrap();
            primary.put("block", b"key", b"value").unwrap();
            primary.flush().unwrap();
        }

        // Open secondary with same config
        {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cf_configs = vec![
                ColumnFamilyConfig::new("block").with_block_based().build(),
            ];

            let secondary = RocksStore::open_secondary_with_cf_config(
                &primary_path,
                &secondary_path,
                db_opts,
                cf_configs
            ).unwrap();

            secondary.catch_up_with_primary().unwrap();
            assert_eq!(secondary.get("block", b"key").unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn flush_covers_all_cfs() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["a", "b"]).unwrap();
        store.put("a", b"k", b"v").unwrap();
        store.put("b", b"k", b"v").unwrap();
        store.flush().unwrap();

        for cf in ["a", "b"] {
            let handle = store.inner().cf_handle(cf).unwrap();
            let entries = store
                .inner()
                .property_int_value_cf(&handle, "rocksdb.num-entries-active-mem-table")
                .unwrap()
                .unwrap();
            assert_eq!(entries, 0, "{cf} memtable not flushed");
        }
    }
}
