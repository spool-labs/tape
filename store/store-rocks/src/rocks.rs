//! RocksDB implementation of the Store trait

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use fs2::available_space;
use rocksdb::{
    ColumnFamilyDescriptor, DBRawIteratorWithThreadMode, DBWithThreadMode, IteratorMode,
    MultiThreaded, Options, ReadOptions, WriteBatch as RocksWriteBatch,
};
use tokio::runtime::Handle;

use tape_store_api::{
    BatchOp, CfDiskUsage, Direction, Error, KeyValue, Result, Store, StoreEngine, StoreIter,
    StoreVolume, WriteBatch, Value,
};

#[cfg(feature = "metrics")]
use tape_store_api::get_metrics;
#[cfg(feature = "metrics")]
use tape_metrics::OperationTimer;

/// An exact key count is served this long before the keys are walked again
const COUNT_TTL: Duration = Duration::from_secs(10);

/// A family whose keys are mirrored, and the family that holds the mirror
struct KeyIndex {
    family: String,
    keys: String,
}

/// The family that mirrors the keys of `family`
pub fn key_index_family(family: &str) -> String {
    format!("{family}_keys")
}

/// RocksDB-based persistent key-value store
///
/// Uses multi-threaded column family access for concurrent operations.
pub struct RocksStore {
    /// Shared so an awaited write can take the database to a blocking thread
    db: Arc<DBWithThreadMode<MultiThreaded>>,
    /// Directory the database opened at
    path: PathBuf,
    /// Every column family the database opened with
    column_families: Vec<String>,
    /// Families whose keys are mirrored into a keys family
    key_indexed: Arc<[KeyIndex]>,
    /// The last exact key count of each family
    counts: Mutex<HashMap<String, KeyCount>>,
}

/// One family's key count and when it was walked
struct KeyCount {
    /// Keys the walk found
    keys: u64,
    /// When the walk ran
    counted_at: Instant,
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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
            counts: Mutex::default(),
            db: Arc::new(db),
            key_indexed: Arc::default(),
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

    /// Mirror the keys of these families in a small family beside each
    ///
    /// An iterator over a BlobDB family loads the payload under every key it
    /// lands on, so a count or a key listing reads the payloads. A mirror row
    /// holds the key and the stored length, and `contains`, the key listings,
    /// the count and the byte total are answered from it. Each family needs its
    /// `key_index_family` open beside it. Rows written before the mirror was
    /// kept are not added to it.
    pub fn with_key_index(mut self, families: &[&str]) -> Result<Self> {
        let mut indexed = Vec::with_capacity(families.len());
        for family in families {
            let keys = key_index_family(family);
            for name in [*family, keys.as_str()] {
                if self.db.cf_handle(name).is_none() {
                    return Err(Error::ColumnFamilyNotFound(name.to_string()));
                }
            }
            indexed.push(KeyIndex {
                family: (*family).to_string(),
                keys,
            });
        }

        self.key_indexed = indexed.into();
        Ok(self)
    }

    /// A raw iterator at `start` over the rows that answer for the keys of `cf`
    fn key_rows(
        &self,
        cf: &str,
        start: &[u8],
    ) -> Result<DBRawIteratorWithThreadMode<'_, DBWithThreadMode<MultiThreaded>>> {
        let family = keys_family(&self.key_indexed, cf).unwrap_or(cf);
        let handle = self
            .db
            .cf_handle(family)
            .ok_or_else(|| Error::ColumnFamilyNotFound(family.to_string()))?;

        let mut rows = self.db.raw_iterator_cf_opt(&handle, total_order());
        rows.seek(start);
        Ok(rows)
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
    fn store_engine(&self) -> StoreEngine {
        StoreEngine::Rocks
    }

    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>> {
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

        result.map(|held| held.map(Value::new))
    }

    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let mut values = Vec::with_capacity(keys.len());
        let mut read = 0usize;
        let mut failure = None;
        for found in self.db.batched_multi_get_cf(&cf_handle, keys, false) {
            match found {
                Ok(held) => {
                    let value = held.map(|slice| Value::new(slice.to_vec()));
                    read += value.as_ref().map_or(0, |value| value.len());
                    values.push(value);
                }
                Err(error) => failure = Some(Error::Database(error.to_string())),
            }
        }
        count_batch(cf, read, failure.is_none());

        match failure {
            Some(error) => Err(error),
            None => Ok(values),
        }
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        put_blocking(&self.db, &self.key_indexed, cf, key, value)
    }

    async fn put_owned_wait(&self, cf: &str, key: &[u8], value: Vec<u8>) -> Result<()> {
        // Another executor has no blocking pool to hand the write to.
        let Ok(runtime) = Handle::try_current() else {
            return put_blocking(&self.db, &self.key_indexed, cf, key, &value);
        };

        let db = Arc::clone(&self.db);
        let indexed = Arc::clone(&self.key_indexed);
        let cf = cf.to_string();
        let key = key.to_vec();
        let written = runtime
            .spawn_blocking(move || put_blocking(&db, &indexed, &cf, &key, &value))
            .await;

        match written {
            Ok(result) => result,
            Err(error) => Err(Error::Database(format!("put task: {error}"))),
        }
    }

    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        let result = match keys_family(&self.key_indexed, cf) {
            None => self.db.delete_range_cf(&cf_handle, start, end),
            Some(keys) => {
                let keys_handle = self
                    .db
                    .cf_handle(keys)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(keys.to_string()))?;

                let mut batch = RocksWriteBatch::default();
                batch.delete_range_cf(&keys_handle, start, end);
                batch.delete_range_cf(&cf_handle, start, end);
                self.db.write(batch)
            }
        }
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

        let result = match keys_family(&self.key_indexed, cf) {
            None => self.db.delete_cf(&cf_handle, key),
            Some(keys) => {
                let keys_handle = self
                    .db
                    .cf_handle(keys)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(keys.to_string()))?;

                let mut batch = RocksWriteBatch::default();
                batch.delete_cf(&keys_handle, key);
                batch.delete_cf(&cf_handle, key);
                self.db.write(batch)
            }
        }
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

        // A mirror row says the key is there with no payload behind the read.
        let family = keys_family(&self.key_indexed, cf).unwrap_or(cf);
        let cf_handle = self
            .db
            .cf_handle(family)
            .ok_or_else(|| Error::ColumnFamilyNotFound(family.to_string()))?;

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
        write_batch_blocking(&self.db, &self.key_indexed, batch)
    }

    async fn write_batch_wait(&self, batch: WriteBatch) -> Result<()> {
        // An empty batch has nothing worth a thread hop.
        if batch.is_empty() {
            return Ok(());
        }

        // Another executor has no blocking pool to hand the write to.
        let Ok(runtime) = Handle::try_current() else {
            return write_batch_blocking(&self.db, &self.key_indexed, batch);
        };

        let db = Arc::clone(&self.db);
        let indexed = Arc::clone(&self.key_indexed);
        let written = runtime
            .spawn_blocking(move || write_batch_blocking(&db, &indexed, batch))
            .await;

        match written {
            Ok(result) => result,
            Err(error) => Err(Error::Database(format!("batch task: {error}"))),
        }
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
            .iterator_cf_opt(&cf_handle, total_order(), IteratorMode::Start)
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
            .iterator_cf_opt(
                &cf_handle,
                up_to(prefix_end(&prefix)),
                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
            )
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

    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        #[cfg(feature = "metrics")]
        let timer = OperationTimer::new();

        let mut iter = self.key_rows(cf, prefix)?;

        let mut keys = Vec::new();
        while iter.valid() {
            match iter.key() {
                Some(key) if key.starts_with(prefix) => keys.push(key.to_vec()),
                _ => break,
            }
            iter.next();
        }
        iter.status().map_err(|e| Error::Database(e.to_string()))?;

        #[cfg(feature = "metrics")]
        if let Some(metrics) = get_metrics() {
            metrics
                .iter_duration
                .with_label_values(&[cf, "keys_prefix"])
                .observe(timer.elapsed_secs());
            metrics
                .operations_total
                .with_label_values(&[cf, "iter_keys_prefix", "success"])
                .inc();
        }

        Ok(keys)
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
            .iterator_cf_opt(&cf_handle, total_order(), mode)
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
            .iterator_cf_opt(
                &cf_handle,
                up_to(Some(end.clone())),
                IteratorMode::From(start, rocksdb::Direction::Forward),
            )
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

    fn live_data_size_bytes(&self) -> Result<Option<u64>> {
        // SST plus blob files, since the bulk store keeps slice payloads in BlobDB.
        let mut total = 0u64;
        for cf in self
            .column_families
            .iter()
            .map(String::as_str)
            .chain(std::iter::once("default"))
        {
            let Some(handle) = self.db.cf_handle(cf) else {
                continue;
            };
            for prop in ["rocksdb.total-sst-files-size", "rocksdb.total-blob-file-size"] {
                if let Ok(Some(bytes)) = self.db.property_int_value_cf(&handle, prop) {
                    total = total.saturating_add(bytes);
                }
            }
        }
        Ok(Some(total))
    }

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        count_op(cf, "count_prefix");
        let mut rows = self.key_rows(cf, prefix)?;

        let mut count = 0u64;
        while rows.valid() {
            match rows.key() {
                Some(key) if key.starts_with(prefix) => count += 1,
                _ => break,
            }
            rows.next();
        }
        rows.status().map_err(|e| Error::Database(e.to_string()))?;

        Ok(count)
    }

    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Option<u64>> {
        // RocksDB keeps no byte counter per key range. A family with a keys
        // mirror answers from the lengths its mirror rows hold, and any other
        // could only answer by reading every payload in the range.
        if keys_family(&self.key_indexed, cf).is_none() {
            return Ok(None);
        }

        let mut rows = self.key_rows(cf, prefix)?;

        let mut bytes = 0u64;
        while rows.valid() {
            let (Some(key), Some(length)) = (rows.key(), rows.value()) else {
                break;
            };
            if !key.starts_with(prefix) {
                break;
            }
            bytes = bytes.saturating_add(mirrored_len(length)?);
            rows.next();
        }
        rows.status().map_err(|e| Error::Database(e.to_string()))?;

        Ok(Some(bytes))
    }

    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<KeyValue>, Option<Vec<u8>>)> {
        count_op(cf, "sweep_prefix");
        let cf_handle = self
            .db
            .cf_handle(cf)
            .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

        // The mark is the first key a page left out, so the next page starts on it.
        let mut rows = self
            .db
            .raw_iterator_cf_opt(&cf_handle, up_to(prefix_end(prefix)));
        rows.seek(from.unwrap_or(prefix));

        let mut page = Vec::new();
        let mut next = None;
        while rows.valid() {
            let (Some(key), Some(value)) = (rows.key(), rows.value()) else {
                break;
            };
            if !key.starts_with(prefix) {
                break;
            }
            if page.len() == limit {
                next = Some(key.to_vec());
                break;
            }
            page.push((key.to_vec(), value.to_vec()));
            rows.next();
        }
        rows.status().map_err(|e| Error::Database(e.to_string()))?;

        Ok((page, next))
    }

    fn sweep_keys_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        count_op(cf, "sweep_keys_prefix");
        // The mark is the first key a page left out, so the next page starts on it.
        let mut rows = self.key_rows(cf, from.unwrap_or(prefix))?;

        let mut keys = Vec::new();
        let mut next = None;
        while rows.valid() {
            let Some(key) = rows.key() else {
                break;
            };
            if !key.starts_with(prefix) {
                break;
            }
            if keys.len() == limit {
                next = Some(key.to_vec());
                break;
            }
            keys.push(key.to_vec());
            rows.next();
        }
        rows.status().map_err(|e| Error::Database(e.to_string()))?;

        Ok((keys, next))
    }

    // estimate-num-keys counts each overwrite until compaction, so walk the keys
    fn key_count_estimate(&self, cf: &str) -> Result<Option<u64>> {
        if self.db.cf_handle(cf).is_none() {
            return Ok(None);
        }
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = counts.get(cf) {
            if count.counted_at.elapsed() < COUNT_TTL {
                return Ok(Some(count.keys));
            }
        }
        let keys = self.count_prefix(cf, &[])?;
        counts.insert(cf.to_string(), KeyCount { keys, counted_at: Instant::now() });
        Ok(Some(keys))
    }

    fn cf_disk_usage(&self) -> Result<Vec<CfDiskUsage>> {
        let mut usage = Vec::with_capacity(self.column_families.len());
        for cf in self.column_families.iter().map(String::as_str) {
            let Some(handle) = self.db.cf_handle(cf) else {
                continue;
            };
            let read = |property: &str| {
                self.db
                    .property_int_value_cf(&handle, property)
                    .ok()
                    .flatten()
                    .unwrap_or(0)
            };
            usage.push(CfDiskUsage {
                cf: cf.to_string(),
                // A single instance is one volume; the split store overrides this per half.
                volume: StoreVolume::Primary,
                sst_bytes: read("rocksdb.total-sst-files-size"),
                blob_bytes: read("rocksdb.total-blob-file-size"),
                num_keys: read("rocksdb.estimate-num-keys"),
            });
        }
        Ok(usage)
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

/// The keys family of `cf`, when its keys are mirrored
fn keys_family<'a>(indexed: &'a [KeyIndex], cf: &str) -> Option<&'a str> {
    for index in indexed {
        if index.family == cf {
            return Some(&index.keys);
        }
    }
    None
}

/// The value of a mirror row: the stored length of the row it mirrors
fn stored_len(value: &[u8]) -> [u8; 8] {
    (value.len() as u64).to_le_bytes()
}

/// The length a mirror row holds
fn mirrored_len(row: &[u8]) -> Result<u64> {
    match <[u8; 8]>::try_from(row) {
        Ok(length) => Ok(u64::from_le_bytes(length)),
        Err(_) => Err(Error::Database(format!("key index row of {} bytes", row.len()))),
    }
}

/// Write one row on the calling thread
fn put_blocking(
    db: &DBWithThreadMode<MultiThreaded>,
    indexed: &[KeyIndex],
    cf: &str,
    key: &[u8],
    value: &[u8],
) -> Result<()> {
    #[cfg(feature = "metrics")]
    let timer = OperationTimer::new();

    let cf_handle = db
        .cf_handle(cf)
        .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;

    let result = match keys_family(indexed, cf) {
        None => db.put_cf(&cf_handle, key, value),
        Some(keys) => {
            let keys_handle = db
                .cf_handle(keys)
                .ok_or_else(|| Error::ColumnFamilyNotFound(keys.to_string()))?;

            // One batch, so a row and its mirror land together. The mirror row
            // goes in first: growing the buffer for the payload then recopies
            // a few bytes, where the other order would recopy the payload.
            let mut batch = RocksWriteBatch::default();
            batch.put_cf(&keys_handle, key, stored_len(value));
            batch.put_cf(&cf_handle, key, value);
            db.write(batch)
        }
    }
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

/// Write a batch atomically on the calling thread
fn write_batch_blocking(
    db: &DBWithThreadMode<MultiThreaded>,
    indexed: &[KeyIndex],
    batch: WriteBatch,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    #[cfg(feature = "metrics")]
    let timer = OperationTimer::new();

    #[cfg(feature = "metrics")]
    let batch_len = batch.len();

    // Size the batch buffer up front. Appending a small op after a large
    // payload would otherwise grow the buffer and recopy the whole payload.
    let mut rocks_batch = RocksWriteBatch::with_capacity_bytes(batch_capacity_bytes(&batch));

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

                let cf_handle = db
                    .cf_handle(cf)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;
                rocks_batch.put_cf(&cf_handle, key, value);

                if let Some(keys) = keys_family(indexed, cf) {
                    let keys_handle = db
                        .cf_handle(keys)
                        .ok_or_else(|| Error::ColumnFamilyNotFound(keys.to_string()))?;
                    rocks_batch.put_cf(&keys_handle, key, stored_len(value));
                }
            }
            BatchOp::Delete { cf, key } => {
                #[cfg(feature = "metrics")]
                {
                    cf_name = cf.clone();
                }

                let cf_handle = db
                    .cf_handle(cf)
                    .ok_or_else(|| Error::ColumnFamilyNotFound(cf.to_string()))?;
                rocks_batch.delete_cf(&cf_handle, key);

                if let Some(keys) = keys_family(indexed, cf) {
                    let keys_handle = db
                        .cf_handle(keys)
                        .ok_or_else(|| Error::ColumnFamilyNotFound(keys.to_string()))?;
                    rocks_batch.delete_cf(&keys_handle, key);
                }
            }
        }
    }

    let result = db
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

/// Read options for an iterator that walks a family in key order
///
/// A family with a prefix extractor seeks in prefix mode by default: the seek
/// skips every table file that holds no key under the seek key's prefix, and a
/// seek key shorter than the extractor matches nothing. A walk that crosses
/// prefixes then loses rows once they are flushed out of the memtable.
fn total_order() -> ReadOptions {
    let mut options = ReadOptions::default();
    options.set_total_order_seek(true);
    options
}

/// Count one store call
#[cfg(feature = "metrics")]
fn count_op(cf: &str, op: &str) {
    if let Some(metrics) = get_metrics() {
        metrics.operations_total.with_label_values(&[cf, op, "success"]).inc();
    }
}

#[cfg(not(feature = "metrics"))]
fn count_op(_: &str, _: &str) {}

/// Count one batch read and the bytes it found
#[cfg(feature = "metrics")]
fn count_batch(cf: &str, read: usize, is_ok: bool) {
    let Some(metrics) = get_metrics() else {
        return;
    };
    let status = if is_ok { "success" } else { "error" };
    metrics.operations_total.with_label_values(&[cf, "get_many", status]).inc();
    metrics.bytes_read_total.with_label_values(&[cf]).inc_by(read as u64);
    if !is_ok {
        metrics.errors_total.with_label_values(&[cf, "get_many", "database"]).inc();
    }
}

#[cfg(not(feature = "metrics"))]
fn count_batch(_: &str, _: usize, _: bool) {}

/// Read options for a walk in key order that stops before `end`
///
/// An iterator over a BlobDB family loads the payload under every key it
/// lands on, the first key past the rows it was asked for included. RocksDB
/// checks the bound before it loads anything, so a bounded walk reads no
/// payload it will not return.
fn up_to(end: Option<Vec<u8>>) -> ReadOptions {
    let mut options = total_order();
    if let Some(end) = end {
        options.set_iterate_upper_bound(end);
    }
    options
}

/// The first key after every key under `prefix`, where there is one
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Bytes to reserve for a batch's serialized form
///
/// RocksDB frames each record with a type tag, a column family id, and varint
/// lengths, ahead of a fixed header. The per-record allowance is a generous
/// upper bound: overshooting wastes a little memory, undershooting forces the
/// buffer to grow and recopy every payload already staged. Every op is given
/// room for a mirror row, whether or not its family keeps one.
fn batch_capacity_bytes(batch: &WriteBatch) -> usize {
    const HEADER_BYTES: usize = 12;
    const PER_RECORD_BYTES: usize = 24;
    const MIRROR_VALUE_BYTES: usize = 8;

    let mut total = HEADER_BYTES;
    for op in batch.iter() {
        let (key, value_len) = match op {
            BatchOp::Put { key, value, .. } => (key, value.len()),
            BatchOp::Delete { key, .. } => (key, 0),
        };
        total += 2 * (PER_RECORD_BYTES + key.len()) + value_len + MIRROR_VALUE_BYTES;
    }
    total
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
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::config::ColumnFamilyConfig;
    use crate::Cache;
    use rocksdb::statistics::Ticker;
    use tempfile::tempdir;

    fn prefixed(path: &Path, prefix_len: usize) -> RocksStore {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cache = Cache::new_lru_cache(8 * 1024 * 1024);
        let cf_configs = vec![
            ColumnFamilyConfig::new("prefixed")
                .with_block_based(&cache)
                .with_prefix_extractor(prefix_len)
                .build(),
        ];
        RocksStore::open_with_cf_config(path, db_opts, cf_configs).expect("open")
    }

    // a blob family with its keys mirror, payloads small enough to write fast
    fn mirrored(path: &Path) -> RocksStore {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cache = Cache::new_lru_cache(8 * 1024 * 1024);
        let cf_configs = vec![
            ColumnFamilyConfig::new("slice")
                .with_block_based(&cache)
                .with_blob_db(1024)
                .build(),
            ColumnFamilyConfig::new(key_index_family("slice"))
                .with_block_based(&cache)
                .build(),
        ];
        RocksStore::open_with_cf_config(path, db_opts, cf_configs)
            .expect("open")
            .with_key_index(&["slice"])
            .expect("key index")
    }

    #[test]
    fn open_basic_ops() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        // Put and get
        store.put("test", b"key1", b"value1").unwrap();
        let result = store.get("test", b"key1").unwrap();
        assert_eq!(result.map(Value::into_vec), Some(b"value1".to_vec()));

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

        assert_eq!(store.get("cf1", b"key").unwrap().map(Value::into_vec), Some(b"value1".to_vec()));
        assert_eq!(store.get("cf2", b"key").unwrap().map(Value::into_vec), Some(b"value2".to_vec()));
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

        assert_eq!(store.get("test", b"key1").unwrap().map(Value::into_vec), Some(b"value1".to_vec()));
        assert_eq!(store.get("test", b"key2").unwrap().map(Value::into_vec), Some(b"value2".to_vec()));
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
        assert_eq!(store.get("dynamic", b"key").unwrap().map(Value::into_vec), Some(b"value".to_vec()));
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
            assert_eq!(store.get("test", b"key").unwrap().map(Value::into_vec), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn binary_data() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        let key = vec![0u8, 1, 2, 255, 254];
        let value = vec![100u8, 200, 0, 1, 255];

        store.put("test", &key, &value).unwrap();
        assert_eq!(store.get("test", &key).unwrap().map(Value::into_vec), Some(value));
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

    // an empty prefix walks a prefix-extractor family whole, and does not crash
    #[test]
    fn empty_prefix() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cache = crate::Cache::new_lru_cache(8 * 1024 * 1024);
        let cf_configs = vec![
            ColumnFamilyConfig::new("prefixed")
                .with_block_based(&cache)
                .with_prefix_extractor(2)
                .build(),
        ];
        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        for key in [b"aa1".as_slice(), b"ab1", b"ba1"] {
            store.put("prefixed", key, b"value").unwrap();
        }

        let all: Vec<_> = store.iter_prefix("prefixed", &[]).unwrap().collect();
        assert_eq!(all.len(), 3);

        let one: Vec<_> = store.iter_prefix("prefixed", b"ab").unwrap().collect();
        assert_eq!(one.len(), 1);
    }

    // a seek from a prefix with no rows lands on the next one, once flushed
    #[test]
    fn seek_crosses_prefix() {
        let dir = tempdir().expect("tempdir");
        let store = prefixed(dir.path(), 2);
        for key in [b"aa1".as_slice(), b"cc1"] {
            store.put("prefixed", key, b"value").expect("put");
        }
        store.flush().expect("flush");

        let first = store
            .iter_from("prefixed", b"bb", Direction::Asc)
            .expect("iter")
            .next();

        assert_eq!(first.map(|(key, _)| key), Some(b"cc1".to_vec()));
    }

    // a prefix shorter than the extractor finds its rows, once flushed
    #[test]
    fn short_prefix() {
        let dir = tempdir().expect("tempdir");
        let store = prefixed(dir.path(), 4);
        for key in [b"aaaa1".as_slice(), b"aabb1", b"bbbb1"] {
            store.put("prefixed", key, b"value").expect("put");
        }
        store.flush().expect("flush");

        let rows: Vec<_> = store.iter_prefix("prefixed", b"aa").expect("iter").collect();

        assert_eq!(rows.len(), 2);
    }

    // an awaited put lands where a blocking put does
    #[tokio::test]
    async fn awaited_put() {
        let dir = tempdir().expect("tempdir");
        let store = RocksStore::open(dir.path(), &["test"]).expect("open");

        store
            .put_owned_wait("test", b"key", b"value".to_vec())
            .await
            .expect("put");

        let stored = store.get("test", b"key").expect("get").map(Value::into_vec);
        assert_eq!(stored, Some(b"value".to_vec()));
    }

    // an awaited batch commits every op in it
    #[tokio::test]
    async fn awaited_batch() {
        let dir = tempdir().expect("tempdir");
        let store = RocksStore::open(dir.path(), &["test"]).expect("open");
        store.put("test", b"old", b"1").expect("put");
        let mut batch = WriteBatch::new();
        batch.put("test", b"new", b"2");
        batch.delete("test", b"old");

        store.write_batch_wait(batch).await.expect("batch");

        assert!(store.get("test", b"old").expect("get").is_none());
        assert!(store.get("test", b"new").expect("get").is_some());
    }

    // the count and the byte total follow puts, overwrites and deletes
    #[test]
    fn mirror_follows_writes() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        store.put("slice", b"aa1", &[1; 2000]).expect("put");
        store.put("slice", b"aa2", &[2; 3000]).expect("put");
        store.put("slice", b"bb1", &[3; 4000]).expect("put");
        store.flush().expect("flush");
        assert_eq!(store.count_prefix("slice", b"aa").expect("count"), 2);
        assert_eq!(store.bytes_prefix("slice", b"aa").expect("bytes"), Some(5000));

        store.put("slice", b"aa1", &[1; 1500]).expect("overwrite");
        store.delete("slice", b"aa2").expect("delete");

        assert_eq!(store.count_prefix("slice", b"aa").expect("count"), 1);
        assert_eq!(store.bytes_prefix("slice", b"aa").expect("bytes"), Some(1500));
        assert!(store.contains("slice", b"aa1").expect("contains"));
        assert!(!store.contains("slice", b"aa2").expect("contains"));
        assert_eq!(store.count_prefix("slice", &[]).expect("count"), 2);
    }

    // a range delete clears the mirror rows with the payloads
    #[test]
    fn mirror_range_delete() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        for key in [b"aa1".as_slice(), b"aa2", b"bb1"] {
            store.put("slice", key, &[7; 2000]).expect("put");
        }

        store.delete_range("slice", b"aa", b"ab").expect("delete range");

        assert_eq!(store.count_prefix("slice", b"aa").expect("count"), 0);
        assert!(store.get("slice", b"aa1").expect("get").is_none());
        assert_eq!(store.count_prefix("slice", b"bb").expect("count"), 1);
    }

    // a batch and an awaited put keep the mirror in step
    #[tokio::test]
    async fn mirror_batched_writes() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        store.put("slice", b"aa1", &[1; 2000]).expect("put");
        let mut batch = WriteBatch::new();
        batch.put("slice", b"aa2", &[2; 3000]);
        batch.delete("slice", b"aa1");

        store.write_batch(batch).expect("batch");
        store
            .put_owned_wait("slice", b"aa3", vec![3; 1000])
            .await
            .expect("awaited put");

        assert_eq!(store.count_prefix("slice", b"aa").expect("count"), 2);
        assert_eq!(store.bytes_prefix("slice", b"aa").expect("bytes"), Some(4000));
    }

    // key pages cover a prefix in order and resume on the mark
    #[test]
    fn mirror_key_pages() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        for key in [b"aa1".as_slice(), b"aa2", b"aa3", b"aa4", b"aa5", b"bb1"] {
            store.put("slice", key, &[7; 2000]).expect("put");
        }
        store.flush().expect("flush");

        let mut paged = Vec::new();
        let mut mark: Option<Vec<u8>> = None;
        loop {
            let (keys, next) = store
                .sweep_keys_prefix("slice", b"aa", mark.as_deref(), 2)
                .expect("page");
            paged.extend(keys);
            match next {
                Some(next) => mark = Some(next),
                None => break,
            }
        }

        assert_eq!(paged, store.iter_keys_prefix("slice", b"aa").expect("keys"));
        assert_eq!(paged.len(), 5);
    }

    // a prefix ends at its successor, and a prefix of all ones has no end
    #[test]
    fn prefix_ends() {
        assert_eq!(prefix_end(b"ab"), Some(b"ac".to_vec()));
        assert_eq!(prefix_end(&[0x61, 0xFF]), Some(vec![0x62]));
        assert_eq!(prefix_end(&[0xFF, 0xFF]), None);
        assert_eq!(prefix_end(&[]), None);
    }

    // value pages cover a prefix in order, resume on the mark and stop at its end
    #[test]
    fn value_pages() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        for key in [b"aa1".as_slice(), b"aa2", b"aa3", b"ab1"] {
            store.put("slice", key, &[7; 2000]).expect("put");
        }
        store.flush().expect("flush");

        let mut paged = Vec::new();
        let mut mark: Option<Vec<u8>> = None;
        loop {
            let (rows, next) = store
                .sweep_prefix("slice", b"aa", mark.as_deref(), 2)
                .expect("page");
            for (key, value) in rows {
                assert_eq!(value.len(), 2000);
                paged.push(key);
            }
            match next {
                Some(next) => mark = Some(next),
                None => break,
            }
        }

        assert_eq!(paged, vec![b"aa1".to_vec(), b"aa2".to_vec(), b"aa3".to_vec()]);
    }

    // a walk over a prefix with no rows reads no payload from the blob files
    #[test]
    fn empty_walk_reads_nothing() {
        let dir = tempdir().expect("tempdir");
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.enable_statistics();
        let statistics = db_opts.clone();
        let cache = Cache::new_lru_cache(8 * 1024 * 1024);
        let cf_configs = vec![
            ColumnFamilyConfig::new("slice")
                .with_block_based(&cache)
                .with_blob_db(1024)
                .build(),
        ];
        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).expect("open");
        store.put("slice", b"bb1", &[7; 4000]).expect("put");
        store.flush().expect("flush");
        let blob_bytes = || statistics.get_ticker_count(Ticker::BlobDbBlobFileBytesRead);

        let before = blob_bytes();
        let rows = store.iter_prefix("slice", b"aa").expect("iter").count();
        let (page, next) = store.sweep_prefix("slice", b"aa", None, 10).expect("page");
        let ranged = store.iter_range("slice", b"aa", b"ab").expect("range").count();
        let after_empty = blob_bytes();
        let held = store.iter_prefix("slice", b"bb").expect("iter").count();
        let after_held = blob_bytes();

        assert_eq!((rows, page.len(), next, ranged), (0, 0, None, 0));
        assert_eq!(after_empty, before);
        assert_eq!(held, 1);
        assert!(after_held > after_empty);
    }

    // keys-only questions are read from the mirror, never from the payload family
    #[test]
    fn reads_from_mirror() {
        let dir = tempdir().expect("tempdir");
        let store = mirrored(dir.path());
        store.put("slice", b"aa1", &[1; 2000]).expect("put");
        let payloads = store.inner().cf_handle("slice").expect("family");
        store
            .inner()
            .put_cf(&payloads, b"aa2", [2; 2000])
            .expect("payload with no mirror row");

        assert_eq!(store.count_prefix("slice", b"aa").expect("count"), 1);
        assert!(!store.contains("slice", b"aa2").expect("contains"));
        assert_eq!(store.iter_keys_prefix("slice", b"aa").expect("keys").len(), 1);
    }

    // a family with no mirror counts its own rows and gives no byte total
    #[test]
    fn unmirrored_family() {
        let dir = tempdir().expect("tempdir");
        let store = RocksStore::open(dir.path(), &["test"]).expect("open");
        for key in [b"aa1".as_slice(), b"aa2", b"bb1"] {
            store.put("test", key, b"value").expect("put");
        }

        assert_eq!(store.count_prefix("test", b"aa").expect("count"), 2);
        assert_eq!(store.bytes_prefix("test", b"aa").expect("bytes"), None);
    }

    // a family cannot be mirrored without its keys family open
    #[test]
    fn mirror_needs_family() {
        let dir = tempdir().expect("tempdir");
        let store = RocksStore::open(dir.path(), &["slice"]).expect("open");

        let mirrored = store.with_key_index(&["slice"]);

        assert!(matches!(mirrored, Err(Error::ColumnFamilyNotFound(_))));
    }

    // with no tokio runtime running, an awaited put finishes on the first poll
    #[test]
    fn off_runtime() {
        let dir = tempdir().expect("tempdir");
        let store = RocksStore::open(dir.path(), &["test"]).expect("open");
        let mut write = pin!(store.put_owned_wait("test", b"key", b"value".to_vec()));

        let polled = write.as_mut().poll(&mut Context::from_waker(Waker::noop()));

        assert!(matches!(polled, Poll::Ready(Ok(()))));
        assert!(store.get("test", b"key").expect("get").is_some());
    }

    #[test]
    fn iter_keys_prefix() {
        let dir = tempdir().unwrap();
        let store = RocksStore::open(dir.path(), &["test"]).unwrap();

        store.put("test", b"user:1", b"alice").unwrap();
        store.put("test", b"user:2", b"bob").unwrap();
        store.put("test", b"post:1", b"hello").unwrap();
        store.put("test", b"user:3", b"charlie").unwrap();

        // Returns only the matching keys, in order, and stops at the prefix edge.
        let keys = store.iter_keys_prefix("test", b"user:").unwrap();
        assert_eq!(
            keys,
            vec![b"user:1".to_vec(), b"user:2".to_vec(), b"user:3".to_vec()]
        );

        // A prefix with no matches yields nothing.
        assert!(store.iter_keys_prefix("test", b"zzz:").unwrap().is_empty());
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

        let cache = crate::Cache::new_lru_cache(8 * 1024 * 1024);
        let cf_configs = vec![
            ColumnFamilyConfig::new("fixed").with_block_based(&cache).build(),
            ColumnFamilyConfig::new("block").with_block_based(&cache).build(),
        ];

        let store = RocksStore::open_with_cf_config(dir.path(), db_opts, cf_configs).unwrap();

        // Test operations on configured column families
        store.put("fixed", &1u64.to_be_bytes(), b"value1").unwrap();
        store.put("block", b"key2", b"value2").unwrap();

        assert_eq!(
            store.get("fixed", &1u64.to_be_bytes()).unwrap().map(Value::into_vec), Some(b"value1".to_vec())
        );
        assert_eq!(
            store.get("block", b"key2").unwrap().map(Value::into_vec), Some(b"value2".to_vec())
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
        assert_eq!(result.map(Value::into_vec), Some(large_value));
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
        assert_eq!(store.get("custom", b"key").unwrap().map(Value::into_vec), Some(b"value".to_vec()));
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
        assert_eq!(store.get("test", b"key").unwrap().map(Value::into_vec), Some(b"value".to_vec()));
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
            assert_eq!(ro_store.get("test", b"key1").unwrap().map(Value::into_vec), Some(b"value1".to_vec()));
            assert_eq!(ro_store.get("test", b"key2").unwrap().map(Value::into_vec), Some(b"value2".to_vec()));
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
            assert_eq!(secondary.get("test", b"key1").unwrap().map(Value::into_vec), Some(b"initial".to_vec()));
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
        assert_eq!(secondary.get("test", b"key1").unwrap().map(Value::into_vec), Some(b"v1".to_vec()));

        // Write more data to primary
        primary.put("test", b"key2", b"v2").unwrap();
        primary.flush().unwrap();

        // Before catch-up, secondary might not see new data
        // After catch-up, it should see it
        secondary.catch_up_with_primary().unwrap();
        assert_eq!(secondary.get("test", b"key2").unwrap().map(Value::into_vec), Some(b"v2".to_vec()));
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
        assert_eq!(ro1.get("test", b"shared").unwrap().map(Value::into_vec), Some(b"data".to_vec()));
        assert_eq!(ro2.get("test", b"shared").unwrap().map(Value::into_vec), Some(b"data".to_vec()));
    }

    #[test]
    fn readonly_cf_config() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let cache = crate::Cache::new_lru_cache(8 * 1024 * 1024);

        // Create database with custom config
        {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cf_configs = vec![
                ColumnFamilyConfig::new("fixed").with_block_based(&cache).build(),
            ];

            let store = RocksStore::open_with_cf_config(&path, db_opts, cf_configs).unwrap();
            store.put("fixed", &1u64.to_be_bytes(), b"value1").unwrap();
            store.flush().unwrap();
        }

        // Open read-only with same config
        {
            let db_opts = Options::default();
            let cf_configs = vec![
                ColumnFamilyConfig::new("fixed").with_block_based(&cache).build(),
            ];

            let ro_store = RocksStore::open_read_only_with_cf_config(&path, db_opts, cf_configs).unwrap();
            assert_eq!(
                ro_store.get("fixed", &1u64.to_be_bytes()).unwrap().map(Value::into_vec), Some(b"value1".to_vec())
            );
        }
    }

    #[test]
    fn secondary_cf_config() {
        use crate::config::ColumnFamilyConfig;

        let dir = tempdir().unwrap();
        let primary_path = dir.path().join("primary");
        let secondary_path = dir.path().join("secondary");
        let cache = crate::Cache::new_lru_cache(8 * 1024 * 1024);

        // Create primary with custom config
        {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cf_configs = vec![
                ColumnFamilyConfig::new("block").with_block_based(&cache).build(),
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
                ColumnFamilyConfig::new("block").with_block_based(&cache).build(),
            ];

            let secondary = RocksStore::open_secondary_with_cf_config(
                &primary_path,
                &secondary_path,
                db_opts,
                cf_configs
            ).unwrap();

            secondary.catch_up_with_primary().unwrap();
            assert_eq!(secondary.get("block", b"key").unwrap().map(Value::into_vec), Some(b"value".to_vec()));
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
