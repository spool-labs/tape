//! Metadata on RocksDB, bulk on the public reel, behind one store trait
//!
//! The layout a node would actually run the reel in: the reel serves the bulk
//! families and nothing else, and the small hot metadata stays on RocksDB where
//! it is today. Routing is by column family, mirroring `store_rocks::SplitStore`,
//! so the only difference between this and the all-rocks split arm is which
//! engine holds the slices.

use std::path::Path;

use store::{
    BatchOp, CfDiskUsage, Direction, DiskVolume, Result, Store, StoreIter, StoreVolume, WriteBatch, Value};
use store_rocks::RocksStore;
use tape_store::config::{BULK_COLUMN_FAMILIES, META_SUBDIR};

use crate::{bench_cache, bench_db_options, bench_metadata_configs, ReelBridge};

/// Subdirectory the reel's segment files live in
pub const REEL_SUBDIR: &str = "reel";

/// A tape store whose bulk families are served by the reel
pub struct MetaBulkStore {
    meta: RocksStore,
    bulk: ReelBridge,
}

impl MetaBulkStore {
    /// Open both halves under one root, the metadata volume beside the reel
    pub fn open(
        root: &Path,
        config: reel::ReelConfig,
        columns: reel::ColumnSet,
    ) -> Result<MetaBulkStore> {
        let meta_dir = root.join(META_SUBDIR);
        std::fs::create_dir_all(&meta_dir)?;

        // The same tuned rocks the all-rocks arm opens, so the only difference
        // between the two arms is which engine holds the slices.
        let cache = bench_cache();
        let meta = RocksStore::open_with_cf_config(
            &meta_dir,
            bench_db_options(),
            bench_metadata_configs(&cache),
        )?;
        let bulk = ReelBridge::open(root.join(REEL_SUBDIR), config, columns)?;
        Ok(MetaBulkStore { meta, bulk })
    }

    /// The metadata half
    pub fn meta(&self) -> &RocksStore {
        &self.meta
    }

    /// The bulk half
    pub fn bulk(&self) -> &ReelBridge {
        &self.bulk
    }

    /// Settle both halves
    pub fn flush(&self) -> Result<()> {
        self.meta.flush()?;
        self.bulk.flush()
    }

    fn is_bulk(&self, cf: &str) -> bool {
        BULK_COLUMN_FAMILIES.contains(&cf)
    }

    fn route(&self, cf: &str) -> &dyn Store {
        if self.is_bulk(cf) {
            &self.bulk
        } else {
            &self.meta
        }
    }
}

impl Store for MetaBulkStore {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>> {
        self.route(cf).get(cf, key)
    }

    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        self.route(cf).get_many(cf, keys)
    }

    fn get_range(&self, cf: &str, key: &[u8], offset: u64, len: usize) -> Result<Option<Value>> {
        self.route(cf).get_range(cf, key, offset, len)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.route(cf).put(cf, key, value)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> Result<()> {
        self.route(cf).delete(cf, key)
    }

    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool> {
        self.route(cf).contains(cf, key)
    }

    fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        // A batch owned by one volume goes over whole, so a staged payload is
        // never copied into a per-volume batch on the path that matters.
        let mut any_bulk = false;
        let mut any_meta = false;
        for op in batch.iter() {
            if self.is_bulk(op.cf()) {
                any_bulk = true;
            } else {
                any_meta = true;
            }
        }
        if !(any_bulk && any_meta) {
            return if any_bulk {
                self.bulk.write_batch(batch)
            } else {
                self.meta.write_batch(batch)
            };
        }

        let mut meta_batch = WriteBatch::new();
        let mut bulk_batch = WriteBatch::new();
        for op in batch {
            match op {
                BatchOp::Put { cf, key, value } => {
                    if self.is_bulk(&cf) {
                        bulk_batch.put_owned(&cf, key, value);
                    } else {
                        meta_batch.put_owned(&cf, key, value);
                    }
                }
                BatchOp::Delete { cf, key } => {
                    if self.is_bulk(&cf) {
                        bulk_batch.delete_owned(&cf, key);
                    } else {
                        meta_batch.delete_owned(&cf, key);
                    }
                }
            }
        }
        self.meta.write_batch(meta_batch)?;
        self.bulk.write_batch(bulk_batch)
    }

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        self.route(cf).count_prefix(cf, prefix)
    }

    fn sweep(
        &self,
        cf: &str,
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<store::KeyValue>, Option<Vec<u8>>)> {
        self.route(cf).sweep(cf, from, limit)
    }

    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<store::KeyValue>, Option<Vec<u8>>)> {
        self.route(cf).sweep_prefix(cf, prefix, from, limit)
    }

    // The awaited calls cannot go through `route`, whose answer is a `dyn Store`
    // and so carries none of them. One family names one half, so the branch is
    // the same routing written out.
    async fn get_wait(&self, cf: &str, key: &[u8]) -> Result<Option<Value>> {
        match self.is_bulk(cf) {
            true => self.bulk.get_wait(cf, key).await,
            false => self.meta.get_wait(cf, key).await,
        }
    }

    async fn get_many_wait(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        match self.is_bulk(cf) {
            true => self.bulk.get_many_wait(cf, keys).await,
            false => self.meta.get_many_wait(cf, keys).await,
        }
    }

    async fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> Result<Option<Value>> {
        match self.is_bulk(cf) {
            true => self.bulk.get_range_wait(cf, key, offset, len).await,
            false => self.meta.get_range_wait(cf, key, offset, len).await,
        }
    }

    async fn put_wait(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        match self.is_bulk(cf) {
            true => self.bulk.put_wait(cf, key, value).await,
            false => self.meta.put_wait(cf, key, value).await,
        }
    }

    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        self.route(cf).delete_range(cf, start, end)
    }

    fn iter(&self, cf: &str) -> Result<StoreIter<'_>> {
        self.route(cf).iter(cf)
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>> {
        self.route(cf).iter_prefix(cf, prefix)
    }

    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.route(cf).iter_keys_prefix(cf, prefix)
    }

    fn iter_from(&self, cf: &str, start: &[u8], way: Direction) -> Result<StoreIter<'_>> {
        self.route(cf).iter_from(cf, start, way)
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>> {
        self.route(cf).iter_range(cf, start, end)
    }

    fn actual_size_bytes(&self) -> Result<u64> {
        Ok(self
            .meta
            .actual_size_bytes()?
            .saturating_add(self.bulk.actual_size_bytes()?))
    }

    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        let meta = self.meta.available_disk_bytes()?;
        let bulk = self.bulk.available_disk_bytes()?;
        Ok([meta, bulk].into_iter().flatten().min())
    }

    fn live_data_size_bytes(&self) -> Result<Option<u64>> {
        let meta = self.meta.live_data_size_bytes()?;
        let bulk = self.bulk.live_data_size_bytes()?;
        Ok([meta, bulk]
            .into_iter()
            .flatten()
            .reduce(u64::saturating_add))
    }

    fn key_count_estimate(&self, cf: &str) -> Result<Option<u64>> {
        self.route(cf).key_count_estimate(cf)
    }

    fn reclaim_space(&self) -> Result<()> {
        self.meta.reclaim_space()?;
        self.bulk.reclaim_space()
    }

    fn cf_disk_usage(&self) -> Result<Vec<CfDiskUsage>> {
        let mut usage = Vec::new();
        for mut entry in self.meta.cf_disk_usage()? {
            entry.volume = StoreVolume::Primary;
            usage.push(entry);
        }
        for mut entry in self.bulk.cf_disk_usage()? {
            entry.volume = StoreVolume::Bulk;
            usage.push(entry);
        }
        Ok(usage)
    }

    fn disk_volumes(&self) -> Result<Vec<DiskVolume>> {
        Ok(vec![
            DiskVolume {
                volume: StoreVolume::Primary,
                used_bytes: self.meta.actual_size_bytes()?,
                free_bytes: self.meta.available_disk_bytes()?,
            },
            DiskVolume {
                volume: StoreVolume::Bulk,
                used_bytes: self.bulk.actual_size_bytes()?,
                free_bytes: self.bulk.available_disk_bytes()?,
            },
        ])
    }
}
