//! Two-instance RocksDB store: a fast metadata tier and a large bulk tier
//!
//! Storage nodes often have a small fast device and a large slow device. This
//! store keeps small hot metadata on one instance and bulk slice data on the
//! other, routing each operation to the owning instance by column family. The
//! split stays behind the store trait, so callers still address it by column
//! family and key.
//!
//! Pointing both instances at the same device gives the old single-device
//! layout, so a one-drive box needs no special handling.

use store::{BatchOp, DiskTier, Direction, Result, Store, StoreIter, StoreTier, WriteBatch};

use crate::RocksStore;

/// Store backed by two RocksDB instances split by column family
///
/// The named bulk column families are served by the bulk store; everything
/// else by the metadata store.
pub struct SplitStore {
    meta: RocksStore,
    bulk: RocksStore,
    bulk_cfs: Vec<String>,
}

impl SplitStore {
    /// Build a split store from an opened metadata store and bulk store
    ///
    /// The bulk column families must match the ones the bulk store was opened
    /// with; every other column family must exist in the metadata store. The
    /// list is tiny, so routing scans it linearly.
    pub fn new(meta: RocksStore, bulk: RocksStore, bulk_cfs: Vec<String>) -> Self {
        Self {
            meta,
            bulk,
            bulk_cfs,
        }
    }

    /// The metadata (fast tier) store
    pub fn meta(&self) -> &RocksStore {
        &self.meta
    }

    /// The bulk (large tier) store
    pub fn bulk(&self) -> &RocksStore {
        &self.bulk
    }

    fn is_bulk(&self, cf: &str) -> bool {
        self.bulk_cfs.iter().any(|name| name == cf)
    }

    fn route(&self, cf: &str) -> &RocksStore {
        if self.is_bulk(cf) {
            &self.bulk
        } else {
            &self.meta
        }
    }

    /// Flush both instances
    pub fn flush(&self) -> Result<()> {
        self.meta.flush()?;
        self.bulk.flush()
    }

    /// Catch both secondary instances up with their primaries
    pub fn catch_up_with_primary(&self) -> Result<()> {
        self.meta.catch_up_with_primary()?;
        self.bulk.catch_up_with_primary()
    }
}

impl Store for SplitStore {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.route(cf).get(cf, key)
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

        let mut meta_batch = WriteBatch::new();
        let mut bulk_batch = WriteBatch::new();

        for op in batch.iter() {
            match op {
                BatchOp::Put { cf, key, value } => {
                    if self.is_bulk(cf) {
                        bulk_batch.put(cf, key, value);
                    } else {
                        meta_batch.put(cf, key, value);
                    }
                }
                BatchOp::Delete { cf, key } => {
                    if self.is_bulk(cf) {
                        bulk_batch.delete(cf, key);
                    } else {
                        meta_batch.delete(cf, key);
                    }
                }
            }
        }

        // Nothing writes a batch spanning both tiers, and two instances cannot
        // commit one atomically; catch such a regression in debug builds.
        debug_assert!(
            meta_batch.is_empty() || bulk_batch.is_empty(),
            "write_batch spans both store tiers; cross-tier batches are not atomic"
        );

        if !meta_batch.is_empty() {
            self.meta.write_batch(meta_batch)?;
        }
        if !bulk_batch.is_empty() {
            self.bulk.write_batch(bulk_batch)?;
        }
        Ok(())
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

    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>> {
        self.route(cf).iter_from(cf, start, direction)
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
        // The tighter of the two tiers; ignores a tier that reports no figure.
        Ok([meta, bulk].into_iter().flatten().min())
    }

    fn reclaim_space(&self) -> Result<()> {
        self.meta.reclaim_space()?;
        self.bulk.reclaim_space()
    }

    fn disk_tiers(&self) -> Result<Vec<DiskTier>> {
        Ok(vec![
            DiskTier {
                tier: StoreTier::Primary,
                used_bytes: self.meta.actual_size_bytes()?,
                free_bytes: self.meta.available_disk_bytes()?,
            },
            DiskTier {
                tier: StoreTier::Bulk,
                used_bytes: self.bulk.actual_size_bytes()?,
                free_bytes: self.bulk.available_disk_bytes()?,
            },
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ColumnFamilyConfig;
    use crate::Options;
    use tempfile::tempdir;

    fn open(dir: &std::path::Path, cfs: &[&str]) -> RocksStore {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        let cf_configs = cfs
            .iter()
            .map(|name| ColumnFamilyConfig::new(*name).with_block_based().build())
            .collect();
        RocksStore::open_with_cf_config(dir, db_opts, cf_configs).unwrap()
    }

    fn split(meta_dir: &std::path::Path, bulk_dir: &std::path::Path) -> SplitStore {
        let meta = open(meta_dir, &["meta"]);
        let bulk = open(bulk_dir, &["slice"]);
        SplitStore::new(meta, bulk, vec!["slice".to_string()])
    }

    // reads and writes land in the column family's owning instance
    #[test]
    fn routes_by_cf() {
        let dir = tempdir().unwrap();
        let store = split(&dir.path().join("meta"), &dir.path().join("bulk"));

        store.put("meta", b"k", b"m").unwrap();
        store.put("slice", b"k", b"s").unwrap();

        // Each value is only visible through its owning instance.
        assert_eq!(store.get("meta", b"k").unwrap(), Some(b"m".to_vec()));
        assert_eq!(store.get("slice", b"k").unwrap(), Some(b"s".to_vec()));
        assert_eq!(store.meta().get("meta", b"k").unwrap(), Some(b"m".to_vec()));
        assert_eq!(store.bulk().get("slice", b"k").unwrap(), Some(b"s".to_vec()));
        assert!(store.meta().get("slice", b"k").is_err());
        assert!(store.bulk().get("meta", b"k").is_err());
    }

    // a single-tier batch commits to that tier only
    #[test]
    fn batch_routing() {
        let dir = tempdir().unwrap();
        let store = split(&dir.path().join("meta"), &dir.path().join("bulk"));

        // Each batch stays within one tier, as all production batches do.
        let mut meta_batch = WriteBatch::new();
        meta_batch.put("meta", b"a", b"1");
        meta_batch.delete("meta", b"gone");
        store.write_batch(meta_batch).unwrap();

        let mut bulk_batch = WriteBatch::new();
        bulk_batch.put("slice", b"b", b"2");
        store.write_batch(bulk_batch).unwrap();

        assert_eq!(store.get("meta", b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get("slice", b"b").unwrap(), Some(b"2".to_vec()));
        // The bulk batch did not leak into the metadata tier.
        assert_eq!(store.meta().get("meta", b"b").unwrap(), None);
    }

    // disk usage is reported as a primary and a bulk tier
    #[test]
    fn reports_two_tiers() {
        let dir = tempdir().unwrap();
        let store = split(&dir.path().join("meta"), &dir.path().join("bulk"));

        let tiers = store.disk_tiers().unwrap();
        assert_eq!(tiers.len(), 2);
        assert_eq!(tiers[0].tier, StoreTier::Primary);
        assert_eq!(tiers[1].tier, StoreTier::Bulk);
    }

    // iteration is scoped to the owning instance
    #[test]
    fn iterates() {
        let dir = tempdir().unwrap();
        let store = split(&dir.path().join("meta"), &dir.path().join("bulk"));
        store.put("slice", b"a", b"1").unwrap();
        store.put("slice", b"b", b"2").unwrap();

        let entries: Vec<_> = store.iter("slice").unwrap().collect();
        assert_eq!(entries.len(), 2);
    }
}
