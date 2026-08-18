//! The internal store trait served by the public reel engine, for benches
//!
//! The internal workspace benches the `store::Store` trait; the public reel at
//! `tape-public/reel` implements a vendored copy of that trait, `reel_core::Store`,
//! which has since moved on. This crate is the bench-scoped adapter between the
//! two, so a `TapeStore` built on the internal trait can run its workload against
//! the reel that shipped rather than against the internal fork of it.
//!
//! Three divergences are bridged here and nowhere else:
//!
//! - A read answers `reel_core::Value`, a handle over the buffer the read used,
//!   where the internal trait answers `Vec<u8>`. `Value::into_vec` moves the
//!   buffer out when the value owns it and copies when it is a window into a
//!   shared block, which is the read-side cost of the bridge.
//! - A batch names its family by `Cow<'static, str>` rather than `String`, and
//!   is consumed rather than iterated by reference. The internal `WriteBatch`
//!   grew an `IntoIterator` so the bridge hands payloads over instead of cloning
//!   them; a clone there would put a memcpy of every slice on the write bench.
//! - The two `Error`, `CfDiskUsage`, `DiskVolume` and `StoreVolume` types are
//!   structurally identical and nominally distinct, so each crosses by hand.
//!
//! What is not bridged: the reel's newer reads (`get_many`, `get_range`, the
//! awaited twins, `walk_from`, `count_prefix`, `maintain`) have no caller on the
//! internal trait, so a bench routed through this bridge exercises the engine's
//! blocking one-at-a-time paths only.
//!
//! The rocks arm's configuration lives here too, in `rocks`. The fleet's is
//! sized for spinning disks behind small memory, and a baseline opened under
//! those ceilings would report the ceilings rather than the engine, so the arm
//! opens with sizing fit for the bench box and with every knob that has an
//! opposite number on the reel side set to match it.

mod arm;
mod columns;
mod rocks;
mod split;

use std::path::Path;

use reel::{ByteCount, Preallocate, ReelConfig, ReelStore, SyncPolicy};
use reel_core::Store as ReelStoreTrait;
use store::{
    CfDiskUsage, Direction, DiskVolume, Error as StoreError, Result as StoreResult, Store,
    StoreIter, StoreVolume, WriteBatch,
};

pub use arm::{scaled, BenchArm, SCALE_VAR, SEGMENT_MIB_VAR};
pub use columns::TAPE_COLUMNS;
pub use rocks::{
    bench_bulk_configs, bench_cache, bench_db_options, bench_metadata_configs, bench_store_configs,
    open_bench_split, CACHE_BYTES,
};
pub use split::{MetaBulkStore, REEL_SUBDIR};

/// The public reel engine behind the internal store trait
pub struct ReelBridge {
    inner: ReelStore,
}

impl ReelBridge {
    /// Open a reel under this directory serving every tape column family
    pub fn open(root: impl AsRef<Path>, config: ReelConfig) -> StoreResult<ReelBridge> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let inner = ReelStore::open(root.to_path_buf(), config, TAPE_COLUMNS).map_err(engine)?;
        Ok(ReelBridge { inner })
    }

    /// The engine underneath, for a caller reading its counters
    pub fn engine(&self) -> &ReelStore {
        &self.inner
    }

    /// Drive every buffered append out to the filesystem
    ///
    /// The reel's answer to a RocksDB flush: what a bench calls between its write
    /// phase and its read phase so neither measures the other.
    pub fn flush(&self) -> StoreResult<()> {
        self.inner.flush().map_err(engine)
    }
}

/// A config sized for a bench rather than for a node
///
/// Segments are small enough that a bench writing a few GiB still seals and
/// reopens several of them, and space is reserved a chunk ahead rather than a
/// whole segment at a time, so a case that writes little is not charged for a
/// segment it never fills. Syncing is left to the caller's flush, matching a
/// RocksDB arm that does not fsync per write either.
pub fn bench_config(segment_bytes: u64) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(segment_bytes),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        ..ReelConfig::default()
    }
}

/// Whatever the engine said, as the internal trait's error
fn engine(error: reel::ReelError) -> StoreError {
    StoreError::Database(error.to_string())
}

/// Whatever the vendored trait said, as the internal one's error
fn crossed(error: reel_core::Error) -> StoreError {
    match error {
        reel_core::Error::Database(message) => StoreError::Database(message),
        reel_core::Error::NotFound => StoreError::NotFound,
        reel_core::Error::ColumnFamilyNotFound(cf) => StoreError::ColumnFamilyNotFound(cf),
        reel_core::Error::Serialization(message) => StoreError::Serialization(message),
        reel_core::Error::Io(error) => StoreError::Io(error),
    }
}

/// The volume a usage report is tagged with, across the two copies of the enum
fn volume(volume: reel_core::StoreVolume) -> StoreVolume {
    match volume {
        reel_core::StoreVolume::Primary => StoreVolume::Primary,
        reel_core::StoreVolume::Bulk => StoreVolume::Bulk,
    }
}

/// One row of a per-family usage report, across the two copies of the struct
fn cf_usage(usage: reel_core::CfDiskUsage) -> CfDiskUsage {
    CfDiskUsage {
        cf: usage.cf,
        volume: volume(usage.volume),
        sst_bytes: usage.sst_bytes,
        blob_bytes: usage.blob_bytes,
        num_keys: usage.num_keys,
    }
}

/// One row of a per-device usage report, across the two copies of the struct
fn disk_volume(disk: reel_core::DiskVolume) -> DiskVolume {
    DiskVolume {
        volume: volume(disk.volume),
        used_bytes: disk.used_bytes,
        free_bytes: disk.free_bytes,
    }
}

/// The internal trait's direction, as the vendored copy spells it
fn direction(direction: Direction) -> reel_core::Direction {
    match direction {
        Direction::Asc => reel_core::Direction::Asc,
        Direction::Desc => reel_core::Direction::Desc,
    }
}

/// The batch the reel takes, from the batch the internal trait staged
///
/// Consuming rather than borrowing, so a staged payload moves into the reel's
/// batch rather than being copied into it.
fn batch(batch: WriteBatch) -> reel_core::WriteBatch {
    let mut crossed = reel_core::WriteBatch::new();
    for op in batch {
        match op {
            store::BatchOp::Put { cf, key, value } => crossed.put_named(cf.into(), key, value),
            store::BatchOp::Delete { cf, key } => crossed.delete_named(cf.into(), key),
        }
    }
    crossed
}

/// Rows the vendored trait lends, as rows the internal one owns
fn rows(iter: reel_core::StoreIter<'_>) -> StoreIter<'_> {
    Box::new(iter.map(|(key, value)| (key, value.into_vec())))
}


impl Store for ReelBridge {
    fn get(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Vec<u8>>> {
        ReelStoreTrait::get(&self.inner, cf, key)
            .map(|value| value.map(reel_core::Value::into_vec))
            .map_err(crossed)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        ReelStoreTrait::put(&self.inner, cf, key, value).map_err(crossed)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> StoreResult<()> {
        ReelStoreTrait::delete(&self.inner, cf, key).map_err(crossed)
    }

    fn contains(&self, cf: &str, key: &[u8]) -> StoreResult<bool> {
        ReelStoreTrait::contains(&self.inner, cf, key).map_err(crossed)
    }

    fn write_batch(&self, staged: WriteBatch) -> StoreResult<()> {
        ReelStoreTrait::write_batch(&self.inner, batch(staged)).map_err(crossed)
    }


    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<()> {
        ReelStoreTrait::delete_range(&self.inner, cf, start, end).map_err(crossed)
    }

    fn iter(&self, cf: &str) -> StoreResult<StoreIter<'_>> {
        ReelStoreTrait::iter(&self.inner, cf)
            .map(rows)
            .map_err(crossed)
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<StoreIter<'_>> {
        ReelStoreTrait::iter_prefix(&self.inner, cf, prefix)
            .map(rows)
            .map_err(crossed)
    }

    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
        ReelStoreTrait::iter_keys_prefix(&self.inner, cf, prefix).map_err(crossed)
    }

    fn iter_from(&self, cf: &str, start: &[u8], way: Direction) -> StoreResult<StoreIter<'_>> {
        ReelStoreTrait::iter_from(&self.inner, cf, start, direction(way))
            .map(rows)
            .map_err(crossed)
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<StoreIter<'_>> {
        ReelStoreTrait::iter_range(&self.inner, cf, start, end)
            .map(rows)
            .map_err(crossed)
    }

    fn actual_size_bytes(&self) -> StoreResult<u64> {
        ReelStoreTrait::actual_size_bytes(&self.inner).map_err(crossed)
    }

    fn available_disk_bytes(&self) -> StoreResult<Option<u64>> {
        ReelStoreTrait::available_disk_bytes(&self.inner).map_err(crossed)
    }

    fn live_data_size_bytes(&self) -> StoreResult<Option<u64>> {
        ReelStoreTrait::live_data_size_bytes(&self.inner).map_err(crossed)
    }

    fn key_count_estimate(&self, cf: &str) -> StoreResult<Option<u64>> {
        ReelStoreTrait::key_count_estimate(&self.inner, cf).map_err(crossed)
    }

    fn cf_disk_usage(&self) -> StoreResult<Vec<CfDiskUsage>> {
        ReelStoreTrait::cf_disk_usage(&self.inner)
            .map(|usage| usage.into_iter().map(cf_usage).collect())
            .map_err(crossed)
    }

    fn reclaim_space(&self) -> StoreResult<()> {
        ReelStoreTrait::reclaim_space(&self.inner).map_err(crossed)
    }

    fn disk_volumes(&self) -> StoreResult<Vec<DiskVolume>> {
        ReelStoreTrait::disk_volumes(&self.inner)
            .map(|volumes| volumes.into_iter().map(disk_volume).collect())
            .map_err(crossed)
    }
}
