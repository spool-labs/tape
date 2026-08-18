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
//!   and the internal trait now answers the same one, so a read crosses without
//!   a copy at all: the store crate re-exports `reel_core::Value` rather than
//!   keeping a second value type beside it.
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

use reel::{
    ByteCount, CompactRate, IoBackend, MapShape, PointReads, Preallocate, ReelConfig, ReelStore,
    ShardShapes, SyncPolicy,
    MAP_EVERYTHING,
};
use reel_core::Store as ReelStoreTrait;
use tape_store::ops::SliceOps;
use tape_store::TapeStore;
use store::{
    CfDiskUsage, Direction, DiskVolume, Error as StoreError, Result as StoreResult, Store,
    StoreIter, StoreVolume, WriteBatch, Value};

pub use arm::{
    scaled, track_data_codec, BenchArm, SCALE_VAR, SEGMENT_MIB_VAR, TRACK_DATA_CODEC_VAR,
};
pub use columns::{RAW_TRACK_DATA_COLUMNS, TAPE_COLUMNS};
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
    /// Open a reel under this directory serving the given tape column families
    ///
    /// The set is a parameter rather than a constant because a run weighing a
    /// codec opens the same families twice and declares one of them both ways.
    /// Open the volume a node runs on, under the node's own config
    ///
    /// The one entry point that is not bench scoped. Everything else in this
    /// crate opens with `bench_config`, which turns durability off.
    pub fn open_node(
        root: impl AsRef<Path>,
        compaction_mbps: u64,
        sync_bytes: u64,
        backend: IoBackend,
    ) -> StoreResult<ReelBridge> {
        ReelBridge::open(
            root,
            node_config(compaction_mbps, sync_bytes, backend),
            TAPE_COLUMNS,
        )
    }

    pub fn open(
        root: impl AsRef<Path>,
        config: ReelConfig,
        columns: reel::ColumnSet,
    ) -> StoreResult<ReelBridge> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let inner = ReelStore::open(root.to_path_buf(), config, columns).map_err(engine)?;
        Ok(ReelBridge { inner })
    }

    /// The engine underneath, for a caller reading its counters
    pub fn engine(&self) -> &ReelStore {
        &self.inner
    }

    /// The shape each column's index actually took, beside the one it declared
    ///
    /// A declaration is a request the engine may decline, and a declined open
    /// shard is a tree that looks like one in the source and nowhere else. A run
    /// reporting a shape number says which it got rather than which it asked for.
    pub fn shapes(&self) -> Vec<(&'static str, MapShape, MapShape)> {
        let index = self.inner.index();
        let mut shapes = Vec::with_capacity(TAPE_COLUMNS.len());
        for spec in TAPE_COLUMNS {
            let Some(column) = index.column(spec.id) else {
                continue;
            };
            shapes.push((spec.name, spec.map_shape, column.map_shape()));
        }
        shapes
    }

    /// Every column whose index did not take the shape it declared
    pub fn declined_shapes(&self) -> Vec<&'static str> {
        let mut declined = Vec::new();
        for (name, asked, got) in self.shapes() {
            if asked != got {
                declined.push(name);
            }
        }
        declined
    }

    /// Drive every buffered append out to the filesystem
    ///
    /// The reel's answer to a RocksDB flush: what a bench calls between its write
    /// phase and its read phase so neither measures the other.
    pub fn flush(&self) -> StoreResult<()> {
        self.inner.flush().map_err(engine)
    }
}

/// The config a node opens its volume with
///
/// The bench config's siblings, minus everything that only makes sense when a
/// run is about to be thrown away: durability is a real policy rather than
/// `Never`, and the segment is the shipped size rather than one small enough
/// that a short run still rolls.
///
/// The knobs that are not the default are the ones this campaign measured.
/// `map_above` puts warm reads on the mapped path instead of a door round trip,
/// and `point_reads` asks the page cache before queueing, which is what keeps a
/// cold read on a path that can report an error rather than raising SIGBUS.
///
/// Durability is `sync_bytes` rather than a sync per put: a node writing slices
/// in batches wants one sync per drain, and what a crash risks is the tail.
/// Nothing here is measured yet; every read figure in this campaign was taken
/// under `SyncPolicy::Never` and the write path has no numbers at all.
pub fn node_config(compaction_mbps: u64, sync_bytes: u64, backend: IoBackend) -> ReelConfig {
    ReelConfig {
        sync: match sync_bytes {
            0 => SyncPolicy::EveryPut,
            bytes => SyncPolicy::Bytes(ByteCount::from_bytes(bytes)),
        },
        compact_mbps: match compaction_mbps {
            0 => CompactRate::Auto,
            capped => CompactRate::Mbps(capped),
        },
        // No mapping. It is worth 3.3x on a warm blocking single read, and it
        // turns a bad sector into SIGBUS and a dead process where the door
        // returns an error the node can act on. Warm blocking singles are a thin
        // slice of an io-bound workload on 64 TB against 64 GB of cache, and the
        // fleet's disks grow bad sectors, so the latency is the cheaper thing to
        // give up. The mapped path stays a bench and tooling knob.
        map_above: None,
        point_reads: probe_for(backend),
        io_backend: backend,
        shard_shapes: ShardShapes::Declared,
        ..ReelConfig::default()
    }
}

/// Whether to ask the page cache before queueing a read, which only a ring wants
///
/// A ring read is a submit and a wait, and the probe recovers that: 2.60 us down
/// to 1.91 on a warm blocking single. A posix read is already answered inline by
/// `submit_inline`, so there is no round trip to skip and the probe is one
/// wasted `preadv2` per read. Coupled here so the pairing is one line rather
/// than something to re-learn.
fn probe_for(backend: IoBackend) -> PointReads {
    match backend {
        IoBackend::Uring => PointReads::Probed,
        IoBackend::Posix | IoBackend::UringDirect => PointReads::Queued,
    }
}

/// The store a node runs, every tape family on one reel volume
///
/// This lives here rather than beside the other `TapeStore` constructors
/// because the bridge sits above `tape-store` in the graph: it needs the tape
/// column declarations, so `tape-store` cannot name it back. Promotion means
/// the node calls this instead of `open_primary_split`.
pub fn open_node_store(
    root: impl AsRef<Path>,
    compaction_mbps: u64,
    sync_bytes: u64,
    backend: IoBackend,
) -> StoreResult<TapeStore<ReelBridge>> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    let store = TapeStore::new(ReelBridge::open_node(
        root,
        compaction_mbps,
        sync_bytes,
        backend,
    )?);

    // A volume written before the size index existed reports no slice totals
    // until the index is laid down.
    SliceOps::ensure_slice_size_index(&store)
        .map_err(|error| StoreError::Database(error.to_string()))?;
    Ok(store)
}

/// A config sized for a bench rather than for a node
///
/// Segments are small enough that a bench writing a few GiB still seals and
/// reopens several of them, and space is reserved a chunk ahead rather than a
/// whole segment at a time, so a case that writes little is not charged for a
/// segment it never fills. Syncing is left to the caller's flush, matching a
/// RocksDB arm that does not fsync per write either.
pub fn bench_config(segment_bytes: u64) -> ReelConfig {
    // Printed rather than assumed: a bench that cannot say which config it
    // opened with cannot tell a real result from a stale binary.
    eprintln!(
        "bench_config segment={segment_bytes} alloc_chunk={} map_above={:?} shapes={:?}",
        alloc_chunk_bytes().min(segment_bytes), MAP_EVERYTHING, ShardShapes::Declared,
    );
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(segment_bytes),
        // A small segment cannot reserve a chunk larger than itself, and a run
        // sweeping segment sizes has no reason to know that. Overridable, since
        // the reservation is the difference between a segment's file size and
        // what it holds.
        alloc_chunk: ByteCount::from_bytes(alloc_chunk_bytes().min(segment_bytes)),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        // Without this the engine drops every open-shard request a column makes
        // and hands back a tree, so a run measuring the shape would measure the
        // default and never say so.
        shard_shapes: ShardShapes::Declared,
        // The mapped read path is gated on this and the default forbids it, so
        // every warm read pays a door round trip it does not need.
        map_above: MAP_EVERYTHING,
        // Ask the page cache before queueing a read. Worthless on a direct
        // plane, which has no cache to ask, and nearly free where it loses on a
        // buffered one: a cold probe costs one nowait syscall against a seek,
        // and a warm one skips the door entirely.
        point_reads: PointReads::Probed,
        ..ReelConfig::default()
    }
}

/// Environment variable naming the allocation chunk in MiB
pub const ALLOC_MIB_VAR: &str = "TAPE_BENCH_ALLOC_MIB";

/// The allocation chunk a bench arm reserves ahead in, the shipped one unless asked
fn alloc_chunk_bytes() -> u64 {
    match std::env::var(ALLOC_MIB_VAR).ok().and_then(|value| value.parse::<u64>().ok()) {
        Some(mib) => mib * 1024 * 1024,
        None => ReelConfig::default().alloc_chunk.to_bytes(),
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
    fn get(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        ReelStoreTrait::get(&self.inner, cf, key)
            
            .map_err(crossed)
    }

    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        ReelStoreTrait::get_many(&self.inner, cf, keys).map_err(crossed)
    }

    async fn get_wait(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        ReelStoreTrait::get_wait(&self.inner, cf, key)
            .await
            
            .map_err(crossed)
    }

    async fn get_many_wait(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        ReelStoreTrait::get_many_wait(&self.inner, cf, keys)
            .await
            .map_err(crossed)
    }

    fn get_range(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        ReelStoreTrait::get_range(&self.inner, cf, key, offset, len)
            
            .map_err(crossed)
    }

    async fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        ReelStoreTrait::get_range_wait(&self.inner, cf, key, offset, len)
            .await
            
            .map_err(crossed)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        ReelStoreTrait::put(&self.inner, cf, key, value).map_err(crossed)
    }

    async fn put_wait(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        ReelStoreTrait::put_wait(&self.inner, cf, key, value)
            .await
            .map_err(crossed)
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

    async fn write_batch_wait(&self, staged: WriteBatch) -> StoreResult<()> {
        ReelStoreTrait::write_batch_wait(&self.inner, batch(staged))
            .await
            .map_err(crossed)
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

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<u64> {
        ReelStoreTrait::count_prefix(&self.inner, cf, prefix).map_err(crossed)
    }

    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<(Vec<store::KeyValue>, Option<Vec<u8>>)> {
        let (rows, next) =
            ReelStoreTrait::sweep_prefix(&self.inner, cf, prefix, from, limit).map_err(crossed)?;
        let mut owned = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            owned.push((key, value.into_vec()));
        }
        Ok((owned, next))
    }

    fn sweep(
        &self,
        cf: &str,
        from: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<(Vec<store::KeyValue>, Option<Vec<u8>>)> {
        let (rows, next) = ReelStoreTrait::sweep(&self.inner, cf, from, limit).map_err(crossed)?;
        let mut owned = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            owned.push((key, value.into_vec()));
        }
        Ok((owned, next))
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
