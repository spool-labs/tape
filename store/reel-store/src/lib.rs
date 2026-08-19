//! The node's store: tapedrive's columns on the reel engine
//!
//! Every family a tape store addresses, declared here and served by one reel
//! volume. A node opens through `open_node_store`, an offline tool through
//! `open_node_store_read_only`, an e2e harness through `open_harness_store`;
//! the benches open through the rest.
//!
//! The reel implements `reel_core::Store`, a vendored copy of the internal
//! `store::Store` that has since moved on, so three divergences cross here and
//! nowhere else:
//!
//! - A read answers `reel_core::Value`, a handle over the buffer the read used,
//!   and the internal trait now answers the same one, so a read crosses without
//!   a copy at all: the store crate re-exports `reel_core::Value` rather than
//!   keeping a second value type beside it.
//! - A batch names its family by `Cow<'static, str>` rather than `String`, and
//!   is consumed rather than iterated by reference. The internal `WriteBatch`
//!   grew an `IntoIterator` so a staged payload moves across instead of being
//!   cloned; a clone there would put a memcpy of every slice on the write path.
//! - The two `Error`, `CfDiskUsage`, `DiskVolume` and `StoreVolume` types are
//!   structurally identical and nominally distinct, so each crosses by hand.
//!
//! What does not cross: the reel's `walk_from` and `maintain` have no caller on
//! the internal trait.
//!
//! The rocks arm's configuration lives here too, in `rocks`. The fleet's is
//! sized for spinning disks behind small memory, and a baseline opened under
//! those ceilings would report the ceilings rather than the engine, so the arm
//! opens with sizing fit for the bench box and with every knob that has an
//! opposite number on the reel side set to match it.

#[cfg(feature = "rocks")]
mod arm;
mod columns;
pub mod fill;
#[cfg(target_os = "linux")]
pub mod written;
#[cfg(feature = "rocks")]
mod rocks;
#[cfg(feature = "rocks")]
mod split;

use std::path::Path;

use reel::{
    ByteCount, CompactRate, IoBackend, MapShape, PointReads, Preallocate, ReelConfig,
    ReelStore as EngineStore, ShardShapes, SyncPolicy, ThreadBudget,
    MAP_EVERYTHING,
};
use reel_core::Store as EngineStoreTrait;
use tape_store::TapeStore;
use store::{
    CfDiskUsage, Direction, DiskVolume, Error as StoreError, Result as StoreResult, Store,
    StoreIter, StoreVolume, WriteBatch, Value};

#[cfg(feature = "rocks")]
pub use arm::{
    scaled, track_data_codec, BenchArm, SCALE_VAR, SEGMENT_MIB_VAR, TRACK_DATA_CODEC_VAR,
};
pub use columns::{RAW_TRACK_DATA_COLUMNS, TAPE_COLUMNS};
// Re-exported so a tool opening a node's volume needs this crate and not the
// engine behind it.
pub use reel::IndexResidency;
#[cfg(feature = "rocks")]
pub use rocks::{
    bench_bulk_configs, bench_cache, bench_db_options, bench_metadata_configs, bench_store_configs,
    open_bench_split, CACHE_BYTES,
};
#[cfg(feature = "rocks")]
pub use split::{MetaBulkStore, REEL_SUBDIR};

/// The public reel engine behind the internal store trait
pub struct ReelStore {
    inner: EngineStore,
}

/// What one column's index holds
pub struct ResidentColumn {
    /// The column, as the volume was opened with it
    pub column: String,

    /// Live records the index counts, absent on a paged open
    pub records: Option<u64>,

    /// Live bytes the index counts, absent on a paged open
    pub bytes: Option<u64>,

    /// Keys the index holds in memory
    pub keys: u64,
}

/// What a whole index holds
pub struct IndexReport {
    /// Bytes the index accounts to itself
    pub resident_bytes: u64,

    /// One row per column the volume was opened over
    pub columns: Vec<ResidentColumn>,
}

impl ReelStore {
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
    ) -> StoreResult<ReelStore> {
        ReelStore::open(
            root,
            node_config(compaction_mbps, sync_bytes, backend),
            TAPE_COLUMNS,
        )
    }

    pub fn open(
        root: impl AsRef<Path>,
        config: ReelConfig,
        columns: reel::ColumnSet,
    ) -> StoreResult<ReelStore> {
        let root = root.as_ref();
        std::fs::create_dir_all(root)?;
        let inner = EngineStore::open(root.to_path_buf(), config, columns).map_err(engine)?;
        Ok(ReelStore { inner })
    }

    /// Open an existing volume read-only, leaving its ownership lock alone
    ///
    /// No directory is created: a path with no volume under it is a mistyped
    /// path, and answering it with an empty store would report a node holding
    /// nothing.
    pub fn open_read_only(
        root: impl AsRef<Path>,
        config: ReelConfig,
        columns: reel::ColumnSet,
    ) -> StoreResult<ReelStore> {
        let root = root.as_ref();
        let inner =
            EngineStore::open_read_only(root.to_path_buf(), config, columns).map_err(engine)?;
        Ok(ReelStore { inner })
    }

    /// The engine underneath, for a caller reading its counters
    pub fn engine(&self) -> &EngineStore {
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

    /// What the engine's index holds, column by column
    ///
    /// The engine's own report layer, for a bench weighing what a column costs in
    /// memory rather than on disk. A paged open counts only the keys it holds, so
    /// the record and byte cells go unanswered there rather than reporting a
    /// fraction of the column as the whole.
    pub fn index_report(&self) -> IndexReport {
        let index = self.inner.index();
        let keys = index.lead_tie_rates();
        let stat = reel::report::stat::stat(&self.inner);

        let mut columns = Vec::with_capacity(stat.columns.len());
        for column in stat.columns {
            let resident = keys
                .iter()
                .find(|(id, _, _)| id.as_u8() == column.id)
                .map_or(0, |(_, _, keys)| *keys);
            columns.push(ResidentColumn {
                column: column.column,
                records: column.records,
                bytes: column.bytes,
                keys: resident,
            });
        }
        IndexReport { resident_bytes: index.resident_bytes().to_bytes(), columns }
    }

    /// Drive every buffered append out to the filesystem
    ///
    /// The reel's answer to a RocksDB flush: what a bench calls between its write
    /// phase and its read phase so neither measures the other.
    pub fn flush(&self) -> StoreResult<()> {
        self.inner.flush().map_err(engine)
    }
}

/// Bytes a node writes between durability syncs
///
/// The HDD battery's answer: syncing every 16 MiB costs 1.09-1.46x the latency
/// of never syncing and writes zero extra bytes, where a sync per put costs
/// 16-67x. A node writing slices in batches wants one sync per drain, and what a
/// crash risks is the tail.
pub const DEFAULT_SYNC_BYTES: u64 = 16 * 1024 * 1024;

/// The file backend a node opens its volume with
///
/// A ring wherever one can exist. `select_backend` downgrades to posix with a
/// warning where the ring cannot be set up, so naming it here costs nothing on a
/// kernel that has none, and `get_many` depth is worth 3.25x on the ring against
/// flat on posix.
pub const fn default_backend() -> IoBackend {
    match cfg!(target_os = "linux") {
        true => IoBackend::Uring,
        false => IoBackend::Posix,
    }
}

/// The config a node opens its volume with
///
/// The bench config's siblings, minus everything that only makes sense when a
/// run is about to be thrown away: durability is a real policy rather than
/// `Never`, and the segment is the shipped size rather than one small enough
/// that a short run still rolls.
///
/// The knobs that are not the engine default are the ones the HDD battery
/// settled, each with its own line below.
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
        // returns an error the node can act on. A mapped cold read also pulls 5x
        // the device bytes a door read pulls, against a spindle answering in
        // ~7.8 ms. Warm blocking singles are a thin slice of an io-bound
        // workload on 64 TB against 64 GB of cache, so the latency is the
        // cheaper thing to give up. The mapped path stays a bench and tooling
        // knob.
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
/// because this crate sits above `tape-store` in the graph: it needs the tape
/// column declarations, so `tape-store` cannot name it back.
pub fn open_node_store(
    root: impl AsRef<Path>,
    compaction_mbps: u64,
    sync_bytes: u64,
    backend: IoBackend,
) -> StoreResult<TapeStore<ReelStore>> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    Ok(TapeStore::new(ReelStore::open_node(
        root,
        compaction_mbps,
        sync_bytes,
        backend,
    )?))
}

/// The config an offline tool reads a node's volume under
///
/// The node's own shape declarations, because a column read under a different
/// declaration is a column the engine refuses to serve. Durability and the
/// compaction ceiling are the node's and irrelevant here: nothing writes.
/// Residency is the caller's, since it is the one thing a reader trades:
/// resident answers what a column holds, paged answers which sealed segments
/// stand over it and fits a volume larger than the memory reading it.
pub fn read_only_config(residency: IndexResidency) -> ReelConfig {
    ReelConfig {
        index: residency,
        // The engine refuses an open shard under a paged walk, and `track_data`
        // declares one, so a paged read takes every column as a tree instead.
        // The shape is how this open builds its index, not how the volume was
        // written, so nothing on disk cares which one was asked for.
        shard_shapes: match residency {
            IndexResidency::Resident => ShardShapes::Declared,
            _ => ShardShapes::Tree,
        },
        ..node_config(0, DEFAULT_SYNC_BYTES, default_backend())
    }
}

/// A tape store over a node's volume, read-only and without its lock
///
/// What every offline tool opens: it reads beside a running node rather than
/// waiting for one to stop.
pub fn open_node_store_read_only(
    root: impl AsRef<Path>,
    residency: IndexResidency,
) -> StoreResult<TapeStore<ReelStore>> {
    Ok(TapeStore::new(ReelStore::open_read_only(
        root,
        read_only_config(residency),
        TAPE_COLUMNS,
    )?))
}

/// Bytes a harness volume seals a segment at
const HARNESS_SEGMENT_BYTES: u64 = 32 * 1024 * 1024;

/// Bytes a harness volume reserves ahead of its write head
const HARNESS_ALLOC_CHUNK: u64 = 4 * 1024 * 1024;

/// The node's own policy at a size a throwaway volume can afford
///
/// Every knob the fleet ships, at a segment an e2e node can fill: the shipped
/// segment is preallocated whole, and twenty-five nodes reserving a gibibyte
/// each is the size of the run rather than the size of its data. One tail for
/// the same reason, since a tail costs a reserved segment.
pub fn harness_config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(HARNESS_SEGMENT_BYTES),
        alloc_chunk: ByteCount::from_bytes(HARNESS_ALLOC_CHUNK),
        preallocate: Preallocate::Chunk,
        active_tails: ThreadBudget::threads(1),
        ..node_config(0, DEFAULT_SYNC_BYTES, default_backend())
    }
}

/// A tape store on a harness volume, every family the node addresses
pub fn open_harness_store(root: impl AsRef<Path>) -> StoreResult<TapeStore<ReelStore>> {
    Ok(TapeStore::new(ReelStore::open(
        root,
        harness_config(),
        TAPE_COLUMNS,
    )?))
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
        "bench_config segment={segment_bytes} alloc_chunk={} sync={:?} map_above={:?} shapes={:?}",
        alloc_chunk_bytes().min(segment_bytes),
        sync_policy(),
        MAP_EVERYTHING,
        ShardShapes::Declared,
    );
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(segment_bytes),
        // A small segment cannot reserve a chunk larger than itself, and a run
        // sweeping segment sizes has no reason to know that. Overridable, since
        // the reservation is the difference between a segment's file size and
        // what it holds.
        alloc_chunk: ByteCount::from_bytes(alloc_chunk_bytes().min(segment_bytes)),
        preallocate: Preallocate::Chunk,
        sync: sync_policy(),
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

/// Environment variable naming the sync policy: `never`, `everyput`, or bytes
pub const SYNC_VAR: &str = "TAPE_BENCH_SYNC";

/// The durability a bench arm runs under, `Never` unless asked
///
/// `Never` is a control column, not an operating mode: it says what the write
/// path costs with the syncs taken out, and every figure taken under it owes a
/// durable one beside it before anything is concluded about a node.
fn sync_policy() -> SyncPolicy {
    match std::env::var(SYNC_VAR).ok().as_deref() {
        None | Some("never") => SyncPolicy::Never,
        Some("everyput") => SyncPolicy::EveryPut,
        Some(bytes) => match bytes.parse::<u64>() {
            Ok(bytes) => SyncPolicy::Bytes(ByteCount::from_bytes(bytes)),
            Err(_) => panic!("{SYNC_VAR} is `never`, `everyput`, or a byte count"),
        },
    }
}

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


impl Store for ReelStore {
    fn get(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        EngineStoreTrait::get(&self.inner, cf, key)
            
            .map_err(crossed)
    }

    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        EngineStoreTrait::get_many(&self.inner, cf, keys).map_err(crossed)
    }

    async fn get_wait(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        EngineStoreTrait::get_wait(&self.inner, cf, key)
            .await
            
            .map_err(crossed)
    }

    async fn get_many_wait(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        EngineStoreTrait::get_many_wait(&self.inner, cf, keys)
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
        EngineStoreTrait::get_range(&self.inner, cf, key, offset, len)
            
            .map_err(crossed)
    }

    async fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        EngineStoreTrait::get_range_wait(&self.inner, cf, key, offset, len)
            .await
            
            .map_err(crossed)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        EngineStoreTrait::put(&self.inner, cf, key, value).map_err(crossed)
    }

    async fn put_wait(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        EngineStoreTrait::put_wait(&self.inner, cf, key, value)
            .await
            .map_err(crossed)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> StoreResult<()> {
        EngineStoreTrait::delete(&self.inner, cf, key).map_err(crossed)
    }

    fn contains(&self, cf: &str, key: &[u8]) -> StoreResult<bool> {
        EngineStoreTrait::contains(&self.inner, cf, key).map_err(crossed)
    }

    fn write_batch(&self, staged: WriteBatch) -> StoreResult<()> {
        EngineStoreTrait::write_batch(&self.inner, batch(staged)).map_err(crossed)
    }

    async fn write_batch_wait(&self, staged: WriteBatch) -> StoreResult<()> {
        EngineStoreTrait::write_batch_wait(&self.inner, batch(staged))
            .await
            .map_err(crossed)
    }

    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<()> {
        EngineStoreTrait::delete_range(&self.inner, cf, start, end).map_err(crossed)
    }

    fn iter(&self, cf: &str) -> StoreResult<StoreIter<'_>> {
        EngineStoreTrait::iter(&self.inner, cf)
            .map(rows)
            .map_err(crossed)
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<StoreIter<'_>> {
        EngineStoreTrait::iter_prefix(&self.inner, cf, prefix)
            .map(rows)
            .map_err(crossed)
    }

    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
        EngineStoreTrait::iter_keys_prefix(&self.inner, cf, prefix).map_err(crossed)
    }

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<u64> {
        EngineStoreTrait::count_prefix(&self.inner, cf, prefix).map_err(crossed)
    }

    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<Option<u64>> {
        EngineStoreTrait::bytes_prefix(&self.inner, cf, prefix).map_err(crossed)
    }

    fn sweep_keys_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
        EngineStoreTrait::sweep_keys_prefix(&self.inner, cf, prefix, from, limit).map_err(crossed)
    }

    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<(Vec<store::KeyValue>, Option<Vec<u8>>)> {
        let (rows, next) =
            EngineStoreTrait::sweep_prefix(&self.inner, cf, prefix, from, limit).map_err(crossed)?;
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
        let (rows, next) = EngineStoreTrait::sweep(&self.inner, cf, from, limit).map_err(crossed)?;
        let mut owned = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            owned.push((key, value.into_vec()));
        }
        Ok((owned, next))
    }

    fn iter_from(&self, cf: &str, start: &[u8], way: Direction) -> StoreResult<StoreIter<'_>> {
        EngineStoreTrait::iter_from(&self.inner, cf, start, direction(way))
            .map(rows)
            .map_err(crossed)
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<StoreIter<'_>> {
        EngineStoreTrait::iter_range(&self.inner, cf, start, end)
            .map(rows)
            .map_err(crossed)
    }

    fn actual_size_bytes(&self) -> StoreResult<u64> {
        EngineStoreTrait::actual_size_bytes(&self.inner).map_err(crossed)
    }

    fn available_disk_bytes(&self) -> StoreResult<Option<u64>> {
        EngineStoreTrait::available_disk_bytes(&self.inner).map_err(crossed)
    }

    fn live_data_size_bytes(&self) -> StoreResult<Option<u64>> {
        EngineStoreTrait::live_data_size_bytes(&self.inner).map_err(crossed)
    }

    fn key_count_estimate(&self, cf: &str) -> StoreResult<Option<u64>> {
        EngineStoreTrait::key_count_estimate(&self.inner, cf).map_err(crossed)
    }

    fn cf_disk_usage(&self) -> StoreResult<Vec<CfDiskUsage>> {
        EngineStoreTrait::cf_disk_usage(&self.inner)
            .map(|usage| usage.into_iter().map(cf_usage).collect())
            .map_err(crossed)
    }

    fn reclaim_space(&self) -> StoreResult<()> {
        EngineStoreTrait::reclaim_space(&self.inner).map_err(crossed)
    }

    fn disk_volumes(&self) -> StoreResult<Vec<DiskVolume>> {
        EngineStoreTrait::disk_volumes(&self.inner)
            .map(|volumes| volumes.into_iter().map(disk_volume).collect())
            .map_err(crossed)
    }
}
