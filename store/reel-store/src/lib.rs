//! Tapedrive's columns on the reel engine
//!
//! Every family a tape store addresses, served by one reel volume. A node opens
//! through `open_node_store`, an offline tool through `open_node_store_read_only`,
//! a harness through `open_harness_store`, and the benches through the rest.
//!
//! The reel implements a vendored copy of the store trait. Batches and values
//! move across without a copy. The error, usage and volume types are structurally
//! identical and nominally distinct, so each crosses by hand.

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
    ReelStore as EngineStore, ServingBackend, ShardShapes, SyncPolicy, ThreadBudget,
    MAP_EVERYTHING,
};
use reel_core::Store as EngineStoreTrait;
use serde::Deserialize;
use tape_store::TapeStore;
#[cfg(feature = "metrics")]
use store::get_metrics;
use store::{
    CfDiskUsage, Direction, DiskVolume, Error as StoreError, Result as StoreResult, Store,
    StoreIter, StoreVolume, Value, WriteBatch,
};

#[cfg(feature = "rocks")]
pub use arm::{
    scaled, track_data_codec, BenchArm, SCALE_VAR, SEGMENT_MIB_VAR, TRACK_DATA_CODEC_VAR,
};
pub use columns::{RAW_TRACK_DATA_COLUMNS, TAPE_COLUMNS};
// So a tool opening a node's volume needs this crate and not the engine behind it.
pub use reel::{IndexCheckpoint, IndexResidency};
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
    /// Open the volume a node runs on, under the node's own config
    ///
    /// The one entry point that is not bench scoped. Everything else in this
    /// crate opens with `bench_config`, which turns durability off.
    pub fn open_node(root: impl AsRef<Path>, options: NodeStoreOptions) -> StoreResult<ReelStore> {
        ReelStore::open(root, node_config(options), TAPE_COLUMNS)
    }

    /// Open a reel under this directory serving the given column families
    ///
    /// The set is a parameter rather than a constant because a run weighing a
    /// codec opens the same families twice and declares one of them both ways.
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
    /// Creates no directory: a path with no volume under it is a mistyped path,
    /// and an empty store would read as a node holding nothing.
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

    /// The backend actually serving this volume
    ///
    /// A configured ring downgrades to posix where the kernel will not give one,
    /// so the config states a request and this states the outcome.
    pub fn serving_backend(&self) -> ServingBackend {
        self.inner.serving_backend()
    }

    /// The shape each column's index actually took, beside the one it declared
    ///
    /// A declaration is a request the engine may decline, and it declines
    /// silently, so a run reports the shape it got rather than the one it asked
    /// for.
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
    /// A paged open counts only the keys it holds, so the record and byte cells
    /// go unanswered there rather than reporting a fraction as the whole.
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
    /// What a bench calls between its write phase and its read phase, so neither
    /// measures the other.
    pub fn flush(&self) -> StoreResult<()> {
        self.inner.flush().map_err(engine)
    }

    /// Write the resident index down at a cue, so the next open skips the sweep
    ///
    /// Takes its own cue and seals every open tail, which is why nothing but a
    /// shutdown calls it. `None` where there is no resident index to write down,
    /// which is a volume opened unarmed or one paging its keys out to the footers.
    pub fn checkpoint_index(&self) -> StoreResult<Option<IndexCheckpoint>> {
        let config = self.inner.config();
        if !config.index_checkpoint || config.index.pages() {
            return Ok(None);
        }
        self.inner.checkpoint_index().map(Some).map_err(engine)
    }
}

/// Bytes a node writes between durability syncs
///
/// A node writes slices in batches and wants one sync per drain. What a crash
/// risks is the tail.
pub const DEFAULT_SYNC_BYTES: u64 = 16 * 1024 * 1024;

/// The file backend a node opens its volume with
///
/// A ring wherever one can exist. The engine downgrades to posix with a warning
/// where the kernel will not give one, so naming it costs nothing on a kernel
/// that has none.
pub const fn default_backend() -> IoBackend {
    match cfg!(target_os = "linux") {
        true => IoBackend::Uring,
        false => IoBackend::Posix,
    }
}

/// What a fresh volume claims before it holds a byte
///
/// A tail costs a reserved segment and the shipped shape pre-writes each one
/// whole. That is the right trade on a fleet disk and the wrong one wherever the
/// reservation is a large share of the volume, or twenty nodes share a laptop.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reserve {
    /// Gibibyte segments pre-written whole, a tail per core up to eight
    #[default]
    Fleet,

    /// Segments a short run can fill, reserved in steps, on one tail
    Small,
}

/// The knobs a node opens its volume with, straight off the node's store config
#[derive(Debug, Clone, Copy)]
pub struct NodeStoreOptions {
    /// Compaction rate cap in MB/s. 0 lets the engine pace itself.
    pub compaction_mbps: u64,
    /// Bytes written between durability syncs. 0 syncs on every put.
    pub sync_bytes: u64,
    /// File backend the volume opens with.
    pub backend: IoBackend,
    /// The reservation preset the rest of the knobs start from.
    pub reserve: Reserve,
    /// Bytes one segment spans. 0 keeps the preset's size.
    pub segment_bytes: u64,
    /// How a segment claims its space. Unset keeps the preset's choice.
    pub preallocate: Option<Preallocate>,
}

impl Default for NodeStoreOptions {
    fn default() -> Self {
        NodeStoreOptions {
            compaction_mbps: 0,
            sync_bytes: DEFAULT_SYNC_BYTES,
            backend: default_backend(),
            reserve: Reserve::default(),
            segment_bytes: 0,
            preallocate: None,
        }
    }
}

/// Bytes a small volume seals a segment at
const SMALL_SEGMENT_BYTES: u64 = 32 * 1024 * 1024;

/// Bytes a small volume reserves ahead of its write head
const SMALL_ALLOC_CHUNK: u64 = 4 * 1024 * 1024;

/// The config a node opens its volume with
///
/// The bench config minus everything that only makes sense when a run is about
/// to be thrown away: durability is a real policy rather than `Never`, and the
/// segment is the shipped size.
pub fn node_config(options: NodeStoreOptions) -> ReelConfig {
    let config = ReelConfig {
        sync: match options.sync_bytes {
            0 => SyncPolicy::EveryPut,
            bytes => SyncPolicy::Bytes(ByteCount::from_bytes(bytes)),
        },
        compact_mbps: match options.compaction_mbps {
            0 => CompactRate::Auto,
            capped => CompactRate::Mbps(capped),
        },
        // No mapping. It buys latency on a warm single read and costs a cold
        // read extra device bytes, and a bad sector under a mapping is SIGBUS
        // and a dead process where the door returns an error the node can act
        // on. The mapped path stays a bench and tooling knob.
        map_above: None,
        point_reads: probe_for(options.backend),
        io_backend: options.backend,
        shard_shapes: ShardShapes::Declared,
        // Armed both ways: a clean shutdown writes the index down, and the next
        // open reads it back instead of sweeping every sealed segment's footer.
        index_checkpoint: true,
        ..ReelConfig::default()
    };

    let mut config = match options.reserve {
        Reserve::Fleet => config,
        // Four knobs rather than one: dropping the tail count alone still
        // reserves a gibibyte, and shrinking the segment alone still pre-writes
        // it. Chunk is what makes the reservation track the data.
        Reserve::Small => ReelConfig {
            segment_bytes: ByteCount::from_bytes(SMALL_SEGMENT_BYTES),
            alloc_chunk: ByteCount::from_bytes(SMALL_ALLOC_CHUNK),
            preallocate: Preallocate::Chunk,
            active_tails: ThreadBudget::threads(1),
            ..config
        },
    };

    // The explicit knobs override whichever preset was picked. The chunk step
    // rides down with a shrunken segment, since the engine refuses a step wider
    // than the segment it feeds.
    if options.segment_bytes != 0 {
        config.segment_bytes = ByteCount::from_bytes(options.segment_bytes);
        let chunk = config.alloc_chunk.to_bytes().min(options.segment_bytes);
        config.alloc_chunk = ByteCount::from_bytes(chunk);
    }
    if let Some(preallocate) = options.preallocate {
        config.preallocate = preallocate;
    }
    config
}

/// Whether to ask the page cache before queueing a read, which only a ring wants
///
/// A ring read is a submit and a wait, and the probe skips that round trip. A
/// posix read is answered inline already, so there the probe is one wasted
/// syscall per read. Coupled here so the pairing is not something to re-learn.
fn probe_for(backend: IoBackend) -> PointReads {
    match backend {
        IoBackend::Uring => PointReads::Probed,
        IoBackend::Posix | IoBackend::UringDirect => PointReads::Queued,
    }
}

/// The store a node runs, every tape family on one reel volume
///
/// Lives here rather than beside the other `TapeStore` constructors because this
/// crate sits above `tape-store` in the graph and needs the column declarations.
pub fn open_node_store(
    root: impl AsRef<Path>,
    options: NodeStoreOptions,
) -> StoreResult<TapeStore<ReelStore>> {
    let root = root.as_ref();
    std::fs::create_dir_all(root)?;
    let requested = options.backend;
    let volume = ReelStore::open_node(root, options)?;

    // The request beside the outcome, since a volume that asked for the ring can
    // be served by posix. The engine's own warning only fires on the downgrade,
    // so it cannot tell a ring from a log nobody configured.
    tracing::info!(
        requested = ?requested,
        serving = %volume.serving_backend(),
        root = %root.display(),
        "opened the node volume",
    );

    Ok(TapeStore::new(volume))
}

/// The config an offline tool reads a node's volume under
///
/// The node's own shape declarations, since a column read under a different one
/// is a column the engine refuses to serve. Residency is the caller's: resident
/// answers what a column holds, paged answers which sealed segments stand over
/// it and fits a volume larger than the memory reading it.
pub fn read_only_config(residency: IndexResidency) -> ReelConfig {
    ReelConfig {
        index: residency,
        // The engine refuses an open shard under a paged walk, so a paged read
        // takes every column as a tree. The shape is how this open builds its
        // index, not how the volume was written.
        shard_shapes: match residency {
            IndexResidency::Resident => ShardShapes::Declared,
            _ => ShardShapes::Tree,
        },
        ..node_config(NodeStoreOptions::default())
    }
}

/// A tape store over a node's volume, read-only and without its lock
///
/// What every offline tool opens, so it reads beside a running node rather than
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

/// The node's own policy at a size a throwaway volume can afford
///
/// Every knob the fleet ships, at a reservation the size of the run. The same
/// shape an operator asks for with `store.reserve: small`.
pub fn harness_config() -> ReelConfig {
    node_config(NodeStoreOptions {
        reserve: Reserve::Small,
        ..NodeStoreOptions::default()
    })
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
/// Segments are small enough that a bench writing a few GiB still seals several
/// of them, and space is reserved a chunk ahead so a case that writes little is
/// not charged for a segment it never fills. Syncing is left to the caller's
/// flush, matching a rocks arm that does not fsync per write either.
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
        // sweeping segment sizes has no reason to know that.
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
        // Ask the page cache before queueing a read: a cold probe costs one
        // nowait syscall against a seek, and a warm one skips the door.
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
/// `Never` is a control column, not an operating mode: every figure taken under
/// it owes a durable one beside it before anything is concluded about a node.
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
/// Bytes the found values weigh, for the read counters
fn values_len(result: &StoreResult<Vec<Option<Value>>>) -> usize {
    match result {
        Ok(values) => values
            .iter()
            .flatten()
            .map(|value| value.len())
            .sum(),
        Err(_) => 0,
    }
}

/// The batch's payload bytes and the cf that stands for it in the counters,
/// the last one seen, the way the rocks backend reports a batch
fn batch_weight(staged: &WriteBatch) -> (String, usize) {
    let mut cf: Option<&str> = None;
    let mut written = 0usize;
    for op in staged.iter() {
        match op {
            store::BatchOp::Put { cf: name, key, value } => {
                cf = Some(name);
                written += key.len() + value.len();
            }
            store::BatchOp::Delete { cf: name, .. } => cf = Some(name),
        }
    }
    (cf.unwrap_or("default").to_string(), written)
}

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


/// Record one store operation against the shared counters, so the board's store
/// panel reads the reel the same way it read rocks
#[cfg(not(feature = "metrics"))]
fn record(_: &str, _: &str, _: bool, _: f64, _: usize, _: usize) {}

#[cfg(feature = "metrics")]
fn record(cf: &str, op: &str, ok: bool, elapsed: f64, read: usize, written: usize) {
    let Some(m) = get_metrics() else { return };
    let status = if ok { "ok" } else { "error" };
    m.operations_total.with_label_values(&[cf, op, status]).inc();
    if read > 0 {
        m.bytes_read_total.with_label_values(&[cf]).inc_by(read as u64);
    }
    if written > 0 {
        m.bytes_written_total.with_label_values(&[cf]).inc_by(written as u64);
    }
    if !ok {
        m.errors_total.with_label_values(&[cf, op, "engine"]).inc();
    }
    match op {
        "get" => m.get_duration.with_label_values(&[cf, &ok.to_string()]).observe(elapsed),
        "put" => m.put_duration.with_label_values(&[cf]).observe(elapsed),
        "delete" => m.delete_duration.with_label_values(&[cf]).observe(elapsed),
        _ => {}
    }
}

impl Store for ReelStore {
    fn get(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::get(&self.inner, cf, key).map_err(crossed);
        let read = match &result {
            Ok(Some(v)) => v.len(),
            _ => 0,
        };
        record(cf, "get", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::get_many(&self.inner, cf, keys).map_err(crossed);
        let read = values_len(&result);
        record(cf, "get_many", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    async fn get_wait(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::get_wait(&self.inner, cf, key)
            .await
            .map_err(crossed);
        let read = match &result {
            Ok(Some(value)) => value.len(),
            _ => 0,
        };
        record(cf, "get", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    async fn get_many_wait(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::get_many_wait(&self.inner, cf, keys)
            .await
            .map_err(crossed);
        let read = values_len(&result);
        record(cf, "get_many", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    fn get_range(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        let timer = std::time::Instant::now();
        let result =
            EngineStoreTrait::get_range(&self.inner, cf, key, offset, len).map_err(crossed);
        let read = match &result {
            Ok(Some(value)) => value.len(),
            _ => 0,
        };
        record(cf, "get_range", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    async fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::get_range_wait(&self.inner, cf, key, offset, len)
            .await
            .map_err(crossed);
        let read = match &result {
            Ok(Some(value)) => value.len(),
            _ => 0,
        };
        record(cf, "get_range", result.is_ok(), timer.elapsed().as_secs_f64(), read, 0);
        result
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::put(&self.inner, cf, key, value).map_err(crossed);
        record(cf, "put", result.is_ok(), timer.elapsed().as_secs_f64(), 0, value.len());
        result
    }

    async fn put_wait(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::put_wait(&self.inner, cf, key, value)
            .await
            .map_err(crossed);
        record(cf, "put", result.is_ok(), timer.elapsed().as_secs_f64(), 0, value.len());
        result
    }

    fn delete(&self, cf: &str, key: &[u8]) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let result = EngineStoreTrait::delete(&self.inner, cf, key).map_err(crossed);
        record(cf, "delete", result.is_ok(), timer.elapsed().as_secs_f64(), 0, 0);
        result
    }

    fn contains(&self, cf: &str, key: &[u8]) -> StoreResult<bool> {
        EngineStoreTrait::contains(&self.inner, cf, key).map_err(crossed)
    }

    fn write_batch(&self, staged: WriteBatch) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let (cf, written) = batch_weight(&staged);
        let result = EngineStoreTrait::write_batch(&self.inner, batch(staged)).map_err(crossed);
        record(&cf, "write_batch", result.is_ok(), timer.elapsed().as_secs_f64(), 0, written);
        result
    }

    async fn write_batch_wait(&self, staged: WriteBatch) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let (cf, written) = batch_weight(&staged);
        let result = EngineStoreTrait::write_batch_wait(&self.inner, batch(staged))
            .await
            .map_err(crossed);
        record(&cf, "write_batch", result.is_ok(), timer.elapsed().as_secs_f64(), 0, written);
        result
    }

    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<()> {
        let timer = std::time::Instant::now();
        let result =
            EngineStoreTrait::delete_range(&self.inner, cf, start, end).map_err(crossed);
        record(cf, "delete_range", result.is_ok(), timer.elapsed().as_secs_f64(), 0, 0);
        result
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

    fn maintain(&self) -> StoreResult<()> {
        EngineStoreTrait::maintain(&self.inner).map_err(crossed)
    }

    fn disk_volumes(&self) -> StoreResult<Vec<DiskVolume>> {
        EngineStoreTrait::disk_volumes(&self.inner)
            .map(|volumes| volumes.into_iter().map(disk_volume).collect())
            .map_err(crossed)
    }

    fn close(&self) -> StoreResult<()> {
        self.inner.close().map_err(engine)
    }
}
