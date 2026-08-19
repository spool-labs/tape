//! The shape each column's index took, against the one it declared
//!
//! A declaration is a request the engine may decline, and it declines silently:
//! `ShardShapes::Tree` drops every open-shard request and hands back a tree. A
//! measurement taken through a declined declaration reports the default under
//! the name of the shape it asked for, which is worse than no number at all.

use reel::MapShape;
use reel_store::{bench_config, ReelStore, TAPE_COLUMNS};
use tempfile::TempDir;

/// The segment size a bench arm opens with
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

// every column's index takes the shape its declaration asked for
#[test]
fn declared_shapes_take() {
    let dir = TempDir::new().expect("dir");
    let store = ReelStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

    let declined = store.declined_shapes();
    assert!(
        declined.is_empty(),
        "columns whose declaration was dropped: {declined:?}",
    );
}

// the column the plan flips is on the open shard, and its neighbours are not
#[test]
fn track_data_is_open() {
    let dir = TempDir::new().expect("dir");
    let store = ReelStore::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

    for (name, _, got) in store.shapes() {
        let wanted = match name {
            "track_data" => MapShape::Open,
            _ => MapShape::Tree,
        };
        assert_eq!(got, wanted, "column {name}");
    }
}

// the node's own config opens, with durability on and the measured knobs set
#[test]
fn node_config_opens() {
    let dir = TempDir::new().expect("dir");
    let store = reel_store::open_node_store(
        dir.path().join("volume"),
        0,
        8 * 1024 * 1024,
        reel::IoBackend::Posix,
        reel_store::Reserve::Fleet,
    )
    .expect("open the node store");

    // The knobs this campaign measured have to be the ones a node gets, not
    // just the ones a bench got.
    let volume = store.inner().inner();
    let config = volume.engine().config();
    // No mapping: the fleet gives up the warm read rather than take SIGBUS on a
    // bad sector, and a posix volume has no ring overhead for the probe to undo.
    assert!(config.map_above.is_none());
    assert_eq!(config.point_reads, reel::PointReads::Queued);
    assert_ne!(
        config.sync,
        reel::SyncPolicy::Never,
        "a node cannot open with durability off",
    );
    assert!(
        volume.declined_shapes().is_empty(),
        "a column's declared shape was dropped",
    );
}

// the three knobs the HDD battery settled are what a node gets unasked
#[test]
fn shipped_defaults() {
    let config = reel_store::node_config(
        0,
        reel_store::DEFAULT_SYNC_BYTES,
        reel_store::default_backend(),
        reel_store::Reserve::Fleet,
    );

    // Sixteen mebibytes between syncs: 1.09-1.46x the latency of never syncing,
    // and no extra bytes written. A sync per put was 16-67x.
    assert_eq!(
        config.sync,
        reel::SyncPolicy::Bytes(reel::ByteCount::from_bytes(16 * 1024 * 1024)),
    );

    // A ring wherever one can exist, and the probe follows it.
    #[cfg(target_os = "linux")]
    {
        assert_eq!(config.io_backend, reel::IoBackend::Uring);
        assert_eq!(config.point_reads, reel::PointReads::Probed);
    }
    #[cfg(not(target_os = "linux"))]
    {
        assert_eq!(config.io_backend, reel::IoBackend::Posix);
        assert_eq!(config.point_reads, reel::PointReads::Queued);
    }

    // A cold mapped read pulls 5x the device bytes against a ~7.8 ms spindle,
    // and a bad sector under a mapping is SIGBUS rather than an error.
    assert!(config.map_above.is_none());
}

// the probe is coupled to the backend, since only a ring has overhead to undo
#[test]
fn probe_follows_the_backend() {
    for (backend, wanted) in [
        (reel::IoBackend::Posix, reel::PointReads::Queued),
        (reel::IoBackend::Uring, reel::PointReads::Probed),
        (reel::IoBackend::UringDirect, reel::PointReads::Queued),
    ] {
        let config =
            reel_store::node_config(0, 8 * 1024 * 1024, backend, reel_store::Reserve::Fleet);
        assert_eq!(config.point_reads, wanted, "{backend:?}");
    }
}

// a small reservation moves all four knobs, since three of them alone still
// leave a gibibyte on disk before the first slice
#[test]
fn small_sizes_the_reservation_to_the_run() {
    let backend = reel_store::default_backend();
    let fleet = reel_store::node_config(0, 8 * 1024 * 1024, backend, reel_store::Reserve::Fleet);
    let small = reel_store::node_config(0, 8 * 1024 * 1024, backend, reel_store::Reserve::Small);

    assert_eq!(fleet.preallocate, reel::Preallocate::Full);
    assert_eq!(small.preallocate, reel::Preallocate::Chunk);
    assert_eq!(small.active_tails.resolve_tails(), 1);
    assert!(small.segment_bytes.to_bytes() < fleet.segment_bytes.to_bytes());
    assert!(small.alloc_chunk.to_bytes() < fleet.alloc_chunk.to_bytes());

    // What the volume claims before it holds anything, which is the whole point.
    let idle = |config: &reel::ReelConfig| {
        config.segment_bytes.to_bytes() * config.tail_count() as u64
    };
    assert!(idle(&small) * 32 < idle(&fleet), "small={} fleet={}", idle(&small), idle(&fleet));

    // Everything the HDD battery settled survives the smaller reservation.
    assert_eq!(small.sync, fleet.sync);
    assert_eq!(small.io_backend, fleet.io_backend);
    assert_eq!(small.point_reads, fleet.point_reads);
    assert_eq!(small.shard_shapes, fleet.shard_shapes);
    assert_eq!(small.map_above, fleet.map_above);
}

// what a fresh small volume actually claims from the filesystem, since the
// knobs are only worth having if the blocks follow them
#[test]
fn a_small_volume_claims_little_before_it_holds_anything() {
    use std::os::unix::fs::MetadataExt;

    let dir = TempDir::new().expect("dir");
    let root = dir.path().join("volume");
    let store = reel_store::open_harness_store(&root).expect("open the harness store");
    drop(store);

    let mut claimed = 0u64;
    let mut stack = vec![root.clone()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read the volume") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("metadata");
            match meta.is_dir() {
                true => stack.push(entry.path()),
                // Blocks rather than length: preallocation is the reservation,
                // and a sparse extend would show a length it never took.
                false => claimed += meta.blocks() * 512,
            }
        }
    }

    // One tail reserving a 4 MiB step, against the gibibyte per tail the shipped
    // shape would have claimed here.
    assert!(
        claimed < 64 * 1024 * 1024,
        "a fresh small volume claimed {claimed} bytes"
    );
}
