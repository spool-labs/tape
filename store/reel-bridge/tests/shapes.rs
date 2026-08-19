//! The shape each column's index took, against the one it declared
//!
//! A declaration is a request the engine may decline, and it declines silently:
//! `ShardShapes::Tree` drops every open-shard request and hands back a tree. A
//! measurement taken through a declined declaration reports the default under
//! the name of the shape it asked for, which is worse than no number at all.

use reel::MapShape;
use reel_bridge::{bench_config, ReelBridge, TAPE_COLUMNS};
use tempfile::TempDir;

/// The segment size a bench arm opens with
const SEGMENT_BYTES: u64 = 256 * 1024 * 1024;

// every column's index takes the shape its declaration asked for
#[test]
fn declared_shapes_take() {
    let dir = TempDir::new().expect("dir");
    let store = ReelBridge::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

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
    let store = ReelBridge::open(dir.path(), bench_config(SEGMENT_BYTES), TAPE_COLUMNS).expect("open");

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
    let store = reel_bridge::open_node_store(
        dir.path().join("volume"),
        0,
        8 * 1024 * 1024,
        reel::IoBackend::Posix,
    )
    .expect("open the node store");

    // The knobs this campaign measured have to be the ones a node gets, not
    // just the ones a bench got.
    let bridge = store.inner().inner();
    let config = bridge.engine().config();
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
        bridge.declined_shapes().is_empty(),
        "a column's declared shape was dropped",
    );
}

// the three knobs the HDD battery settled are what a node gets unasked
#[test]
fn shipped_defaults() {
    let config = reel_bridge::node_config(
        0,
        reel_bridge::DEFAULT_SYNC_BYTES,
        reel_bridge::default_backend(),
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
        let config = reel_bridge::node_config(0, 8 * 1024 * 1024, backend);
        assert_eq!(config.point_reads, wanted, "{backend:?}");
    }
}
