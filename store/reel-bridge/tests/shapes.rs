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
    let store = reel_bridge::open_node_store(dir.path().join("volume"), 0, 8 * 1024 * 1024)
        .expect("open the node store");

    // The knobs this campaign measured have to be the ones a node gets, not
    // just the ones a bench got.
    let bridge = store.inner().inner();
    let config = bridge.engine().config();
    assert!(config.map_above.is_some(), "warm reads would take the door");
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
