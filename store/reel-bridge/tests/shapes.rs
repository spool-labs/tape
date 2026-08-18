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
