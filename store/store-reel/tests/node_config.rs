//! The config a node opens its store with: the knobs it ships and what a small
//! volume claims from the filesystem.

use tempfile::TempDir;

/// Bytes a filesystem block occupies, as `stat` reports them
#[cfg(unix)]
const BLOCK_BYTES: u64 = 512;

// the node's own config opens, with durability on and the shipped knobs set
#[test]
fn node_opens() {
    let dir = TempDir::new().expect("dir");
    let store = tape_store_reel::open_node_store(
        dir.path().join("volume"),
        tape_store_reel::NodeStoreOptions {
            sync_bytes: 8 * 1024 * 1024,
            backend: reel::IoBackend::Posix,
            ..tape_store_reel::NodeStoreOptions::default()
        },
    )
    .expect("open the node store");

    // The shipped knobs have to be the ones a node gets, not just the ones a
    // bench got.
    let volume = store.inner().inner();
    let config = volume.engine().config();
    assert!(config.map_above.is_none());
    assert_ne!(
        config.sync,
        reel::SyncPolicy::Never,
        "a node cannot open with durability off",
    );
}

// a node gets the shipped sync, backend and mapping knobs unasked
#[test]
fn shipped_defaults() {
    let config = tape_store_reel::node_config(tape_store_reel::NodeStoreOptions::default());

    assert_eq!(
        config.sync,
        reel::SyncPolicy::Bytes(reel::ByteCount::from_bytes(16 * 1024 * 1024)),
    );

    // A ring wherever one can exist.
    #[cfg(target_os = "linux")]
    assert_eq!(config.io_backend, reel::IoBackend::Uring);
    #[cfg(not(target_os = "linux"))]
    assert_eq!(config.io_backend, reel::IoBackend::Posix);

    // A bad sector under a mapping is SIGBUS rather than an error the node can
    // act on.
    assert!(config.map_above.is_none());
}

// a small volume moves both knobs: smaller segments, on one tail
#[test]
fn small_reservation() {
    let fleet = tape_store_reel::node_config(tape_store_reel::NodeStoreOptions::default());
    let small = tape_store_reel::node_config(tape_store_reel::NodeStoreOptions {
        reserve: tape_store_reel::Reserve::Small,
        ..tape_store_reel::NodeStoreOptions::default()
    });

    assert_eq!(small.active_tails.resolve_tails(), 1);
    assert!(small.segment_bytes.to_bytes() < fleet.segment_bytes.to_bytes());

    // What the open segments span across every tail before the first seal.
    let idle = |config: &reel::ReelConfig| {
        config.segment_bytes.to_bytes() * config.tail_count() as u64
    };
    assert!(idle(&small) * 32 < idle(&fleet), "small={} fleet={}", idle(&small), idle(&fleet));

    // Every other shipped knob survives the smaller reservation.
    assert_eq!(small.sync, fleet.sync);
    assert_eq!(small.io_backend, fleet.io_backend);
    assert_eq!(small.map_above, fleet.map_above);
}

// a fresh small volume claims little from the filesystem, since the knobs are
// only worth having if the blocks follow them
#[cfg(unix)]
#[test]
fn small_claims_little() {
    use std::os::unix::fs::MetadataExt;

    let dir = TempDir::new().expect("dir");
    let root = dir.path().join("volume");
    let store = tape_store_reel::open_harness_store(&root).expect("open the harness store");
    drop(store);

    let mut claimed = 0u64;
    let mut stack = vec![root.clone()];
    while let Some(path) = stack.pop() {
        for entry in std::fs::read_dir(&path).expect("read the volume") {
            let entry = entry.expect("entry");
            let meta = entry.metadata().expect("metadata");
            match meta.is_dir() {
                true => stack.push(entry.path()),
                // Blocks: a tail zeroes a window ahead of its writes, and the
                // rest of the segment is sparse.
                false => claimed += meta.blocks() * BLOCK_BYTES,
            }
        }
    }

    // One tail's window, against a window per tail under the shipped shape.
    assert!(
        claimed < 64 * 1024 * 1024,
        "a fresh small volume claimed {claimed} bytes"
    );
}

// a fleet volume runs one tail on a spinning disk and two otherwise
#[test]
fn fleet_tails() {
    let spinning = tape_store_reel::fleet_tails(Some(true));
    let solid = tape_store_reel::fleet_tails(Some(false));
    let unknown = tape_store_reel::fleet_tails(None);

    assert_eq!(spinning.resolve_tails(), 1);
    assert_eq!(solid.resolve_tails(), 2);
    assert_eq!(unknown.resolve_tails(), 2);
}
