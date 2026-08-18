//! The plumbing every tape bench runs through, exercised once per engine
//!
//! Not a measurement: this is what proves a `TapeStore` on the public reel takes
//! the same slice and track traffic the rocks arm does, so a bench that reports a
//! reel row reported it off a store that actually served the workload.

use reel_bridge::{BenchArm, MetaBulkStore, ReelBridge};
use store_rocks::SplitStore;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{GroupIndex, SpoolIndex, StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::ops::{SliceOps, TrackOps};
use tempfile::TempDir;

/// Slices written per spool, enough that a prefix walk has something to walk
const SLICE_COUNT: usize = 16;

/// Slice payload size, small so the smoke stays a smoke
const SLICE_SIZE: usize = 4 * 1024;

fn track(tape: Address) -> CompressedTrack {
    CompressedTrack {
        tape,
        key: Hash::new_unique(),
        track_number: TrackNumber(0),
        kind: TrackKind::Coded as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(SLICE_SIZE as u64),
        group: GroupIndex(3),
        value_hash: Hash::new_unique(),
    }
}

fn slice_and_track_traffic<A: BenchArm>() {
    let dir = TempDir::new().unwrap();
    let store = A::open_bench(dir.path());
    let spool = SpoolIndex(7);
    let other = SpoolIndex(8);

    let mut written = Vec::new();
    for index in 0..SLICE_COUNT {
        let address = Address::new_unique();
        let payload = vec![index as u8; SLICE_SIZE];
        store.put_slice(spool, address, payload.clone()).unwrap();
        store.put_slice(other, address, payload).unwrap();
        written.push(address);
    }
    A::settle(&store);

    for (index, address) in written.iter().enumerate() {
        let payload = store.get_slice(spool, *address).unwrap().unwrap();
        assert_eq!(payload.len(), SLICE_SIZE, "{}", A::NAME);
        assert_eq!(payload[0], index as u8, "{}", A::NAME);
        assert!(store.has_slice(spool, *address).unwrap(), "{}", A::NAME);
    }

    assert_eq!(
        store.count_slices_by_spool(spool).unwrap(),
        SLICE_COUNT,
        "{}",
        A::NAME
    );
    assert_eq!(
        store.iter_slice_keys_by_spool(spool).unwrap().len(),
        SLICE_COUNT,
        "{}",
        A::NAME
    );
    // Stored bytes cover the payloads and the sidecar in front of each of them,
    // so the floor is the payloads and the arms differ above it.
    let (count, bytes) = store.slice_totals_by_spool(spool).unwrap();
    assert_eq!(count, SLICE_COUNT as u64, "{}", A::NAME);
    if let Some(bytes) = bytes {
        assert!(
            bytes >= StorageUnits::from_bytes((SLICE_COUNT * SLICE_SIZE) as u64),
            "{} weighed {bytes:?} of slices",
            A::NAME
        );
    }

    let sizes = store.iter_slice_sizes_by_spool(spool).unwrap();
    assert_eq!(sizes.len(), SLICE_COUNT, "{}", A::NAME);

    for address in &written {
        let tape = Address::new_unique();
        store.put_track(*address, track(tape)).unwrap();
    }
    A::settle(&store);
    assert_eq!(store.count_tracks().unwrap(), SLICE_COUNT, "{}", A::NAME);

    store.delete_all_slices_for_spool(spool).unwrap();
    A::settle(&store);
    assert_eq!(store.count_slices_by_spool(spool).unwrap(), 0, "{}", A::NAME);
    assert_eq!(
        store.count_slices_by_spool(other).unwrap(),
        SLICE_COUNT,
        "the range delete took the neighbouring spool with it on {}",
        A::NAME
    );
}

#[test]
fn rocks_serves_the_tape_workload() {
    slice_and_track_traffic::<SplitStore>();
}

#[test]
fn reel_serves_the_tape_workload() {
    slice_and_track_traffic::<ReelBridge>();
}

#[test]
fn rocks_meta_plus_reel_bulk_serves_the_tape_workload() {
    slice_and_track_traffic::<MetaBulkStore>();
}
