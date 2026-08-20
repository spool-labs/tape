//! The plumbing every tape bench runs through, exercised once per engine
//!
//! Not a measurement. This proves a `TapeStore` on the reel takes the same slice
//! and track traffic the rocks arm does, so a reel row comes off a store that
//! actually served the workload.

use reel_store::{BenchArm, MetaBulkStore, ReelStore};
use store_rocks::SplitStore;
use tape_core::erasure::{
    sample_window, sample_window_range, slice_sidecar, SAMPLE_WINDOW_BYTES, SAMPLE_WINDOW_LEAVES,
    SUB_LEAF_BYTES,
};
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
    // Stored bytes rather than payload bytes: a coded column reports what the
    // codec left, and this fill is a run of one byte, so the ceiling is what an
    // uncoded arm holds and the floor is only that it weighs something.
    let (count, bytes) = store.slice_totals_by_spool(spool).unwrap();
    assert_eq!(count, SLICE_COUNT as u64, "{}", A::NAME);
    if let Some(bytes) = bytes {
        let uncoded = StorageUnits::from_bytes((SLICE_COUNT * (SLICE_SIZE + 64)) as u64);
        assert!(bytes > StorageUnits(0) && bytes <= uncoded, "{} weighed {bytes:?}", A::NAME);
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

/// A slice big enough to hold several sample windows, in bytes a codec shrinks
fn windowed_slice() -> Vec<u8> {
    reel_store::fill::markdown(3, 3 * SAMPLE_WINDOW_BYTES + 777)
}

/// A challenge answers the same bytes whatever the column did with them
///
/// The window and the sub-leaf are logical offsets into the payload, so a coded
/// column has to answer what the caller wrote and not what it stored.
fn windows_are_logical<A: BenchArm>() {
    let dir = TempDir::new().unwrap();
    let store = A::open_bench(dir.path());
    let spool = SpoolIndex(4);
    let track = Address::new_unique();
    let payload = windowed_slice();

    store.put_slice(spool, track, payload.clone()).unwrap();
    A::settle(&store);

    for sub_leaf in [0usize, 1, SAMPLE_WINDOW_LEAVES, 2 * SAMPLE_WINDOW_LEAVES + 5] {
        let asked = sample_window_range(sub_leaf);
        let (sidecar, window) = store
            .slice_window(spool, track, asked.start, asked.len())
            .unwrap()
            .unwrap();

        assert_eq!(sidecar, slice_sidecar(&payload).unwrap(), "{}", A::NAME);
        assert_eq!(window, payload[sample_window(sub_leaf, payload.len())], "{}", A::NAME);

        // The sub-leaf a proof signs, cut out of the window the same way the
        // challenge cuts it.
        let at = (sub_leaf % SAMPLE_WINDOW_LEAVES) * SUB_LEAF_BYTES;
        let leaf = &window[at..(at + SUB_LEAF_BYTES).min(window.len())];
        let start = sub_leaf * SUB_LEAF_BYTES;
        assert_eq!(leaf, &payload[start..(start + SUB_LEAF_BYTES).min(payload.len())],
            "{} at sub-leaf {sub_leaf}", A::NAME);
    }
}

// a challenge window reads the same bytes off rocks
#[test]
fn rocks_windows() {
    windows_are_logical::<SplitStore>();
}

// a challenge window reads the same bytes off the reel
#[test]
fn reel_windows() {
    windows_are_logical::<ReelStore>();
}

// a challenge window reads the same bytes off the split arm
#[test]
fn split_windows() {
    windows_are_logical::<MetaBulkStore>();
}

// rocks serves the whole tape workload
#[test]
fn rocks_traffic() {
    slice_and_track_traffic::<SplitStore>();
}

// the reel serves the whole tape workload
#[test]
fn reel_traffic() {
    slice_and_track_traffic::<ReelStore>();
}

// the split arm serves the whole tape workload
#[test]
fn split_traffic() {
    slice_and_track_traffic::<MetaBulkStore>();
}
