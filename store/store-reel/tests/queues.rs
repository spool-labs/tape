//! The missing_list on the reel
//!
//! A live spool's page leans on the engine's key order: missing tracks sort
//! by position, then origin, then virtual group, then track. These cases hold
//! that order on the reel itself, before and after its appends reach disk.

use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_recover::track::MissingTrack;
use tape_recover::time::WallTime;
use tape_store::ops::SpoolOps;
use tape_store::TapeStore;
use tape_store_reel::{open_harness_store, ReelStore};
use tempfile::TempDir;

const POSITION: usize = 4;

fn key(group: u64, is_handover: bool, byte: u8) -> MissingTrack {
    let spool = VirtualGroup(group).spool_at(POSITION);
    let track = Address::new([byte; 32]);
    if is_handover {
        return MissingTrack::new_handover(spool, track, WallTime(1));
    }
    MissingTrack::new_upload(spool, track, WallTime(1))
}

fn page(store: &TapeStore<ReelStore>, live: SpoolIndex, fold: GroupFold) -> Vec<u8> {
    let page = store.get_missing_tracks(live, fold, None, 100).expect("page");
    page.iter().map(|missing| missing.track.to_bytes()[0]).collect()
}

// a live spool's page walks its runs in key order, plain entries first, and a
// purge of one stored spool leaves the others
#[test]
fn missing_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_harness_store(dir.path()).expect("open");
    let live = GroupIndex(0).spool_at(POSITION);
    let fold = GroupFold::new(2).expect("fold");

    for missing in [key(4, false, 3), key(0, true, 4), key(2, false, 2), key(0, false, 1)] {
        store.put_missing_track(&missing).expect("missing track");
    }
    store.put_missing_track(&key(1, false, 9)).expect("missing track");

    assert_eq!(page(&store, live, fold), [1, 2, 3, 4]);

    store.inner().inner().flush().expect("flush");
    assert_eq!(page(&store, live, fold), [1, 2, 3, 4]);
    assert!(store.has_handover_tracks(live, fold).expect("handover"));

    let spool = VirtualGroup(0).spool_at(POSITION);
    assert_eq!(store.delete_missing_tracks(spool, 10).expect("purge"), 2);
    assert_eq!(page(&store, live, fold), [2, 3]);
    assert!(!store.has_handover_tracks(live, fold).expect("handover"));
}
