//! The missing_list on the reel
//!
//! A live spool's page leans on the engine's key order: entries sort by
//! position, then handover, then virtual group, then track. These cases hold
//! that order on the reel itself, before and after its appends reach disk.

use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::SpoolOps;
use tape_store::types::{Entry, EntryKey};
use tape_store::TapeStore;
use tape_store_reel::{open_harness_store, ReelStore};
use tempfile::TempDir;

const POSITION: usize = 4;

fn key(group: u64, is_handover: bool, byte: u8) -> EntryKey {
    EntryKey {
        spool: VirtualGroup(group).spool_at(POSITION),
        is_handover,
        track: Address::new([byte; 32]),
    }
}

fn page(store: &TapeStore<ReelStore>, live: SpoolIndex, fold: GroupFold) -> Vec<u8> {
    let page = store.missing_page(live, fold, None, 100).expect("page");
    page.iter().map(|(key, _)| key.track.to_bytes()[0]).collect()
}

// a live spool's page walks its runs in key order, plain entries first, and a
// purge of one stored spool leaves the others
#[test]
fn missing_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_harness_store(dir.path()).expect("open");
    let live = GroupIndex(0).spool_at(POSITION);
    let fold = GroupFold::new(2).expect("fold");

    for entry in [key(4, false, 3), key(0, true, 4), key(2, false, 2), key(0, false, 1)] {
        store.put_entry(entry, Entry::never_tried()).expect("entry");
    }
    store.put_entry(key(1, false, 9), Entry::never_tried()).expect("entry");

    assert_eq!(page(&store, live, fold), [1, 2, 3, 4]);

    store.inner().inner().flush().expect("flush");
    assert_eq!(page(&store, live, fold), [1, 2, 3, 4]);
    assert!(store.has_handover_entries(live, fold).expect("handover"));

    let spool = VirtualGroup(0).spool_at(POSITION);
    assert_eq!(store.delete_spool_entries(spool, 10).expect("purge"), 2);
    assert_eq!(page(&store, live, fold), [2, 3]);
    assert!(!store.has_handover_entries(live, fold).expect("handover"));
}
