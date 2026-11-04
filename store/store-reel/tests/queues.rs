//! The repair and recovery queues on the reel
//!
//! The queues lean on the engine's key order: rows sort by spool, then ready
//! slot, then track. These cases hold that order on the reel itself, before
//! and after its appends reach disk.

use tape_core::types::{SlotNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store::ops::{MetaOps, SpoolOps};
use tape_store::TapeStore;
use tape_store_reel::{open_harness_store, ReelStore};
use tempfile::TempDir;

const SPOOL: SpoolIndex = SpoolIndex(43);

fn address(byte: u8) -> Address {
    Address::new([byte; 32])
}

fn tracks_due(store: &TapeStore<ReelStore>, until: SlotNumber) -> Vec<Address> {
    let rows = store.repairs_due(SPOOL, None, until, 100).expect("due rows");
    rows.into_iter().map(|row| row.track).collect()
}

// rows come back by ready slot then track, a row put back goes last, and a
// clear leaves nothing behind
#[test]
fn queue_order() {
    let dir = TempDir::new().expect("temp dir");
    let store = open_harness_store(dir.path()).expect("open");

    store.set_sync_cursor(SlotNumber(10), None).expect("cursor");
    for byte in [3, 1, 2] {
        store.add_pending_repair(SPOOL, address(byte)).expect("queue");
    }
    store.defer_pending_repair(SPOOL, address(4), SlotNumber(9)).expect("defer");
    store.defer_pending_repair(SPOOL, address(5), SlotNumber(30)).expect("defer");
    store.add_pending_repair(SpoolIndex(44), address(6)).expect("queue");
    store.requeue_repair(SPOOL, address(1)).expect("requeue");

    assert_eq!(tracks_due(&store, SlotNumber(10)), [address(4), address(2), address(3)]);
    store.inner().inner().flush().expect("flush");
    assert_eq!(
        tracks_due(&store, SlotNumber(30)),
        [address(4), address(2), address(3), address(1), address(5)]
    );

    let first = store.repairs_due(SPOOL, None, SlotNumber(30), 2).expect("page");
    let rest = store.repairs_due(SPOOL, first.last().copied(), SlotNumber(30), 10).expect("page");
    let tracks: Vec<Address> = rest.iter().map(|row| row.track).collect();
    assert_eq!(tracks, [address(3), address(1), address(5)]);

    store.clear_all_pending_repairs(SPOOL).expect("clear");
    assert!(tracks_due(&store, SlotNumber(u64::MAX)).is_empty());
    assert!(!store.has_pending_repair(SPOOL, address(1)).expect("index"));
    assert_eq!(store.pending_repair_spools().expect("spools"), [SpoolIndex(44)]);
}
