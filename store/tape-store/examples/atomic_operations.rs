//! Atomic operations example using operation traits
//!
//! Run with: cargo run -p tape-store --features rocks --example atomic_operations

use tape_core::spooler::VirtualGroup;
use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
use tape_core::types::{SlotNumber, SpoolIndex, StorageUnits, TapeNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_recover::state::SpoolState;
use tape_recover::track::MissingTrack;
use tape_recover::time::WallTime;
use tape_store::{error::Result, ops::*, TapeStore};

fn sample_track(tape: Address, track_number: u64) -> CompressedTrack {
    CompressedTrack {
        tape,
        tape_number: TapeNumber(1),
        key: Hash::new_unique(),
        track_number: TrackNumber(track_number),
        kind: TrackKind::Inline as u64,
        state: TrackState::Certified as u64,
        flags: TrackFlags::NONE,
        size: StorageUnits::from_bytes(1024),
        group: VirtualGroup(3),
        value_hash: Hash::new_unique(),
    }
}

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = TapeStore::open_primary(temp_dir.path())?;

    // TrackOps - store track metadata
    let tape_address = Address::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Address::new([i as u8; 32]);
        let info = sample_track(tape_address, (i - 1) as u64);
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Verify track
    let track1 = Address::new([1; 32]);
    let info = store.get_track(track1)?.unwrap();
    println!("Track 1 tape: {:?}", info.tape);

    // SliceDataOps - store slices
    let track_address = Address::new([1; 32]);
    for spool_id in 0..10 {
        store.put_slice(
            SpoolIndex(spool_id),
            track_address,
            vec![spool_id as u8; 1024],
        )?;
    }
    println!("Stored 10 slices for track 1");

    // Query slices by spool
    let spool_5_slices = store.iter_slices_by_spool(SpoolIndex(5))?;
    println!("Spool 5 has {} slices", spool_5_slices.len());

    // Get specific slice
    let slice_data = store.get_slice(SpoolIndex(5), track_address)?.unwrap();
    println!("Slice (5, track1) has {} bytes", slice_data.len());

    // Delete slice
    store.delete_slice(SpoolIndex(9), track_address)?;
    println!("Deleted slice (9, track1)");

    // SpoolOps - NOT epoch-namespaced
    let mut rows = Vec::new();
    for spool_id in [0u64, 5, 10] {
        rows.push((SpoolIndex(spool_id), SpoolState::new_scanning()));
    }
    store.put_spool_states(&rows)?;
    println!("Wrote 3 spool rows");

    // List every spool row
    let spools = store.get_spool_states(None, STORED_SPOOL_COUNT)?;
    println!("All spools: {:?}", spools);

    // The missing_list
    let now = WallTime::now();
    let missing = MissingTrack::new_handover(SpoolIndex(5), track_address, now);
    store.put_missing_track(&missing)?;
    println!("Added a missing track for spool 5");

    let is_listed = store.has_missing_track(SpoolIndex(5), track_address)?;
    println!("Is listed: {}", is_listed);

    // MetaOps - node state
    store.set_sync_cursor(SlotNumber(12345), None)?;
    println!("Set sync cursor 12345");

    // Verify meta state
    let cursor = store.get_sync_cursor()?.unwrap();
    println!("Sync cursor: {:?}", cursor);

    Ok(())
}
