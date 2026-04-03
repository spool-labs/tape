//! Basic usage example for TapeStore
//!
//! Run with: cargo run --example basic_usage

use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::{ops::*, types::*, TapeStore};

fn sample_track(tape: Address, track_number: u64) -> CompressedTrack {
    CompressedTrack {
        tape,
        key: Hash::new_unique(),
        track_number: TrackNumber(track_number),
        kind: TrackKind::Raw as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(1024),
        spool_group: SpoolGroup(3),
        value_hash: Hash::new_unique(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let store = TapeStore::open_primary(temp_dir.path())?;

    // Store tracks
    let tape_address = Address::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Address::new([i as u8; 32]);
        let info = sample_track(tape_address, (i - 1) as u64);
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve a track
    let track1 = store.get_track(Address::new([1; 32]))?;
    println!(
        "Track 1 tape address: {:?}",
        track1.map(|track| track.tape)
    );

    // Store slices
    let track_address = Address::new([1; 32]);
    for spool_id in 0..5u16 {
        store.put_slice(spool_id, track_address, vec![spool_id as u8; 1024])?;
    }
    println!("Stored 5 slices for track 1");

    // Query slices by spool
    let spool_slices = store.iter_slices_by_spool(0)?;
    println!("Spool 0 has {} slices", spool_slices.len());

    // Spool status (NOT epoch-namespaced)
    for spool_id in 0..3u16 {
        store.set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(0)))?;
    }

    // Iterate all spools
    let spools = store.iter_all_spools()?;
    println!("Active spools: {:?}", spools);

    // Tape info
    let tape_info = TapeInfo {
        end_epoch: EpochNumber(200),
        next_track_number: TrackNumber(0),
    };
    store.put_tape(tape_address, tape_info)?;
    println!("Stored tape info");

    // Object info
    let obj_address = Address::new_unique();
    store.put_object_info(
        obj_address,
        ObjectInfo::Valid {
            track_address,
            registered_epoch: EpochNumber(5),
            certified_epoch: Some(EpochNumber(6)),
            slot: SlotNumber(50),
        },
    )?;
    println!("Stored object info");

    // Verify storage
    println!("\nFinal state:");
    println!(
        "  Has tape info: {}",
        store.get_tape(tape_address)?.is_some()
    );
    Ok(())
}
