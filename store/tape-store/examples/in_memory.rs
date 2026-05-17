//! In-memory store example for testing and prototyping
//!
//! This example demonstrates:
//! - Using TapeStore with MemoryStore backend (no filesystem)
//! - Quick setup for unit tests
//! - All operations work identically to RocksDB backend
//!
//! Run with: cargo run --example in_memory

use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::{ops::*, types::*, MemoryStore, TapeStore};

fn sample_track(tape: Address, track_number: u64) -> CompressedTrack {
    CompressedTrack {
        tape,
        key: Hash::new_unique(),
        track_number: TrackNumber(track_number),
        kind: TrackKind::Raw as u64,
        state: TrackState::Certified as u64,
        size: StorageUnits::from_bytes(1024),
        group: GroupIndex(3),
        value_hash: Hash::new_unique(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = TapeStore::new(MemoryStore::new());

    // Create tracks
    let tape_address = Address::new([0xAA; 32]);
    for i in 1..=3 {
        let track_address = Address::new([i as u8; 32]);
        let info = sample_track(tape_address, (i - 1) as u64);
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve by address
    let track1 = Address::new([1; 32]);
    let retrieved = store.get_track(track1)?;
    println!("Retrieved track 1: {:?}", retrieved.is_some());

    // Store slices for track 1
    let track_address = Address::new([1; 32]);
    for spool_id in 0..5u16 {
        store.put_slice(spool_id, track_address, vec![spool_id as u8; 1024])?;
    }

    // Query slices by spool
    for spool_id in 0..5u16 {
        let slices = store.iter_slices_by_spool(spool_id)?;
        println!("Spool {} has {} slices", spool_id, slices.len());
    }

    // Spool status (NOT epoch-namespaced)
    for spool_id in 0..3u16 {
        store.set_spool_state(spool_id, SpoolState::new(SpoolStatus::Active, EpochNumber(0)))?;
    }

    let spools = store.iter_all_spools()?;
    println!("All spools: {:?}", spools);

    // Metadata
    store.set_sync_cursor(SlotNumber(12345))?;

    println!("\nFinal state:");
    println!("  Sync cursor: {:?}", store.get_sync_cursor()?);

    Ok(())
}
