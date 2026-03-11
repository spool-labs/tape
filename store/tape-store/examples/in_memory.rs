//! In-memory store example for testing and prototyping
//!
//! This example demonstrates:
//! - Using TapeStore with MemoryStore backend (no filesystem)
//! - Quick setup for unit tests
//! - All operations work identically to RocksDB backend
//!
//! Run with: cargo run --example in_memory

use tape_store::{ops::*, types::*, MemoryStore, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = TapeStore::new(MemoryStore::new());

    // Create tracks
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=3 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            tape_address,
            spool_group: SpoolGroup(3),
            original_size: 1024,
            encoding_type: 1,
            encoding_params: 0,
            stripe_size: 0,
            stripe_count: 0,
            commitment: vec![],
        };
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve by address
    let track1 = Pubkey::new([1; 32]);
    let retrieved = store.get_track(track1)?;
    println!("Retrieved track 1: {:?}", retrieved.is_some());

    // Store slices for track 1
    let track_address = Pubkey::new([1; 32]);
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
        store.set_spool_state(spool_id, SpoolState::Active { epoch: EpochNumber(0) })?;
    }

    let spools = store.iter_all_spools()?;
    println!("All spools: {:?}", spools);

    // Metadata
    store.set_sync_cursor(SlotNumber(12345))?;

    println!("\nFinal state:");
    println!("  Sync cursor: {:?}", store.get_sync_cursor()?);

    Ok(())
}
