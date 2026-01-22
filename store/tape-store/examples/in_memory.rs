//! In-memory store example for testing and prototyping
//!
//! This example demonstrates:
//! - Using TapeStore with MemoryStore backend (no filesystem)
//! - Quick setup for unit tests
//! - All operations work identically to RocksDB backend
//!
//! Run with: cargo run --example in_memory

use tape_store::{
    ops::*,
    types::*,
    MemoryStore, TapeStore,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = TapeStore::new(MemoryStore::new());

    // Create tracks with new TrackInfo structure
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=3 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo::new(tape_address, EpochNumber(0));
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve by address
    let track1 = Pubkey::new([1; 32]);
    let retrieved = store.get_track_info(track1)?;
    println!("Retrieved track 1: {:?}", retrieved.is_some());

    // Store primary and recovery slices for track 1
    let track_address = Pubkey::new([1; 32]);
    for spool_id in 0..5u16 {
        let primary = PrimarySliceData::new(vec![spool_id as u8; 1024], 0);
        let recovery = RecoverySliceData::new(vec![spool_id as u8 + 100; 1024], 0);
        store.put_both_slices(spool_id, track_address, primary, recovery)?;
    }

    // Query slices by spool
    for spool_id in 0..5u16 {
        let slices: Vec<_> = store
            .iter_primary_slices_by_spool(spool_id)?
            .map(|r| r.unwrap())
            .collect();
        println!("Spool {} has {} primary slices", spool_id, slices.len());
    }

    // Register epoch-namespaced spool status
    let epoch = EpochNumber(100);
    for spool_id in 0..3u16 {
        store.set_spool_status(epoch, spool_id, SpoolStatus::Active)?;
    }

    let assigned: Vec<_> = store
        .iter_assigned_spools(epoch)?
        .map(|r| r.unwrap())
        .collect();
    println!("Assigned spools in epoch 100: {:?}", assigned);

    // Metadata
    store.set_node_status(NodeStatus::Active)?;
    store.set_current_epoch(epoch)?;
    store.set_sync_cursor(SlotNumber(12345))?;

    println!("\nFinal state:");
    println!("  Node status: {:?}", store.get_node_status()?);
    println!("  Current epoch: {:?}", store.get_current_epoch()?);
    println!("  Sync cursor: {:?}", store.get_sync_cursor()?);

    Ok(())
}
