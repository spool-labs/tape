//! In-memory store example for testing and prototyping
//!
//! This example demonstrates:
//! - Using TapeStore with MemoryStore backend (no filesystem)
//! - Quick setup for unit tests
//! - All operations work identically to RocksDB backend
//!
//! Run with: cargo run --example in_memory

use tape_store::{
    ops::{SliceOps, SpoolOps, StatsOps, TrackOps},
    types::*,
    MemoryStore, TapeStore,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let store = TapeStore::new(MemoryStore::new());

    // Create tracks with minimal TrackInfo
    for i in 1..=3 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            commitment_hash: Hash::from([i as u8; 32]),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve by address
    let track1 = Pubkey::new([1; 32]);
    let retrieved = store.get_track_info(track1)?;
    println!("Retrieved track 1: {:?}", retrieved.is_some());

    // Store slices for track 1 with new key structure
    let track_address = Pubkey::new([1; 32]);
    for spool_idx in 0..5 {
        let meta = SliceMeta {
            len: 32 * 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            received_at: 1000000,
        };
        store.put_slice(spool_idx, track_address, vec![spool_idx as u8; 1024], meta)?;
    }

    // Query slices by spool
    for spool_idx in 0..5 {
        let slices = store.get_spool_slices(spool_idx)?;
        println!("Spool {} has {} slices", spool_idx, slices.len());
    }

    // Register spools as owned
    for spool_idx in 0..3 {
        let state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };
        store.put_spool_state(spool_idx, state)?;
    }

    let my_spools = store.get_my_spools()?;
    println!("My spools: {:?}", my_spools);

    // Stats
    let stats = store.get_storage_stats()?;
    println!("\nStorage stats:");
    println!("  Tracks:      {}", stats.track_count);
    println!("  Slice Meta:  {}", stats.slice_meta_count);
    println!("  Slice Data:  {}", stats.slice_data_count);
    println!("  Spools:      {}", stats.spool_count);

    Ok(())
}
