//! Atomic operations example using operation traits
//!
//! Run with: cargo run --example atomic_operations

use tape_store::{error::Result, ops::*, types::*, TapeStore};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = TapeStore::open_primary(temp_dir.path())?;

    // TrackOps - store track metadata
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            tape_address,
            spool_allocation: SpoolAllocation::SpoolGroup(3),
            original_size: 1024,
            stripe_size: 1024,
            stripe_count: 1,
            encoding_type: 1,
            encoding_params: 0,
            commitment: vec![],
        };
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Verify track
    let track1 = Pubkey::new([1; 32]);
    let info = store.get_track(track1)?.unwrap();
    println!("Track 1 tape_address: {:?}", info.tape_address);

    // SliceDataOps - store slices
    let track_address = Pubkey::new([1; 32]);
    for spool_id in 0..10 {
        store.put_slice(spool_id, track_address, vec![spool_id as u8; 1024])?;
    }
    println!("Stored 10 slices for track 1");

    // Query slices by spool
    let spool_5_slices = store.iter_slices_by_spool(5)?;
    println!("Spool 5 has {} slices", spool_5_slices.len());

    // Get specific slice
    let slice_data = store.get_slice(5, track_address)?.unwrap();
    println!("Slice (5, track1) has {} bytes", slice_data.len());

    // Delete slice
    store.delete_slice(9, track_address)?;
    println!("Deleted slice (9, track1)");

    // SpoolOps - NOT epoch-namespaced
    for spool_id in [0u16, 5, 10] {
        store.set_spool_status(spool_id, SpoolStatus::Active)?;
    }
    println!("Set 3 spools as Active");

    // Iterate all spools
    let spools = store.iter_all_spools()?;
    println!("All spools: {:?}", spools);

    // Pending recovery operations
    store.add_pending_recovery(5, track_address)?;
    println!("Added pending recovery for spool 5");

    let has_pending = store.has_pending_recovery(5, track_address)?;
    println!("Has pending recovery: {}", has_pending);

    // MetaOps - node state
    store.set_node_status(NodeStatus::Active)?;
    store.set_current_epoch(EpochNumber(100))?;
    store.set_sync_cursor(SlotNumber(12345))?;
    println!("Set node to Active, epoch 100, sync cursor 12345");

    // Verify meta state
    let status = store.get_node_status()?.unwrap();
    let current_epoch = store.get_current_epoch()?.unwrap();
    let cursor = store.get_sync_cursor()?.unwrap();
    println!(
        "Node status: {:?}, epoch: {:?}, cursor: {:?}",
        status, current_epoch, cursor
    );

    Ok(())
}
