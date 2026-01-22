//! Atomic operations example using operation traits
//!
//! Run with: cargo run --example atomic_operations

use tape_store::{
    error::Result,
    ops::*,
    types::*,
    TapeStore,
};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = TapeStore::open_primary(temp_dir.path())?;

    // TrackInfoOps - store track metadata
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo::new(tape_address, EpochNumber(0));
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Certify a track
    let track1 = Pubkey::new([1; 32]);
    store.certify_track(track1, EpochNumber(100))?;
    println!("Track 1 marked as certified at epoch 100");

    // Verify certification
    let info = store.get_track_info(track1)?.unwrap();
    println!("Track 1 certified_epoch: {:?}", info.certified_epoch);

    // SliceDataOps - store primary and recovery slices
    let track_address = Pubkey::new([1; 32]);
    for spool_id in 0..10 {
        let primary = PrimarySliceData::new(vec![spool_id as u8; 1024], 0);
        let recovery = RecoverySliceData::new(vec![spool_id as u8 + 100; 1024], 0);
        store.put_both_slices(spool_id, track_address, primary, recovery)?;
    }
    println!("Stored 10 primary+recovery slice pairs for track 1");

    // Query slices by spool
    let spool_5_slices: Vec<_> = store
        .iter_primary_slices_by_spool(5)?
        .map(|r| r.unwrap())
        .collect();
    println!("Spool 5 has {} primary slices", spool_5_slices.len());

    // Get specific slice
    let primary = store.get_primary_slice(5, track_address)?.unwrap();
    println!("Primary slice (5, track1) has {} bytes", primary.symbols.len());

    // Delete slices atomically
    store.delete_both_slices(9, track_address)?;
    println!("Deleted slice pair (9, track1)");

    // SpoolOps - epoch-namespaced spool management
    let epoch = EpochNumber(100);
    for spool_id in [0u16, 5, 10] {
        store.set_spool_status(epoch, spool_id, SpoolStatus::Active)?;
    }
    println!("Set 3 spools as Active for epoch 100");

    // Iterate assigned spools
    let assigned: Vec<_> = store
        .iter_assigned_spools(epoch)?
        .map(|r| r.unwrap())
        .collect();
    println!("Assigned spools in epoch 100: {:?}", assigned);

    // Pending recovery operations
    store.add_pending_recovery(epoch, 5, SliceType::Primary, track_address)?;
    println!("Added pending primary recovery for spool 5");

    let has_pending = store.has_pending_recovery(epoch, 5, SliceType::Primary, track_address)?;
    println!("Has pending recovery: {}", has_pending);

    // SliceInfoOps - store erasure coding metadata
    let slice_info = SliceInfo {
        encoding_type: EncodingType::Rotated,
        unencoded_length: 32 * 1024 * 1024,
        primary: vec![Hash::default(); 1024],
        recovery: vec![Hash::default(); 1024],
    };
    store.put_slice_info(track_address, slice_info)?;
    println!("Stored slice info with 1024 primary + 1024 recovery hashes");

    // MetaOps - node state
    store.set_node_status(NodeStatus::Active)?;
    store.set_current_epoch(epoch)?;
    store.set_sync_cursor(SlotNumber(12345))?;
    println!("Set node to Active, epoch 100, sync cursor 12345");

    // Verify meta state
    let status = store.get_node_status()?.unwrap();
    let current_epoch = store.get_current_epoch()?.unwrap();
    let cursor = store.get_sync_cursor()?.unwrap();
    println!("Node status: {:?}, epoch: {:?}, cursor: {:?}", status, current_epoch, cursor);

    Ok(())
}
