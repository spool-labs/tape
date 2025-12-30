//! Atomic operations example using operation traits
//!
//! Run with: cargo run --example atomic_operations

use tape_store::{
    error::Result,
    ops::{SliceOps, SpoolOps, StatsOps, TrackOps},
    types::*,
    TapeStore,
};

fn main() -> Result<()> {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = TapeStore::open_primary(temp_dir.path())?;

    // TrackOps - minimal track info
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            commitment_hash: Hash::from([100 + i as u8; 32]),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Increment slice count
    let track1 = Pubkey::new([1; 32]);
    let count = store.increment_slice_count(track1)?;
    println!("Track 1 slice count after increment: {}", count);

    // Mark as certified
    store.mark_certified(track1, EpochNumber(100))?;
    println!("Track 1 marked as certified at epoch 100");

    // Verify
    let info = store.get_track_info(track1)?.unwrap();
    println!("Track 1 certified_epoch: {}", info.certified_epoch.0);

    // SliceOps - new key structure (spool_idx, track_address)
    let track_address = Pubkey::new([1; 32]);
    for spool_idx in 0..10 {
        let meta = SliceMeta {
            len: 32 * 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 1000000,
        };
        store.put_slice(spool_idx, track_address, vec![spool_idx as u8; 1024], meta)?;
    }
    println!("Stored 10 slices for track 1");

    // Query by spool
    let spool_5_slices = store.get_spool_slices(5)?;
    println!("Spool 5 has {} slices", spool_5_slices.len());

    // Get specific slice
    let (data, meta) = store.get_slice(5, track_address)?.unwrap();
    println!("Slice (5, track1) has {} bytes, len={}", data.len(), meta.len);

    // Delete slice
    store.delete_slice(9, track_address)?;
    println!("Deleted slice (9, track1)");

    // SpoolOps - spool management
    for spool_idx in [0, 5, 10] {
        let state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };
        store.put_spool_state(spool_idx, state)?;
    }
    println!("Registered 3 spools as owned");

    let my_spools = store.get_my_spools()?;
    println!("My spools: {:?}", my_spools);

    // StatsOps
    let stats = store.get_storage_stats()?;
    println!("\nStats: {} tracks, {} slices, {} spools",
        stats.track_count, stats.slice_meta_count, stats.spool_count);

    Ok(())
}
