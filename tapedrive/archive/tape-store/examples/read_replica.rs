//! Read replica example demonstrating secondary database patterns
//!
//! Run with: cargo run --example read_replica

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tape_store::{
    ops::{StatsOps, TrackOps},
    types::*,
    TapeStore,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let primary_path = temp_dir.path().join("primary");
    let secondary_path = temp_dir.path().join("secondary");

    // Setup primary
    let primary = TapeStore::open_primary(&primary_path)?;
    for i in 1..=3 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            commitment_hash: Hash::from([i as u8; 32]),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };
        primary.put_track_info(track_address, info)?;
    }

    let stats = primary.get_storage_stats()?;
    println!("Primary: {} tracks", stats.track_count);

    // Read-only replica (static snapshot)
    drop(primary);
    let read_only = TapeStore::open_read_only(&primary_path)?;
    let track1 = read_only.get_track_info(Pubkey::new([1; 32]))?;
    println!("Read-only sees track 1: {}", track1.is_some());
    drop(read_only);

    // Secondary with manual sync
    let primary = TapeStore::open_primary(&primary_path)?;
    let secondary = TapeStore::open_secondary(&primary_path, &secondary_path)?;
    secondary.catch_up_with_primary()?;

    // Write to primary, secondary doesn't see it yet
    let new_track = Pubkey::new([10; 32]);
    primary.put_track_info(new_track, TrackInfo {
        commitment_hash: Hash::from([10; 32]),
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    })?;

    let before = secondary.get_track_info(new_track)?;
    println!("Before sync: {}", before.is_some());

    secondary.catch_up_with_primary()?;

    let after = secondary.get_track_info(new_track)?;
    println!("After sync: {}", after.is_some());

    // Background sync loop
    let secondary = Arc::new(secondary);
    let secondary_clone = Arc::clone(&secondary);
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    let sync_thread = thread::spawn(move || {
        while running_clone.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = secondary_clone.catch_up_with_primary();
            thread::sleep(Duration::from_secs(1));
        }
    });

    // Write while syncing
    for i in 11..=13 {
        thread::sleep(Duration::from_millis(500));
        let track_address = Pubkey::new([i as u8; 32]);
        primary.put_track_info(track_address, TrackInfo {
            commitment_hash: Hash::from([i as u8; 32]),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        })?;
    }

    thread::sleep(Duration::from_millis(1500));

    // Verify sync
    for i in 11..=13 {
        let track = secondary.get_track_info(Pubkey::new([i as u8; 32]))?;
        println!("Track {}: {}", i, track.is_some());
    }

    // Operation traits work on secondary
    let found = secondary.get_track_info(Pubkey::new([1; 32]))?;
    println!("Found track 1: {:?}", found.is_some());

    let stats = secondary.get_storage_stats()?;
    println!("Secondary stats: {} tracks", stats.track_count);

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sync_thread.join().unwrap();

    Ok(())
}
