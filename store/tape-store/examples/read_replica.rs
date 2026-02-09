//! Read replica example demonstrating secondary database patterns
//!
//! Run with: cargo run --example read_replica

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tape_crypto::Hash;
use tape_store::{ops::*, types::*, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let primary_path = temp_dir.path().join("primary");
    let secondary_path = temp_dir.path().join("secondary");

    // Setup primary
    let primary = TapeStore::open_primary(&primary_path)?;
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=3 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            tape_address,
            spool_allocation: SpoolAllocation::SpoolGroup(3),
            original_size: 1024,
            encoding_type: 1,
            encoding_params: 0,
            commitment_hash: Hash::default(),
        };
        primary.put_track(track_address, info)?;
    }

    // Set some metadata
    primary.set_node_status(NodeStatus::Active)?;
    primary.set_current_epoch(EpochNumber(100))?;
    println!("Primary: created 3 tracks");

    // Read-only replica (static snapshot)
    drop(primary);
    let read_only = TapeStore::open_read_only(&primary_path)?;
    let track1 = read_only.get_track(Pubkey::new([1; 32]))?;
    println!("Read-only sees track 1: {}", track1.is_some());
    drop(read_only);

    // Secondary with manual sync
    let primary = TapeStore::open_primary(&primary_path)?;
    let secondary = TapeStore::open_secondary(&primary_path, &secondary_path)?;
    secondary.catch_up_with_primary()?;

    // Write to primary, secondary doesn't see it yet
    let new_track = Pubkey::new([10; 32]);
    let new_info = TrackInfo {
        tape_address,
        spool_allocation: SpoolAllocation::SpoolGroup(3),
        original_size: 1024,
        encoding_type: 1,
        encoding_params: 0,
        commitment_hash: Hash::default(),
    };
    primary.put_track(new_track, new_info)?;

    let before = secondary.get_track(new_track)?;
    println!("Before sync: {}", before.is_some());

    secondary.catch_up_with_primary()?;

    let after = secondary.get_track(new_track)?;
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
    for i in 11..=13u8 {
        thread::sleep(Duration::from_millis(500));
        let track_address = Pubkey::new([i; 32]);
        let info = TrackInfo {
            tape_address,
            spool_allocation: SpoolAllocation::SpoolGroup(3),
            original_size: 1024,
            encoding_type: 1,
            encoding_params: 0,
            commitment_hash: Hash::default(),
        };
        primary.put_track(track_address, info)?;
    }

    thread::sleep(Duration::from_millis(1500));

    // Verify sync
    for i in 11..=13u8 {
        let track = secondary.get_track(Pubkey::new([i; 32]))?;
        println!("Track {}: {}", i, track.is_some());
    }

    // Operation traits work on secondary
    let found = secondary.get_track(Pubkey::new([1; 32]))?;
    println!("Found track 1: {:?}", found.is_some());

    let status = secondary.get_node_status()?;
    println!("Secondary sees node status: {:?}", status);

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sync_thread.join().unwrap();

    Ok(())
}
