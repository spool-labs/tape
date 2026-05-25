//! Read replica example demonstrating secondary database patterns
//!
//! Run with: cargo run --example read_replica

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tape_core::spooler::GroupIndex;
use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
use tape_core::types::{StorageUnits, TrackNumber};
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_store::{ops::*, TapeStore};

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
    let temp_dir = tempfile::tempdir()?;
    let primary_path = temp_dir.path().join("primary");
    let secondary_path = temp_dir.path().join("secondary");

    // Setup primary
    let primary = TapeStore::open_primary(&primary_path)?;
    let tape_address = Address::new([0xAA; 32]);
    for i in 1..=3 {
        let track_address = Address::new([i as u8; 32]);
        let info = sample_track(tape_address, (i - 1) as u64);
        primary.put_track(track_address, info)?;
    }

    println!("Primary: created 3 tracks");

    // Read-only replica (static snapshot)
    drop(primary);
    let read_only = TapeStore::open_read_only(&primary_path)?;
    let track1 = read_only.get_track(Address::new([1; 32]))?;
    println!("Read-only sees track 1: {}", track1.is_some());
    drop(read_only);

    // Secondary with manual sync
    let primary = TapeStore::open_primary(&primary_path)?;
    let secondary = TapeStore::open_secondary(&primary_path, &secondary_path)?;
    secondary.catch_up_with_primary()?;

    // Write to primary, secondary doesn't see it yet
    let new_track = Address::new([10; 32]);
    let new_info = sample_track(tape_address, 9);
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
        let track_address = Address::new([i; 32]);
        let info = sample_track(tape_address, (i - 1) as u64);
        primary.put_track(track_address, info)?;
    }

    thread::sleep(Duration::from_millis(1500));

    // Verify sync
    for i in 11..=13u8 {
        let track = secondary.get_track(Address::new([i; 32]))?;
        println!("Track {}: {}", i, track.is_some());
    }

    // Operation traits work on secondary
    let found = secondary.get_track(Address::new([1; 32]))?;
    println!("Found track 1: {:?}", found.is_some());

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sync_thread.join().unwrap();

    Ok(())
}
