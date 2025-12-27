//! Read replica example demonstrating secondary database patterns
//!
//! Run with: cargo run --example read_replica

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tape_store::{
    columns::*,
    ops::{StatsOps, TapeOps},
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
        let tape = TapeData {
            id: TapeNumber(i),
            authority: Pubkey::new([i as u8; 32]),
            capacity: 10_000_000 * i,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        primary.put_tape(&tape)?;
    }

    let stats = primary.get_storage_stats()?;
    println!("Primary: {} tapes", stats.tape_count);

    // Read-only replica (static snapshot)
    drop(primary);
    let read_only = TapeStore::open_read_only(&primary_path)?;
    let tape1 = read_only.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    println!("Read-only sees tape 1: {}", tape1.is_some());
    drop(read_only);

    // Secondary with manual sync
    let primary = TapeStore::open_primary(&primary_path)?;
    let secondary = TapeStore::open_secondary(&primary_path, &secondary_path)?;
    secondary.catch_up_with_primary()?;

    // Write to primary, secondary doesn't see it yet
    let new_tape = TapeData {
        id: TapeNumber(10),
        authority: Pubkey::new([10; 32]),
        capacity: 50_000_000,
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };
    primary.put_tape(&new_tape)?;

    let before = secondary.get::<TapesById>(&TapeKey(TapeNumber(10)))?;
    println!("Before sync: {}", before.is_some());

    secondary.catch_up_with_primary()?;

    let after = secondary.get::<TapesById>(&TapeKey(TapeNumber(10)))?;
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
        let tape = TapeData {
            id: TapeNumber(i),
            authority: Pubkey::new([i as u8; 32]),
            capacity: 10_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        primary.put_tape(&tape)?;
    }

    thread::sleep(Duration::from_millis(1500));

    // Verify sync
    for i in 11..=13 {
        let tape = secondary.get::<TapesById>(&TapeKey(TapeNumber(i)))?;
        println!("Tape {}: {}", i, tape.is_some());
    }

    // Operation traits work on secondary
    let found = secondary.get_tape_by_address(&Pubkey::new([1; 32]))?;
    println!("Found by address: {:?}", found.map(|t| t.id.0));

    let stats = secondary.get_storage_stats()?;
    println!("Secondary stats: {} tapes", stats.tape_count);

    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sync_thread.join().unwrap();

    Ok(())
}
