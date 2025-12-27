//! Read replica example demonstrating secondary database patterns
//!
//! This example demonstrates:
//! - Opening read-only replicas for static reads
//! - Opening secondary instances with catch-up sync
//! - Background sync loop patterns
//! - Verifying operation traits work on secondaries
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
    // Create temporary directories for primary and secondary
    let temp_dir = tempfile::tempdir()?;
    let primary_path = temp_dir.path().join("primary");
    let secondary_path = temp_dir.path().join("secondary");

    println!("=== Setting up Primary Database ===\n");
    println!("Primary path: {:?}\n", primary_path);

    // Create and populate primary database
    let primary = TapeStore::open_primary(&primary_path)?;

    println!("Populating primary database with sample data...");

    // Add some tapes
    for i in 1..=3 {
        let tape = TapeData {
            id: TapeNumber(i),
            authority: StoredPubkey::new([i as u8; 32]),
            capacity: 10_000_000 * i,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        primary.put_tape(&tape)?;
        println!("  Created tape {}", i);
    }

    println!("✓ Primary database populated\n");

    // Get initial stats
    let initial_stats = primary.get_storage_stats()?;
    println!("Initial stats:");
    println!("  Tapes: {}", initial_stats.tape_count);
    println!("  Tracks: {}", initial_stats.track_count);
    println!();

    // ========================================================================
    // Part 1: Read-Only Replica
    // ========================================================================
    println!("=== Part 1: Read-Only Replica (Static Snapshot) ===\n");

    // Close primary to ensure read-only can open
    drop(primary);

    println!("Opening read-only replica...");
    let read_only = TapeStore::open_read_only(&primary_path)?;
    println!("✓ Read-only replica opened\n");

    println!("Reading from read-only replica:");

    // Read tape data
    let tape1 = read_only.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    println!("  Tape 1: {:?}", tape1.is_some());

    // Use operation traits
    let tape_by_addr = read_only.get_tape_by_address(&StoredPubkey::new([1; 32]))?;
    println!("  Tape by address: {:?}", tape_by_addr.is_some());

    // Get stats
    let ro_stats = read_only.get_storage_stats()?;
    println!("  Stats: {} tapes\n", ro_stats.tape_count);

    println!("✓ Read-only replica works correctly");
    println!("Note: Read-only replicas show a static snapshot at open time\n");

    drop(read_only);

    // ========================================================================
    // Part 2: Secondary Instance with Manual Sync
    // ========================================================================
    println!("=== Part 2: Secondary Instance with Manual Sync ===\n");

    // Reopen primary
    let primary = TapeStore::open_primary(&primary_path)?;

    // Open secondary
    println!("Opening secondary instance...");
    println!("Secondary path: {:?}\n", secondary_path);
    let secondary = TapeStore::open_secondary(&primary_path, &secondary_path)?;
    println!("✓ Secondary instance opened\n");

    // Initial sync
    println!("Performing initial catch-up...");
    secondary.catch_up_with_primary()?;
    println!("✓ Initial sync complete\n");

    // Verify data is visible
    let sec_stats = secondary.get_storage_stats()?;
    println!("Secondary stats after initial sync:");
    println!("  Tapes: {}\n", sec_stats.tape_count);

    // Write new data to primary
    println!("Writing new tape to primary...");
    let new_tape = TapeData {
        id: TapeNumber(10),
        authority: StoredPubkey::new([10; 32]),
        capacity: 50_000_000,
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };
    primary.put_tape(&new_tape)?;
    println!("✓ Tape 10 written to primary\n");

    // Secondary doesn't see it yet
    let tape10_before = secondary.get::<TapesById>(&TapeKey(TapeNumber(10)))?;
    println!("Secondary sees tape 10 before sync: {}", tape10_before.is_some());

    // Sync secondary
    println!("Syncing secondary...");
    secondary.catch_up_with_primary()?;
    println!("✓ Sync complete\n");

    // Now secondary sees it
    let tape10_after = secondary.get::<TapesById>(&TapeKey(TapeNumber(10)))?;
    println!("Secondary sees tape 10 after sync: {}", tape10_after.is_some());
    println!("✓ Secondary successfully caught up with primary\n");

    // ========================================================================
    // Part 3: Background Sync Loop Pattern
    // ========================================================================
    println!("=== Part 3: Background Sync Loop (1-second interval) ===\n");

    // Wrap secondary in Arc for sharing across threads
    let secondary = Arc::new(secondary);
    let secondary_clone = Arc::clone(&secondary);

    // Flag to stop the sync loop
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    // Spawn background sync thread
    let sync_thread = thread::spawn(move || {
        let mut sync_count = 0;
        while running_clone.load(std::sync::atomic::Ordering::Relaxed) {
            if let Err(e) = secondary_clone.catch_up_with_primary() {
                eprintln!("Sync error: {}", e);
            } else {
                sync_count += 1;
                if sync_count <= 3 {
                    println!("  [Sync thread] Completed sync #{}", sync_count);
                }
            }
            thread::sleep(Duration::from_secs(1));
        }
        println!("  [Sync thread] Stopped after {} syncs", sync_count);
    });

    println!("✓ Background sync thread started\n");

    // Simulate writes to primary while secondary syncs in background
    println!("Writing data to primary while secondary syncs...");

    for i in 11..=13 {
        thread::sleep(Duration::from_millis(500));

        let tape = TapeData {
            id: TapeNumber(i),
            authority: StoredPubkey::new([i as u8; 32]),
            capacity: 10_000_000,
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };
        primary.put_tape(&tape)?;
        println!("  Wrote tape {} to primary", i);
    }

    // Give sync thread time to catch up
    thread::sleep(Duration::from_millis(1500));

    // Verify secondary has all the new tapes
    println!("\nVerifying secondary caught up:");
    for i in 11..=13 {
        let tape = secondary.get::<TapesById>(&TapeKey(TapeNumber(i)))?;
        println!("  Tape {} in secondary: {}", i, tape.is_some());
    }
    println!();

    // ========================================================================
    // Part 4: Operation Traits on Secondary
    // ========================================================================
    println!("=== Part 4: Operation Traits on Secondary Instance ===\n");

    // All operation traits work on secondary instances
    println!("Testing TapeOps::get_tape_by_address()");
    let tape_by_addr = secondary.get_tape_by_address(&StoredPubkey::new([1; 32]))?;
    println!("✓ Found tape: {:?}\n", tape_by_addr.map(|t| t.id.0));

    println!("Testing StatsOps::get_storage_stats()");
    let final_stats = secondary.get_storage_stats()?;
    println!("✓ Final stats:");
    println!("  Tapes: {}\n", final_stats.tape_count);

    // ========================================================================
    // Cleanup
    // ========================================================================
    println!("=== Cleanup ===\n");

    // Stop sync thread
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    sync_thread.join().unwrap();

    println!("✓ Background sync thread stopped\n");

    // ========================================================================
    // Summary
    // ========================================================================
    println!("=== Summary ===\n");
    println!("✓ Read-only replicas provide static snapshots");
    println!("✓ Secondary instances can catch up with primary");
    println!("✓ Background sync loops keep secondaries up-to-date");
    println!("✓ All operation traits work on secondary instances");
    println!("✓ Secondaries enable read scaling without impacting primary");
    println!("\nDeployment patterns:");
    println!("  - Use read-only for analytics (static snapshot)");
    println!("  - Use secondary with 1s sync for near-real-time reads");
    println!("  - Use secondary with 30s sync for reduced overhead");
    println!("  - Multiple secondaries can scale reads horizontally");

    Ok(())
}
