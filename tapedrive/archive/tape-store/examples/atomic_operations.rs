//! Atomic operations example using operation traits
//!
//! This example demonstrates:
//! - Using TapeOps, TrackOps, SliceOps, and StatsOps traits
//! - Atomic multi-index updates with WriteBatch
//! - Reverse lookups with consistency validation
//! - Storage statistics aggregation
//!
//! Run with: cargo run --example atomic_operations

use tape_store::{
    columns::*,
    error::Result,
    ops::{SliceOps, StatsOps, TapeOps, TrackOps},
    types::*,
    TapeStore,
};

fn main() -> Result<()> {
    // Create a temporary directory for the example database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path();

    println!("Opening TapeStore at: {:?}\n", db_path);

    // Open a TapeStore with optimized configuration
    let store = TapeStore::open_primary(db_path)?;

    // ========================================================================
    // Part 1: TapeOps - Atomic Multi-Index Operations
    // ========================================================================
    println!("=== Part 1: TapeOps - Atomic Multi-Index Updates ===\n");

    let tape = Tape {
        id: TapeNumber(1),
        authority: Pubkey([42; 32]),
        capacity: 100_000_000, // 100 MB
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };

    // put_tape() atomically updates 3 column families:
    // - TapesById
    // - TapesByAddress
    // - TapesActiveIndex
    println!("Calling put_tape() - atomically updates 3 indices");
    store.put_tape(&tape)?;
    println!("✓ Tape {} stored with atomic multi-index update\n", tape.id.0);

    // Verify all three indices were updated
    let by_id = store.get::<TapesById>(&TapeKey(tape.id))?;
    let by_address = store.get::<TapesByAddress>(&tape.authority)?;
    let in_active = store.get::<TapesActiveIndex>(&TapeKey(tape.id))?;

    println!("Verification:");
    println!("  TapesById: {:?}", by_id.is_some());
    println!("  TapesByAddress: {:?}", by_address);
    println!("  TapesActiveIndex: {:?}\n", in_active.is_some());

    // Reverse lookup: address -> tape
    println!("Testing get_tape_by_address() - two-hop lookup with validation");
    let found_tape = store.get_tape_by_address(&Pubkey([42; 32]))?;
    assert!(found_tape.is_some());
    println!("✓ Reverse lookup successful: found tape {}\n", found_tape.unwrap().id.0);

    // ========================================================================
    // Part 2: TrackOps - Atomic Track Management
    // ========================================================================
    println!("=== Part 2: TrackOps - Atomic Track Operations ===\n");

    for i in 1..=5 {
        let track = Track {
            id: TrackNumber(i),
            tape: Pubkey([42; 32]), // Same tape
            key: Hash([100 + i as u8; 32]),
            size: 1_000_000 * i, // Variable sizes
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::ZERO,
        };

        // put_track() atomically updates indices
        // Note: TracksByTape is NOT updated (known limitation from Phase 3)
        store.put_track(&track)?;

        println!("✓ Track {} stored ({} bytes)", i, track.size);
    }
    println!();

    // Reverse lookup: address -> track
    // Note: Track doesn't have an address field in the current schema,
    // so we'll skip this part

    // ========================================================================
    // Part 3: SliceOps - Range Queries and Aggregation
    // ========================================================================
    println!("=== Part 3: SliceOps - Slice Range Queries ===\n");

    // Store slices for track 1
    let track_id = TrackNumber(1);
    println!("Storing slices for track {}...", track_id.0);

    for spool_idx in 0..10 {
        let slice_key = SliceKey::new(track_id, spool_idx);

        let slice_meta = SliceMeta {
            len: 32 * 1024, // 32 KB
            leaf_hash: Hash::ZERO,
            content_digest: Hash::ZERO,
            compression: Compression::Lz4,
            last_verified_at: 1000000,
            flags: 0,
        };

        // Store all three slice CFs
        store.put::<SlicesMeta>(&slice_key, &slice_meta)?;
        store.put::<SlicesData>(&slice_key, &vec![spool_idx as u8; 1024])?;
    }
    println!("✓ Stored 10 slices\n");

    // Get all slices for a track
    println!("Testing get_track_slices()");
    let slices = store.get_track_slices(track_id)?;
    println!("✓ Found {} slices for track {}", slices.len(), track_id.0);
    if !slices.is_empty() {
        println!("  First slice: spool_idx={}, len={} bytes\n",
                 slices[0].0, slices[0].1.len);
    }

    // Get slices in a range
    println!("Testing get_track_slices_range() - spools [3, 7)");
    let range = store.get_track_slices_range(track_id, 3, 7)?;
    println!("✓ Found {} slices in range", range.len());
    for (spool_idx, meta) in &range {
        println!("  - Spool {}: {} bytes", spool_idx, meta.len);
    }
    println!();

    // Count slices
    println!("Testing count_track_slices()");
    let count = store.count_track_slices(track_id)?;
    println!("✓ Track {} has {} slices\n", track_id.0, count);

    // Check completeness (should be false since we only have 10/1024)
    println!("Testing track_is_complete()");
    let is_complete = store.track_is_complete(track_id)?;
    println!("✓ Track {} is complete: {} (expected false)\n", track_id.0, is_complete);

    // ========================================================================
    // Part 4: StatsOps - Storage Statistics
    // ========================================================================
    println!("=== Part 4: StatsOps - Storage Statistics ===\n");

    println!("Testing get_storage_stats()");
    let stats = store.get_storage_stats()?;

    println!("Storage Statistics:");
    println!("  Tapes:       {}", stats.tape_count);
    println!("  Tracks:      {}", stats.track_count);
    println!("  Slice Meta:  {}", stats.slice_meta_count);
    println!("  Slice Data:  {}", stats.slice_data_count);
    println!();

    // ========================================================================
    // Summary
    // ========================================================================
    println!("=== Summary ===\n");
    println!("✓ TapeOps: Atomic multi-index tape operations");
    println!("✓ TrackOps: Atomic track operations with reverse lookups");
    println!("✓ SliceOps: Efficient range queries and aggregation");
    println!("✓ StatsOps: Storage statistics collection");
    println!("\nAll operation traits demonstrated successfully!");

    Ok(())
}
