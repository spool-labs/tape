//! In-memory store example for testing and prototyping
//!
//! This example demonstrates:
//! - Using TapeStore with MemoryStore backend (no filesystem)
//! - Quick setup for unit tests
//! - All operations work identically to RocksDB backend
//!
//! Run with: cargo run --example in_memory

use tape_store::{
    columns::*,
    ops::{SliceOps, StatsOps, TapeOps, TrackOps},
    types::*,
    MemoryStore, TapeStore,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== In-Memory TapeStore Example ===\n");

    // Create an in-memory store - no filesystem, no cleanup needed
    let store = TapeStore::new(MemoryStore::new());

    println!("Created in-memory TapeStore (no files created)\n");

    // ========================================================================
    // Basic CRUD Operations
    // ========================================================================
    println!("=== Basic CRUD Operations ===\n");

    // Create a tape
    let tape = TapeData {
        id: TapeNumber(1),
        authority: Pubkey::new([1; 32]),
        capacity: 100_000_000,
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };

    // Store using operation trait (atomic multi-index update)
    store.put_tape(&tape)?;
    println!("Stored tape {} with capacity {} bytes", tape.id.0, tape.capacity);

    // Retrieve by ID
    let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    println!("Retrieved tape: {:?}", retrieved.is_some());

    // Retrieve by address (reverse lookup)
    let by_addr = store.get_tape_by_address(&Pubkey::new([1; 32]))?;
    println!("Found by address: {:?}\n", by_addr.map(|t| t.id.0));

    // ========================================================================
    // Tracks and Slices
    // ========================================================================
    println!("=== Tracks and Slices ===\n");

    // Create tracks
    for i in 1..=3 {
        let track = TrackData {
            id: TrackNumber(i),
            tape: Pubkey::new([1; 32]),
            key: Hash::from([i as u8; 32]),
            size: 1024 * 1024, // 1 MB each
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::default(),
        };
        store.put_track(&track)?;
        println!("Created track {}", i);
    }

    // Store slices for track 1
    let track_id = TrackNumber(1);
    for spool_idx in 0..5 {
        let slice_key = SliceKey::new(track_id, spool_idx);
        let meta = SliceMeta {
            len: 32 * 1024,
            leaf_hash: Hash::default(),
            content_digest: Hash::default(),
            compression: Compression::Lz4,
            last_verified_at: 1000000,
            flags: 0,
        };
        store.put::<SlicesMeta>(&slice_key, &meta)?;
        store.put::<SlicesData>(&slice_key, &vec![spool_idx as u8; 1024])?;
    }
    println!("Stored 5 slices for track 1\n");

    // Query slices
    let slices = store.get_track_slices(track_id)?;
    println!("Track 1 has {} slices", slices.len());

    let count = store.count_track_slices(track_id)?;
    println!("Slice count: {}\n", count);

    // ========================================================================
    // Statistics
    // ========================================================================
    println!("=== Storage Statistics ===\n");

    let stats = store.get_storage_stats()?;
    println!("Tapes:       {}", stats.tape_count);
    println!("Tracks:      {}", stats.track_count);
    println!("Slice Meta:  {}", stats.slice_meta_count);
    println!("Slice Data:  {}", stats.slice_data_count);

    // ========================================================================
    // Why Use In-Memory?
    // ========================================================================
    println!("\n=== Use Cases for In-Memory Store ===\n");
    println!("1. Unit tests - fast, isolated, no cleanup needed");
    println!("2. Integration tests - verify logic without I/O");
    println!("3. Prototyping - quick iteration without persistence");
    println!("4. Benchmarking - isolate CPU from disk I/O");
    println!("5. Template for custom backends - copy store-memory crate");

    println!("\nExample complete - no files to clean up!");

    Ok(())
}
