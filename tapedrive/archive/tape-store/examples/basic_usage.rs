//! Basic usage example for TapeStore
//!
//! This example demonstrates:
//! - Opening a TapeStore database
//! - Storing and retrieving tapes and tracks
//! - Using different column families
//! - Basic iteration patterns
//!
//! Run with: cargo run --example basic_usage

use tape_store::{
    columns::*,
    types::*,
    TapeStore,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the example database
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path();

    println!("Opening TapeStore at: {:?}\n", db_path);

    // Open a TapeStore with optimized configuration
    let store = TapeStore::open_primary(db_path)?;

    println!("=== Creating and storing Tapes ===\n");

    // Create tapes
    for i in 1..=3 {
        let tape = Tape {
            id: TapeNumber(i),
            authority: Pubkey([i as u8; 32]),
            capacity: 10_000_000 * i, // 10 MB * i
            used: 0,
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            track_count: 0,
        };

        // Store the tape
        store.put::<TapesById>(&TapeKey(tape.id), &tape)?;
        store.put::<TapesByAddress>(&tape.authority, &tape.id)?;
        store.put::<TapesActiveIndex>(&TapeKey(tape.id), &())?;

        println!("Created tape {}: capacity={} MB", i, tape.capacity / 1_000_000);
    }

    println!("\n=== Retrieving Tapes ===\n");

    // Retrieve by ID
    let tape1 = store.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    println!("Retrieved tape 1: {:?}", tape1.map(|t| format!("capacity={}", t.capacity)));

    // Check if tape is in active index
    let is_active = store.get::<TapesActiveIndex>(&TapeKey(TapeNumber(1)))?;
    println!("Tape 1 is active: {}", is_active.is_some());

    println!("\n=== Creating and storing Tracks ===\n");

    // Create tracks
    for i in 1..=5 {
        let track = Track {
            id: TrackNumber(i),
            tape: Pubkey([1; 32]), // All on tape 1
            key: Hash([i as u8; 32]),
            size: 1024 * 1024 * i, // 1 MB * i
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::ZERO,
        };

        store.put::<TracksById>(&TrackKey(track.id), &track)?;
        println!("Created track {}: size={} MB", i, track.size / (1024 * 1024));
    }

    println!("\n=== Storing Slice Metadata ===\n");

    // Store metadata for some slices of track 1
    let track_id = TrackNumber(1);
    for spool_idx in 0..5 {
        let slice_key = SliceKey::new(track_id, spool_idx);
        let slice_meta = SliceMeta {
            len: 32 * 1024, // 32 KB slice
            leaf_hash: Hash::ZERO,
            content_digest: Hash::ZERO,
            compression: Compression::Lz4,
            last_verified_at: 1000000,
            flags: 0,
        };

        store.put::<SlicesMeta>(&slice_key, &slice_meta)?;

        // Store slice data
        let slice_data = vec![spool_idx as u8; 1024]; // 1 KB for demo
        store.put::<SlicesData>(&slice_key, &slice_data)?;
    }
    println!("Stored 5 slices for track 1 (metadata + data)");

    println!("\n=== Iterating over Tracks ===\n");

    // Iterate all tracks
    let all_tracks = store.iter::<TracksById>()?;
    println!("Total tracks in database: {}", all_tracks.len());

    for (_key, track) in &all_tracks {
        println!("  Track {}: {} MB", track.id.0, track.size / (1024 * 1024));
    }

    println!("\n=== Committee and Metadata ===\n");

    // Store committee for an epoch
    let committee = Committee {
        epoch: EpochNumber(100),
        members: vec![
            CommitteeMember {
                id: NodeId(1),
                stake: 1000,
                weight: 100,
            },
            CommitteeMember {
                id: NodeId(2),
                stake: 2000,
                weight: 200,
            },
        ],
        total_stake: 3000,
    };

    store.put::<CommitteeByEpoch>(&committee.epoch, &committee)?;
    println!("Stored committee for epoch {}: {} members",
             committee.epoch.0, committee.members.len());

    // Store metadata
    store.put::<Meta>(&"schema_version".to_string(), &vec![1, 0, 0])?;
    let version = store.get::<Meta>(&"schema_version".to_string())?;
    println!("Schema version: {:?}", version);

    println!("\n=== Summary ===");
    println!("✓ Stored 3 tapes with indices");
    println!("✓ Stored 5 tracks");
    println!("✓ Stored 5 slices with metadata and data");
    println!("✓ Stored committee data");
    println!("✓ All operations successful!");

    Ok(())
}
