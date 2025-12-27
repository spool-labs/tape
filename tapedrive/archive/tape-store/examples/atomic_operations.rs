//! Atomic operations example using operation traits
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
    let temp_dir = tempfile::tempdir().unwrap();
    let store = TapeStore::open_primary(temp_dir.path())?;

    // TapeOps - atomic multi-index updates
    let tape = TapeData {
        id: TapeNumber(1),
        authority: Pubkey::new([42; 32]),
        capacity: 100_000_000,
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };
    store.put_tape(&tape)?;

    // Verify indices
    assert!(store.get::<TapesById>(&TapeKey(tape.id))?.is_some());
    assert!(store.get::<TapesByAddress>(&tape.authority)?.is_some());
    assert!(store.get::<TapesActiveIndex>(&TapeKey(tape.id))?.is_some());

    // Reverse lookup
    let found = store.get_tape_by_address(&Pubkey::new([42; 32]))?;
    println!("Found tape by address: {:?}", found.map(|t| t.id.0));

    // TrackOps
    for i in 1..=5 {
        let track = TrackData {
            id: TrackNumber(i),
            tape: Pubkey::new([42; 32]),
            key: Hash::from([100 + i as u8; 32]),
            size: 1_000_000 * i,
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::default(),
        };
        store.put_track(&track)?;
    }

    // SliceOps - range queries
    let track_id = TrackNumber(1);
    for spool_idx in 0..10 {
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

    let slices = store.get_track_slices(track_id)?;
    println!("Track slices: {}", slices.len());

    let range = store.get_track_slices_range(track_id, 3, 7)?;
    println!("Slices in range [3,7): {}", range.len());

    let count = store.count_track_slices(track_id)?;
    println!("Slice count: {}", count);

    let complete = store.track_is_complete(track_id)?;
    println!("Track complete: {}", complete);

    // StatsOps
    let stats = store.get_storage_stats()?;
    println!("Stats: {} tapes, {} tracks, {} slices",
        stats.tape_count, stats.track_count, stats.slice_meta_count);

    Ok(())
}
