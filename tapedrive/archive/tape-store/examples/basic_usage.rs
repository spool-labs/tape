//! Basic usage example for TapeStore
//!
//! Run with: cargo run --example basic_usage

use tape_store::{columns::*, types::*, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let store = TapeStore::open_primary(temp_dir.path())?;

    // Store tapes with indices
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
        store.put::<TapesById>(&TapeKey(tape.id), &tape)?;
        store.put::<TapesByAddress>(&tape.authority, &tape.id)?;
        store.put::<TapesActiveIndex>(&TapeKey(tape.id), &())?;
    }

    // Retrieve
    let tape1 = store.get::<TapesById>(&TapeKey(TapeNumber(1)))?;
    println!("Tape 1: {:?}", tape1.map(|t| t.capacity));

    let is_active = store.get::<TapesActiveIndex>(&TapeKey(TapeNumber(1)))?;
    println!("Tape 1 active: {}", is_active.is_some());

    // Store tracks
    for i in 1..=5 {
        let track = TrackData {
            id: TrackNumber(i),
            tape: Pubkey::new([1; 32]),
            key: Hash::from([i as u8; 32]),
            size: 1024 * 1024 * i,
            registered_epoch: EpochNumber(100),
            certified_epoch: EpochNumber(101),
            commitment_hash: Hash::default(),
        };
        store.put::<TracksById>(&TrackKey(track.id), &track)?;
    }

    // Store slices
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

    // Iterate tracks
    let all_tracks = store.iter::<TracksById>()?;
    println!("Tracks: {}", all_tracks.len());

    // Committee data
    let committee = CommitteeData {
        epoch: EpochNumber(100),
        members: vec![
            CommitteeMemberData { id: NodeId(1), stake: 1000, weight: 100 },
            CommitteeMemberData { id: NodeId(2), stake: 2000, weight: 200 },
        ],
        total_stake: 3000,
    };
    store.put::<CommitteeByEpoch>(&committee.epoch, &committee)?;

    // Metadata
    store.put::<Meta>(&"schema_version".to_string(), &vec![1, 0, 0])?;
    let version = store.get::<Meta>(&"schema_version".to_string())?;
    println!("Schema version: {:?}", version);

    Ok(())
}
