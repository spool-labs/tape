//! Basic usage example for TapeStore
//!
//! Run with: cargo run --example basic_usage

use tape_store::{columns::*, ops::*, types::*, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let store = TapeStore::open_primary(temp_dir.path())?;

    // Store tracks with the new minimal TrackInfo
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            commitment_hash: Hash::from([i as u8; 32]),
            certified_epoch: EpochNumber(0),
            slice_count: 0,
        };
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve a track
    let track1 = store.get_track_info(Pubkey::new([1; 32]))?;
    println!("Track 1 commitment hash: {:?}", track1.map(|t| t.commitment_hash));

    // Store slices with the new key structure (spool_idx, track_address)
    let track_address = Pubkey::new([1; 32]);
    for spool_idx in 0..5 {
        let meta = SliceMeta {
            len: 32 * 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            received_at: 1000000,
        };
        store.put_slice(spool_idx, track_address, vec![spool_idx as u8; 1024], meta)?;
    }
    println!("Stored 5 slices for track 1");

    // Query slices by spool
    let spool_slices = store.get_spool_slices(0)?;
    println!("Spool 0 has {} slices", spool_slices.len());

    // Store spool state
    for spool_idx in 0..3 {
        let state = SpoolState {
            status: SpoolStatus::Active,
            assigned_epoch: EpochNumber(100),
            sync_cursor: None,
        };
        store.put_spool_state(spool_idx, state)?;
    }

    // Get my spools
    let my_spools = store.get_my_spools()?;
    println!("My spools: {:?}", my_spools);

    // Store committee cache
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;

    let member1 = CommitteeMemberInfo {
        id: NodeId(1),
        pubkey: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        network_address: "192.168.1.1:8080".to_string(),
    };

    let member2 = CommitteeMemberInfo {
        id: NodeId(2),
        pubkey: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        network_address: "192.168.1.2:8080".to_string(),
    };

    let committee = CommitteeCache {
        epoch: EpochNumber(100),
        members: vec![member1, member2],
        spool_assignment: vec![0, 1, 0, 1],
        my_member_index: Some(0),
        my_spools: vec![0, 2],
    };
    store.put_committee(committee)?;
    println!("Stored committee for epoch 100");

    // Metadata
    store.put::<Meta>(&"schema_version".to_string(), &vec![2, 0, 0])?;
    let version = store.get::<Meta>(&"schema_version".to_string())?;
    println!("Schema version: {:?}", version);

    // Storage stats
    let stats = store.get_storage_stats()?;
    println!("\nStorage stats:");
    println!("  Tracks:      {}", stats.track_count);
    println!("  Slice Meta:  {}", stats.slice_meta_count);
    println!("  Slice Data:  {}", stats.slice_data_count);
    println!("  Spools:      {}", stats.spool_count);

    Ok(())
}
