//! Basic usage example for TapeStore
//!
//! Run with: cargo run --example basic_usage

use tape_store::{columns::*, ops::*, types::*, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let store = TapeStore::open_primary(temp_dir.path())?;

    // Store tracks with the new TrackInfo structure
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo::new(tape_address, EpochNumber(0), [0; 64]);
        store.put_track_info(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve a track
    let track1 = store.get_track_info(Pubkey::new([1; 32]))?;
    println!("Track 1 tape address: {:?}", track1.map(|t| t.tape_address));

    // Store primary and recovery slices with the new structure
    let track_address = Pubkey::new([1; 32]);
    for spool_idx in 0..5u16 {
        let primary = PrimarySliceData::new(vec![spool_idx as u8; 1024], 0);
        let recovery = RecoverySliceData::new(vec![spool_idx as u8 + 100; 1024], 0);
        store.put_both_slices(spool_idx, track_address, primary, recovery)?;
    }
    println!("Stored 5 primary+recovery slice pairs for track 1");

    // Query slices by spool
    let spool_slices: Vec<_> = store
        .iter_primary_slices_by_spool(0)?
        .map(|r| r.unwrap())
        .collect();
    println!("Spool 0 has {} primary slices", spool_slices.len());

    // Store epoch-namespaced spool state
    let epoch = EpochNumber(100);
    for spool_idx in 0..3u16 {
        store.set_spool_status(epoch, spool_idx, SpoolStatus::Active)?;
    }

    // Get assigned spools for this epoch
    let assigned: Vec<_> = store
        .iter_assigned_spools(epoch)?
        .map(|r| r.unwrap())
        .collect();
    println!("Assigned spools in epoch 100: {:?}", assigned);

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

    // Metadata via MetaOps
    store.set_node_status(NodeStatus::Active)?;
    store.set_current_epoch(epoch)?;
    let status = store.get_node_status()?;
    println!("Node status: {:?}", status);

    // Slice info (erasure coding metadata)
    let slice_info = SliceInfo {
        encoding_type: EncodingType::Rotated,
        unencoded_length: 32 * 1024 * 1024,
        primary: vec![Hash::default(); 1024],
        recovery: vec![Hash::default(); 1024],
    };
    store.put_slice_info(track_address, slice_info)?;
    println!("Stored slice info with 1024+1024 hashes");

    // Verify storage
    println!("\nFinal state:");
    println!("  Node status: {:?}", store.get_node_status()?);
    println!("  Current epoch: {:?}", store.get_current_epoch()?);
    println!("  Has slice info for track 1: {}", store.get_slice_info(track_address)?.is_some());
    println!("  Committee for epoch 100: {}", store.get_committee(EpochNumber(100))?.is_some());

    Ok(())
}
