//! Basic usage example for TapeStore
//!
//! Run with: cargo run --example basic_usage

use tape_store::{ops::*, types::*, TapeStore};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = tempfile::tempdir()?;
    let store = TapeStore::open_primary(temp_dir.path())?;

    // Store tracks
    let tape_address = Pubkey::new([0xAA; 32]);
    for i in 1..=5 {
        let track_address = Pubkey::new([i as u8; 32]);
        let info = TrackInfo {
            tape_address,
            spool_group: SpoolGroup(3),
            original_size: 1024,
            encoding_type: 1,
            encoding_params: 0,
            stripe_size: 0,
            stripe_count: 0,
            commitment: vec![],
        };
        store.put_track(track_address, info)?;
        println!("Created track {}", i);
    }

    // Retrieve a track
    let track1 = store.get_track(Pubkey::new([1; 32]))?;
    println!(
        "Track 1 tape address: {:?}",
        track1.map(|t| t.tape_address)
    );

    // Store slices
    let track_address = Pubkey::new([1; 32]);
    for spool_id in 0..5u16 {
        store.put_slice(spool_id, track_address, vec![spool_id as u8; 1024])?;
    }
    println!("Stored 5 slices for track 1");

    // Query slices by spool
    let spool_slices = store.iter_slices_by_spool(0)?;
    println!("Spool 0 has {} slices", spool_slices.len());

    // Spool status (NOT epoch-namespaced)
    for spool_id in 0..3u16 {
        store.set_spool_state(spool_id, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })?;
    }

    // Iterate all spools
    let spools = store.iter_all_spools()?;
    println!("Active spools: {:?}", spools);

    // Store committee
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::types::network::NetworkAddress;

    let member1 = NodeInfo {
        node_address: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        tls_pubkey: Pubkey::new_unique(),
        network_address: NetworkAddress::new_ipv4([192, 168, 1, 1], 8080),
        spools: vec![0, 2],
    };

    let member2 = NodeInfo {
        node_address: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        tls_pubkey: Pubkey::new_unique(),
        network_address: NetworkAddress::new_ipv4([192, 168, 1, 2], 8080),
        spools: vec![1, 3],
    };

    store.put_committee(EpochNumber(100), vec![member1, member2])?;
    println!("Stored committee for epoch 100");

    // Tape info
    let tape_info = TapeInfo {
        end_epoch: EpochNumber(200),
    };
    store.put_tape(tape_address, tape_info)?;
    println!("Stored tape info");

    // Object info
    let obj_address = Pubkey::new_unique();
    store.put_object_info(
        obj_address,
        ObjectInfo::Valid {
            is_stored: true,
            track_address,
            registered_epoch: EpochNumber(5),
            certified_epoch: Some(EpochNumber(6)),
            slot: SlotNumber(50),
        },
    )?;
    println!("Stored object info");

    // Verify storage
    println!("\nFinal state:");
    println!(
        "  Has tape info: {}",
        store.get_tape(tape_address)?.is_some()
    );
    println!(
        "  Committee for epoch 100: {}",
        store.get_committee(EpochNumber(100))?.is_some()
    );

    Ok(())
}
