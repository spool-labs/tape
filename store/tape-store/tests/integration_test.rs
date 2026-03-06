//! Integration tests for TapeStore with RocksDB backend

use tape_store::{ops::*, types::*, TapeStore};
use tempfile::TempDir;

#[test]
fn open_primary() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let track_address = Pubkey::new_unique();
    let info = TrackInfo {
        tape_address: Pubkey::new_unique(),
        spool_group: SpoolGroup(3),
        original_size: 1024,
        stripe_size: 0,
        stripe_count: 0,
        encoding_type: 1,
        encoding_params: 0,
        commitment: vec![],
    };

    store.put_track(track_address, info.clone()).unwrap();
    let retrieved = store.get_track(track_address).unwrap();
    assert_eq!(retrieved, Some(info));
}

#[test]
fn open_primary_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let track_address = Pubkey::new_unique();
    let tape_address = Pubkey::new_unique();

    // Write data
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let info = TrackInfo {
            tape_address,
            spool_group: SpoolGroup(3),
            original_size: 512,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 1,
            encoding_params: 0,
            commitment: vec![],
        };
        store.put_track(track_address, info).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let retrieved = store.get_track(track_address).unwrap();
        assert!(retrieved.is_some());
        let info = retrieved.unwrap();
        assert_eq!(info.tape_address, tape_address);
    }
}

#[test]
fn all_column_families() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Tracks
    let track_address = Pubkey::new_unique();
    let tape_address = Pubkey::new_unique();
    let track_info = TrackInfo {
        tape_address,
        spool_group: SpoolGroup(3),
        original_size: 1024 * 1024,
        stripe_size: 0,
        stripe_count: 0,
        encoding_type: 2, // Clay
        encoding_params: 0,
        commitment: vec![],
    };
    store.put_track(track_address, track_info).unwrap();

    // Tape info
    let tape_info = TapeInfo {
        end_epoch: EpochNumber(150),
    };
    store.put_tape(tape_address, tape_info).unwrap();

    // Object info
    let object_address = Pubkey::new_unique();
    let object_info = ObjectInfo::Valid {
        is_stored: true,
        track_address,
        registered_epoch: EpochNumber(5),
        certified_epoch: Some(EpochNumber(6)),
        slot: SlotNumber(50),
    };
    store
        .put_object_info(object_address, object_info)
        .unwrap();

    // Spool status (NOT epoch-namespaced)
    store
        .set_spool_state(42, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
        .unwrap();

    // Sync progress
    let progress_track = Pubkey::new_unique();
    store.set_spool_sync_cursor(42, progress_track).unwrap();

    // Pending recovery
    store
        .add_pending_recovery(42, track_address)
        .unwrap();

    // Slices
    store
        .put_slice(42, track_address, vec![0u8; 1024])
        .unwrap();

    // Committee
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::types::network::NetworkAddress;

    let member = NodeInfo {
        node_id: NodeId(1),
        node_address: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        tls_pubkey: Pubkey::new_unique(),
        network_address: NetworkAddress::new_ipv4([192, 168, 1, 1], 8080),
        spools: vec![0, 2],
    };

    store
        .put_committee(EpochNumber(1), vec![member])
        .unwrap();

    // Verify we can read everything back
    assert!(store.get_track(track_address).unwrap().is_some());
    assert!(store.get_tape(tape_address).unwrap().is_some());
    assert!(store.get_object_info(object_address).unwrap().is_some());
    assert_eq!(
        store.get_spool_state(42).unwrap().unwrap().status,
        SpoolStatus::Active
    );
    assert_eq!(
        store.get_spool_sync_cursor(42).unwrap(),
        Some(progress_track)
    );
    assert!(store.has_pending_recovery(42, track_address).unwrap());
    assert!(store.get_slice(42, track_address).unwrap().is_some());
    assert!(store.get_committee(EpochNumber(1)).unwrap().is_some());
}

#[test]
fn large_slice_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test with a 2 MiB slice - large enough to trigger BlobDB (threshold is 256 KiB)
    let large_data = vec![0xAB; 2 * 1024 * 1024];
    let track_address = Pubkey::new_unique();

    store
        .put_slice(0, track_address, large_data.clone())
        .unwrap();

    let retrieved = store.get_slice(0, track_address).unwrap().unwrap();
    assert_eq!(retrieved, large_data);
}

#[test]
fn spool_prefix_iteration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Add slices to multiple spools
    let spool_42_tracks: Vec<Pubkey> = (0..10).map(|_| Pubkey::new_unique()).collect();
    let spool_100_tracks: Vec<Pubkey> = (0..5).map(|_| Pubkey::new_unique()).collect();

    for track in &spool_42_tracks {
        store.put_slice(42, *track, vec![0u8; 100]).unwrap();
    }

    for track in &spool_100_tracks {
        store.put_slice(100, *track, vec![0u8; 100]).unwrap();
    }

    // Query spool 42 only
    let spool_42_slices = store.iter_slices_by_spool(42).unwrap();
    assert_eq!(spool_42_slices.len(), 10);

    // Query spool 100 only
    let spool_100_slices = store.iter_slices_by_spool(100).unwrap();
    assert_eq!(spool_100_slices.len(), 5);

    // Verify empty spool returns empty
    let spool_999_slices = store.iter_slices_by_spool(999).unwrap();
    assert_eq!(spool_999_slices.len(), 0);
}

#[test]
fn track_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();
    let track = Pubkey::new_unique();

    let info = TrackInfo {
        tape_address: Pubkey::new_unique(),
        spool_group: SpoolGroup(3),
        original_size: 1024,
        stripe_size: 0,
        stripe_count: 0,
        encoding_type: 1,
        encoding_params: 0,
        commitment: vec![],
    };
    store.put_track(track, info.clone()).unwrap();

    let retrieved = store.get_track(track).unwrap().unwrap();
    assert_eq!(retrieved, info);

    store.delete_track(track).unwrap();
    assert!(store.get_track(track).unwrap().is_none());
}

#[test]
fn committee_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;
    use tape_core::types::network::NetworkAddress;

    // Add committees for multiple epochs
    for epoch in [95u64, 100, 98] {
        let members = vec![NodeInfo {
            node_id: NodeId(epoch),
            node_address: Pubkey::new_unique(),
            bls_pubkey: BlsPubkey::zeroed(),
            tls_pubkey: Pubkey::new_unique(),
            network_address: NetworkAddress::new_ipv4([192, 168, 1, epoch as u8], 8080),
            spools: vec![0],
        }];
        store
            .put_committee(EpochNumber(epoch), members)
            .unwrap();
    }

    // Get specific epoch
    let members = store.get_committee(EpochNumber(98)).unwrap().unwrap();
    assert_eq!(members.len(), 1);

    // Delete committee
    store.delete_committee(EpochNumber(95)).unwrap();
    assert!(store.get_committee(EpochNumber(95)).unwrap().is_none());
}

#[test]
fn spool_ops() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Set status (NOT epoch-namespaced)
    store
        .set_spool_state(42, SpoolState { status: SpoolStatus::Active, epoch: EpochNumber(0) })
        .unwrap();
    store
        .set_spool_state(43, SpoolState { status: SpoolStatus::ActiveSync, epoch: EpochNumber(0) })
        .unwrap();

    assert_eq!(
        store.get_spool_state(42).unwrap().unwrap().status,
        SpoolStatus::Active
    );
    assert_eq!(
        store.get_spool_state(43).unwrap().unwrap().status,
        SpoolStatus::ActiveSync
    );

    // Iterate all spools
    let spools = store.iter_all_spools().unwrap();
    assert_eq!(spools.len(), 2);

    // Remove
    store.remove_spool_state(42).unwrap();
    assert!(store.get_spool_state(42).unwrap().is_none());
}

#[test]
fn pending_recovery_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let spool_id = 42;
    let track1 = Pubkey::new_unique();
    let track2 = Pubkey::new_unique();
    let track3 = Pubkey::new_unique();

    // Add pending recoveries
    store.add_pending_recovery(spool_id, track1).unwrap();
    store.add_pending_recovery(spool_id, track2).unwrap();
    store.add_pending_recovery(spool_id, track3).unwrap();

    // Check existence
    assert!(store.has_pending_recovery(spool_id, track1).unwrap());
    assert!(store.has_pending_recovery(spool_id, track2).unwrap());
    assert!(store.has_pending_recovery(spool_id, track3).unwrap());

    // Iterate pending for spool
    let pending = store.iter_pending_recoveries(spool_id, 100).unwrap();
    assert_eq!(pending.len(), 3);

    // Remove one
    store.remove_pending_recovery(spool_id, track1).unwrap();
    assert!(!store.has_pending_recovery(spool_id, track1).unwrap());

    // Others still exist
    assert!(store.has_pending_recovery(spool_id, track2).unwrap());
    assert!(store.has_pending_recovery(spool_id, track3).unwrap());
}

#[test]
fn slice_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let spool_id = 42;
    let track = Pubkey::new_unique();

    let data = vec![0xAB; 100];

    // Put and get
    store
        .put_slice(spool_id, track, data.clone())
        .unwrap();
    assert_eq!(
        store.get_slice(spool_id, track).unwrap(),
        Some(data)
    );

    // Delete
    store.delete_slice(spool_id, track).unwrap();
    assert!(store.get_slice(spool_id, track).unwrap().is_none());
}

#[test]
fn gc_tracking() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Initially empty
    assert!(store.get_gc_started_epoch().unwrap().is_none());
    assert!(store.get_gc_completed_epoch().unwrap().is_none());

    // Set GC progress
    store.set_gc_started_epoch(EpochNumber(50)).unwrap();
    store.set_gc_completed_epoch(EpochNumber(49)).unwrap();

    // Verify
    assert_eq!(
        store.get_gc_started_epoch().unwrap(),
        Some(EpochNumber(50))
    );
    assert_eq!(
        store.get_gc_completed_epoch().unwrap(),
        Some(EpochNumber(49))
    );
}

#[test]
fn object_info_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let addr = Pubkey::new_unique();

    // Blacklisted
    store
        .put_object_info(addr, ObjectInfo::Blacklisted)
        .unwrap();
    assert_eq!(
        store.get_object_info(addr).unwrap(),
        Some(ObjectInfo::Blacklisted)
    );

    // Overwrite with Valid
    let info = ObjectInfo::Valid {
        is_stored: true,
        track_address: Pubkey::new_unique(),
        registered_epoch: EpochNumber(5),
        certified_epoch: Some(EpochNumber(6)),
        slot: SlotNumber(50),
    };
    store.put_object_info(addr, info.clone()).unwrap();
    assert_eq!(store.get_object_info(addr).unwrap(), Some(info));

    // Has and delete
    assert!(store.has_object_info(addr).unwrap());
    store.delete_object_info(addr).unwrap();
    assert!(!store.has_object_info(addr).unwrap());
}

#[test]
fn tape_info_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let tape = Pubkey::new_unique();
    let info = TapeInfo {
        end_epoch: EpochNumber(200),
    };

    store.put_tape(tape, info.clone()).unwrap();
    assert_eq!(store.get_tape(tape).unwrap(), Some(info));

    store.delete_tape(tape).unwrap();
    assert!(store.get_tape(tape).unwrap().is_none());
}
