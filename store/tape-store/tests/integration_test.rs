//! Integration tests for TapeStore with RocksDB backend

use tape_store::{ops::*, types::*, TapeStore};
use tempfile::TempDir;

#[test]
fn open_primary() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Open with optimized config
    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test basic operations
    let track_address = Pubkey::new_unique();
    let tape_address = Pubkey::new_unique();
    let info = TrackInfo::new(tape_address, EpochNumber(0), [0; 64]);

    store.put_track_info(track_address, info.clone()).unwrap();
    let retrieved = store.get_track_info(track_address).unwrap();
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
        let info = TrackInfo::new(tape_address, EpochNumber(50), [0xAB; 64]);
        store.put_track_info(track_address, info).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let retrieved = store.get_track_info(track_address).unwrap();
        assert!(retrieved.is_some());
        let info = retrieved.unwrap();
        assert_eq!(info.registered_epoch, EpochNumber(50));
        assert_eq!(info.tape_address, tape_address);
    }
}

#[test]
fn all_column_families() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test each column family to ensure they're all properly configured

    // Meta - test via MetaOps
    store.set_node_status(NodeStatus::Active).unwrap();
    store.set_current_epoch(EpochNumber(100)).unwrap();

    // Tracks
    let track_address = Pubkey::new_unique();
    let tape_address = Pubkey::new_unique();
    let info = TrackInfo::new(tape_address, EpochNumber(0), [0; 64]);
    store.put_track_info(track_address, info).unwrap();

    // Slice info
    let slice_info = SliceInfo {
        encoding_type: EncodingType::Rotated,
        unencoded_length: 1024 * 1024,
        primary: vec![Hash::default(); 10],
        recovery: vec![Hash::default(); 10],
    };
    store.put_slice_info(track_address, slice_info).unwrap();

    // Tape info
    let tape_info = TapeInfo {
        active_epoch: EpochNumber(50),
        expiry_epoch: EpochNumber(150),
        authority: Pubkey::new_unique(),
    };
    store.put_tape_info(tape_address, tape_info).unwrap();

    // Spool status (epoch-namespaced)
    let epoch = EpochNumber(100);
    store.set_spool_status(epoch, 42, SpoolStatus::Active).unwrap();

    // Sync progress
    let progress = SyncProgress {
        last_synced_track: Some(track_address),
        slice_type: SliceType::Primary,
    };
    store.set_sync_progress(epoch, 42, progress).unwrap();

    // Pending recovery
    store.add_pending_recovery(epoch, 42, SliceType::Primary, track_address).unwrap();

    // Primary slices
    let primary_data = PrimarySliceData::new(vec![0u8; 1024], 0);
    store.put_primary_slice(42, track_address, primary_data).unwrap();

    // Recovery slices
    let recovery_data = RecoverySliceData::new(vec![0u8; 1024], 0);
    store.put_recovery_slice(42, track_address, recovery_data).unwrap();

    // Committee
    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;

    let member = CommitteeMemberInfo {
        id: NodeId(1),
        pubkey: Pubkey::new_unique(),
        bls_pubkey: BlsPubkey::zeroed(),
        network_address: "192.168.1.1:8080".to_string(),
    };

    let committee = CommitteeCache {
        epoch: EpochNumber(1),
        members: vec![member],
        spool_assignment: vec![0],
        my_member_index: Some(0),
        my_spools: vec![0],
    };
    store.put_committee(committee).unwrap();

    // Verify we can read everything back
    assert_eq!(store.get_node_status().unwrap(), Some(NodeStatus::Active));
    assert_eq!(store.get_current_epoch().unwrap(), Some(EpochNumber(100)));
    assert!(store.get_track_info(track_address).unwrap().is_some());
    assert!(store.get_slice_info(track_address).unwrap().is_some());
    assert!(store.get_tape_info(tape_address).unwrap().is_some());
    assert_eq!(store.get_spool_status(epoch, 42).unwrap(), Some(SpoolStatus::Active));
    assert!(store.get_sync_progress(epoch, 42).unwrap().is_some());
    assert!(store.has_pending_recovery(epoch, 42, SliceType::Primary, track_address).unwrap());
    assert!(store.get_primary_slice(42, track_address).unwrap().is_some());
    assert!(store.get_recovery_slice(42, track_address).unwrap().is_some());
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

    let primary = PrimarySliceData::new(large_data.clone(), 0);
    store.put_primary_slice(0, track_address, primary).unwrap();

    let retrieved = store.get_primary_slice(0, track_address).unwrap().unwrap();
    assert_eq!(retrieved.symbols, large_data);
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
        let data = PrimarySliceData::new(vec![0u8; 100], 0);
        store.put_primary_slice(42, *track, data).unwrap();
    }

    for track in &spool_100_tracks {
        let data = PrimarySliceData::new(vec![0u8; 100], 0);
        store.put_primary_slice(100, *track, data).unwrap();
    }

    // Query spool 42 only
    let spool_42_slices: Vec<_> = store
        .iter_primary_slices_by_spool(42)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(spool_42_slices.len(), 10);

    // Query spool 100 only
    let spool_100_slices: Vec<_> = store
        .iter_primary_slices_by_spool(100)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(spool_100_slices.len(), 5);

    // Verify empty spool returns empty
    let spool_999_slices: Vec<_> = store
        .iter_primary_slices_by_spool(999)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(spool_999_slices.len(), 0);
}

#[test]
fn track_info_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();
    let track = Pubkey::new_unique();
    let tape = Pubkey::new_unique();

    // Create track
    let info = TrackInfo::new(tape, EpochNumber(0), [0; 64]);
    store.put_track_info(track, info).unwrap();

    // Verify not certified initially
    let retrieved = store.get_track_info(track).unwrap().unwrap();
    assert!(retrieved.certified_epoch.is_none());

    // Certify
    store.certify_track(track, EpochNumber(100)).unwrap();

    // Verify certified
    let retrieved = store.get_track_info(track).unwrap().unwrap();
    assert_eq!(retrieved.certified_epoch, Some(EpochNumber(100)));
}

#[test]
fn committee_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    use bytemuck::Zeroable;
    use tape_core::bls::BlsPubkey;

    // Add committees for multiple epochs
    for epoch in [95u64, 100, 98] {
        let cache = CommitteeCache {
            epoch: EpochNumber(epoch),
            members: vec![CommitteeMemberInfo {
                id: NodeId(1),
                pubkey: Pubkey::new_unique(),
                bls_pubkey: BlsPubkey::zeroed(),
                network_address: format!("node-{}.example.com:8080", epoch),
            }],
            spool_assignment: vec![0],
            my_member_index: Some(0),
            my_spools: vec![0],
        };
        store.put_committee(cache).unwrap();
    }

    // Get specific epoch
    let cache = store.get_committee(EpochNumber(98)).unwrap().unwrap();
    assert_eq!(cache.epoch, EpochNumber(98));

    // Get current (highest) epoch via fallback iteration
    let current = store.get_current_committee().unwrap().unwrap();
    assert_eq!(current.epoch, EpochNumber(100));
}

#[test]
fn epoch_namespaced_spool_ops() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let epoch_100 = EpochNumber(100);
    let epoch_101 = EpochNumber(101);

    // Set status in epoch 100
    store.set_spool_status(epoch_100, 42, SpoolStatus::Active).unwrap();
    store.set_spool_status(epoch_100, 43, SpoolStatus::Sync).unwrap();

    // Set status in epoch 101
    store.set_spool_status(epoch_101, 42, SpoolStatus::Recover).unwrap();

    // Verify epoch 100 has its own state
    assert_eq!(store.get_spool_status(epoch_100, 42).unwrap(), Some(SpoolStatus::Active));
    assert_eq!(store.get_spool_status(epoch_100, 43).unwrap(), Some(SpoolStatus::Sync));

    // Verify epoch 101 has its own state
    assert_eq!(store.get_spool_status(epoch_101, 42).unwrap(), Some(SpoolStatus::Recover));
    assert!(store.get_spool_status(epoch_101, 43).unwrap().is_none());

    // Iterate assigned spools for epoch 100
    let assigned: Vec<_> = store
        .iter_assigned_spools(epoch_100)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(assigned.len(), 2);

    // Cleanup epoch 100
    store.cleanup_epoch_state(epoch_100).unwrap();

    // Epoch 100 should be empty now
    assert!(store.get_spool_status(epoch_100, 42).unwrap().is_none());
    assert!(store.get_spool_status(epoch_100, 43).unwrap().is_none());

    // Epoch 101 should still have its state
    assert_eq!(store.get_spool_status(epoch_101, 42).unwrap(), Some(SpoolStatus::Recover));
}

#[test]
fn pending_recovery_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let epoch = EpochNumber(100);
    let spool_id = 42;
    let track1 = Pubkey::new_unique();
    let track2 = Pubkey::new_unique();

    // Add pending recoveries
    store.add_pending_recovery(epoch, spool_id, SliceType::Primary, track1).unwrap();
    store.add_pending_recovery(epoch, spool_id, SliceType::Recovery, track1).unwrap();
    store.add_pending_recovery(epoch, spool_id, SliceType::Primary, track2).unwrap();

    // Check existence
    assert!(store.has_pending_recovery(epoch, spool_id, SliceType::Primary, track1).unwrap());
    assert!(store.has_pending_recovery(epoch, spool_id, SliceType::Recovery, track1).unwrap());
    assert!(store.has_pending_recovery(epoch, spool_id, SliceType::Primary, track2).unwrap());
    assert!(!store.has_pending_recovery(epoch, spool_id, SliceType::Recovery, track2).unwrap());

    // Iterate pending for spool
    let pending: Vec<_> = store
        .iter_pending_recoveries(epoch, spool_id)
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(pending.len(), 3);

    // Remove one
    store.remove_pending_recovery(epoch, spool_id, SliceType::Primary, track1).unwrap();
    assert!(!store.has_pending_recovery(epoch, spool_id, SliceType::Primary, track1).unwrap());

    // Other still exists
    assert!(store.has_pending_recovery(epoch, spool_id, SliceType::Recovery, track1).unwrap());
}

#[test]
fn atomic_slice_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let spool_id = 42;
    let track = Pubkey::new_unique();

    let primary = PrimarySliceData::new(vec![1u8; 100], 10);
    let recovery = RecoverySliceData::new(vec![2u8; 100], 20);

    // Put both atomically
    store.put_both_slices(spool_id, track, primary.clone(), recovery.clone()).unwrap();

    // Both should exist
    assert_eq!(store.get_primary_slice(spool_id, track).unwrap(), Some(primary));
    assert_eq!(store.get_recovery_slice(spool_id, track).unwrap(), Some(recovery));

    // Delete both atomically
    store.delete_both_slices(spool_id, track).unwrap();

    // Both should be gone
    assert!(store.get_primary_slice(spool_id, track).unwrap().is_none());
    assert!(store.get_recovery_slice(spool_id, track).unwrap().is_none());
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
    assert_eq!(store.get_gc_started_epoch().unwrap(), Some(EpochNumber(50)));
    assert_eq!(store.get_gc_completed_epoch().unwrap(), Some(EpochNumber(49)));
}
