//! Integration tests for TapeStore with RocksDB backend

use tape_store::{columns::*, ops::*, types::*, TapeStore};
use tempfile::TempDir;

#[test]
fn open_primary() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Open with optimized config
    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test basic operations
    let track_address = Pubkey::new_unique();
    let info = TrackInfo {
        commitment_hash: Hash::new_unique(),
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    };

    store.put_track_info(track_address, info.clone()).unwrap();
    let retrieved = store.get_track_info(track_address).unwrap();
    assert_eq!(retrieved, Some(info));
}

#[test]
fn open_primary_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let track_address = Pubkey::new_unique();

    // Write data
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let info = TrackInfo {
            commitment_hash: Hash::new_unique(),
            certified_epoch: EpochNumber(50),
            slice_count: 10,
        };
        store.put_track_info(track_address, info).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let retrieved = store.get_track_info(track_address).unwrap();
        assert!(retrieved.is_some());
        let info = retrieved.unwrap();
        assert_eq!(info.certified_epoch, EpochNumber(50));
        assert_eq!(info.slice_count, 10);
    }
}

#[test]
fn all_column_families() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test each column family to ensure they're all properly configured

    // Meta
    store
        .put::<Meta>(&"test_key".to_string(), &vec![1, 2, 3])
        .unwrap();

    // Tracks
    let track_address = Pubkey::new_unique();
    let info = TrackInfo {
        commitment_hash: Hash::new_unique(),
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    };
    store.put_track_info(track_address, info).unwrap();

    // Slices
    let slice_key = SliceKey::new(42, track_address);
    store
        .put::<SlicesData>(&slice_key, &vec![0u8; 1024])
        .unwrap();

    let meta = SliceMeta {
        len: 1024,
        leaf_hash: Hash::default(),
        merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        compression: Compression::Lz4,
        received_at: 123456789,
    };
    store.put::<SlicesMeta>(&slice_key, &meta).unwrap();

    // Spools
    let spool_state = SpoolState {
        status: SpoolStatus::Active,
        assigned_epoch: EpochNumber(100),
        sync_cursor: None,
    };
    store.put_spool_state(42, spool_state).unwrap();

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

    // Pending recovery
    let recovery_info = RecoveryInfo {
        source_node: Pubkey::new_unique(),
        attempts: 0,
        last_attempt: 0,
    };
    store
        .put::<PendingRecover>(&slice_key, &recovery_info)
        .unwrap();

    // Pending handoff
    let handoff_info = HandoffInfo {
        target_node: Pubkey::new_unique(),
        attempts: 0,
        last_attempt: 0,
    };
    store
        .put::<PendingHandoff>(&slice_key, &handoff_info)
        .unwrap();

    // GC scheduled
    let gc_key = GcKey::new(123456789, 42, track_address);
    store.put::<GcScheduled>(&gc_key, &()).unwrap();

    // Verify we can read everything back
    assert!(store
        .get::<Meta>(&"test_key".to_string())
        .unwrap()
        .is_some());
    assert!(store.get_track_info(track_address).unwrap().is_some());
    assert!(store.get::<SlicesData>(&slice_key).unwrap().is_some());
    assert!(store.get::<SlicesMeta>(&slice_key).unwrap().is_some());
    assert!(store.get_spool_state(42).unwrap().is_some());
    assert!(store.get_committee(EpochNumber(1)).unwrap().is_some());
    assert!(store.get::<PendingRecover>(&slice_key).unwrap().is_some());
    assert!(store.get::<PendingHandoff>(&slice_key).unwrap().is_some());
    assert!(store.get::<GcScheduled>(&gc_key).unwrap().is_some());
}

#[test]
fn large_slice_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test with a 2 MiB slice - large enough to trigger BlobDB (threshold is 1 MiB)
    // but small enough to avoid wincode preallocation limits
    let large_data = vec![0xAB; 2 * 1024 * 1024];
    let track_address = Pubkey::new_unique();
    let slice_key = SliceKey::new(0, track_address);

    store
        .put::<SlicesData>(&slice_key, &large_data)
        .unwrap();
    let retrieved = store.get::<SlicesData>(&slice_key).unwrap();
    assert_eq!(retrieved, Some(large_data));
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
        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 0,
        };
        store
            .put_slice(42, *track, vec![0u8; 100], meta)
            .unwrap();
    }

    for track in &spool_100_tracks {
        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 0,
        };
        store
            .put_slice(100, *track, vec![0u8; 100], meta)
            .unwrap();
    }

    // Query spool 42 only
    let spool_42_slices = store.get_spool_slices(42).unwrap();
    assert_eq!(spool_42_slices.len(), 10);

    // Query spool 100 only
    let spool_100_slices = store.get_spool_slices(100).unwrap();
    assert_eq!(spool_100_slices.len(), 5);

    // Verify empty spool returns empty
    let spool_999_slices = store.get_spool_slices(999).unwrap();
    assert_eq!(spool_999_slices.len(), 0);
}

#[test]
fn slice_key_ordering() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Insert slices in random spool order
    for spool_idx in [500u16, 1, 100, 50, 999] {
        let track = Pubkey::new_unique();
        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 0,
        };
        store.put_slice(spool_idx, track, vec![0u8; 10], meta).unwrap();
    }

    // Verify they come back in sorted order due to BE encoding
    let mut collected = Vec::new();
    for (key, _meta) in store.iter::<SlicesMeta>().unwrap() {
        collected.push(key.spool_idx);
    }

    assert_eq!(collected, vec![1, 50, 100, 500, 999]);
}

#[test]
fn track_info_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();
    let address = Pubkey::new_unique();

    // Create track
    let info = TrackInfo {
        commitment_hash: Hash::new_unique(),
        certified_epoch: EpochNumber(0),
        slice_count: 0,
    };
    store.put_track_info(address, info).unwrap();

    // Increment slice count
    let count = store.increment_slice_count(address).unwrap();
    assert_eq!(count, 1);

    let count = store.increment_slice_count(address).unwrap();
    assert_eq!(count, 2);

    // Mark certified
    store.mark_certified(address, EpochNumber(100)).unwrap();

    // Verify
    let retrieved = store.get_track_info(address).unwrap().unwrap();
    assert_eq!(retrieved.slice_count, 2);
    assert_eq!(retrieved.certified_epoch, EpochNumber(100));
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

    // Get current (highest) epoch
    let current = store.get_current_committee().unwrap().unwrap();
    assert_eq!(current.epoch, EpochNumber(100));
}

#[test]
fn storage_stats() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Initially empty
    let stats = store.get_storage_stats().unwrap();
    assert_eq!(stats.track_count, 0);
    assert_eq!(stats.slice_meta_count, 0);
    assert_eq!(stats.spool_count, 0);

    // Add some data
    for _ in 0..5 {
        let track = Pubkey::new_unique();
        store
            .put_track_info(
                track,
                TrackInfo {
                    commitment_hash: Hash::default(),
                    certified_epoch: EpochNumber(0),
                    slice_count: 0,
                },
            )
            .unwrap();
    }

    for spool_idx in [10, 20, 30] {
        let track = Pubkey::new_unique();
        let meta = SliceMeta {
            len: 1024,
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
            compression: Compression::Lz4,
            received_at: 0,
        };
        store.put_slice(spool_idx, track, vec![0u8; 100], meta).unwrap();

        store
            .put_spool_state(
                spool_idx,
                SpoolState {
                    status: SpoolStatus::Active,
                    assigned_epoch: EpochNumber(100),
                    sync_cursor: None,
                },
            )
            .unwrap();
    }

    let stats = store.get_storage_stats().unwrap();
    assert_eq!(stats.track_count, 5);
    assert_eq!(stats.slice_meta_count, 3);
    assert_eq!(stats.slice_data_count, 3);
    assert_eq!(stats.spool_count, 3);
}
