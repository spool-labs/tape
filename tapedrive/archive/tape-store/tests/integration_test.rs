//! Integration tests for TapeStore with RocksDB backend

use tape_store::{columns::*, types::*, TapeStore};
use tempfile::TempDir;

#[test]
fn open_primary() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Open with optimized config
    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test basic operations
    let tape = TapeData {
        id: TapeNumber(1),
        authority: StoredPubkey::default(),
        capacity: 1_000_000,
        used: 0,
        active_epoch: EpochNumber(100),
        expiry_epoch: EpochNumber(200),
        track_count: 0,
    };

    store
        .put::<TapesById>(&TapeKey(TapeNumber(1)), &tape)
        .unwrap();
    let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap();
    assert_eq!(retrieved, Some(tape));
}

#[test]
fn open_primary_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    // Write data
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let tape = TapeData {
            id: TapeNumber(42),
            authority: StoredPubkey::default(),
            capacity: 500_000,
            used: 100_000,
            active_epoch: EpochNumber(50),
            expiry_epoch: EpochNumber(150),
            track_count: 10,
        };
        store
            .put::<TapesById>(&TapeKey(TapeNumber(42)), &tape)
            .unwrap();
    }

    // Reopen and verify persistence
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let retrieved = store.get::<TapesById>(&TapeKey(TapeNumber(42))).unwrap();
        assert!(retrieved.is_some());
        let tape = retrieved.unwrap();
        assert_eq!(tape.id, TapeNumber(42));
        assert_eq!(tape.capacity, 500_000);
        assert_eq!(tape.used, 100_000);
    }
}

#[test]
fn all_column_families() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test each column family to ensure they're all properly configured

    // Meta
    store.put::<Meta>(&"test_key".to_string(), &vec![1, 2, 3]).unwrap();

    // Tapes
    let tape = TapeData {
        id: TapeNumber(1),
        authority: StoredPubkey::default(),
        capacity: 1000,
        used: 0,
        active_epoch: EpochNumber(1),
        expiry_epoch: EpochNumber(10),
        track_count: 0,
    };
    store.put::<TapesById>(&TapeKey(TapeNumber(1)), &tape).unwrap();
    store.put::<TapesByAddress>(&StoredPubkey::default(), &TapeNumber(1)).unwrap();
    store.put::<TapesActiveIndex>(&TapeKey(TapeNumber(1)), &()).unwrap();

    // Tracks
    let track = TrackData {
        id: TrackNumber(1),
        tape: StoredPubkey::default(),
        key: Hash::default(),
        size: 1024,
        registered_epoch: EpochNumber(1),
        certified_epoch: EpochNumber(2),
        commitment_hash: Hash::default(),
    };
    store.put::<TracksById>(&TrackKey(TrackNumber(1)), &track).unwrap();
    store.put::<TracksByAddress>(&StoredPubkey::default(), &TrackNumber(1)).unwrap();
    store.put::<TracksByBlobKey>(&Hash::default(), &TrackNumber(1)).unwrap();

    // Slices
    let slice_key = SliceKey::new(TrackNumber(1), 42);
    store.put::<SlicesData>(&slice_key, &vec![0u8; 1024]).unwrap();

    let meta = SliceMeta {
        len: 1024,
        leaf_hash: Hash::default(),
        content_digest: Hash::default(),
        compression: Compression::Lz4,
        last_verified_at: 123456789,
        flags: 0,
    };
    store.put::<SlicesMeta>(&slice_key, &meta).unwrap();

    let state = SliceState {
        current_epoch: EpochNumber(1),
        status: SliceStatus::Verified,
        prev_owner: StoredPubkey::default(),
        current_owner: StoredPubkey::default(),
        next_owner: StoredPubkey::default(),
        repair_from: StoredPubkey::default(),
        repair_last_attempt: 0,
        repair_retries: 0,
        handoff_to: StoredPubkey::default(),
        handoff_last_attempt: 0,
        handoff_retries: 0,
        gc_at: 0,
        last_state_change: 123456789,
    };
    store.put::<SlicesState>(&slice_key, &state).unwrap();

    // Assignment
    store.put::<AssignmentStatusCF>(&SpoolKey(42), &AssignmentStatus::ActiveSync).unwrap();
    let progress = SyncProgress {
        last_synced_track_id: 1000,
        phase: SyncPhase::Ingesting,
    };
    store.put::<AssignmentProgressCF>(&SpoolKey(42), &progress).unwrap();

    // Committee
    let committee = CommitteeData {
        epoch: EpochNumber(1),
        members: vec![
            CommitteeMemberData {
                id: NodeId(1),
                stake: 1000,
                weight: 100,
            },
        ],
        total_stake: 1000,
    };
    store.put::<CommitteeByEpoch>(&EpochNumber(1), &committee).unwrap();

    // Recovery queue
    let recovery_key = RecoveryKey::new(42, TrackNumber(1000));
    store.put::<PendingRecover>(&recovery_key, &()).unwrap();

    // GC index
    let gc_key = GcKey::new(123456789, TrackNumber(1000), 42);
    store.put::<GcIndex>(&gc_key, &()).unwrap();

    // Verify we can read everything back
    assert!(store.get::<Meta>(&"test_key".to_string()).unwrap().is_some());
    assert!(store.get::<TapesById>(&TapeKey(TapeNumber(1))).unwrap().is_some());
    assert!(store.get::<TapesByAddress>(&StoredPubkey::default()).unwrap().is_some());
    assert!(store.get::<TapesActiveIndex>(&TapeKey(TapeNumber(1))).unwrap().is_some());
    assert!(store.get::<TracksById>(&TrackKey(TrackNumber(1))).unwrap().is_some());
    assert!(store.get::<TracksByAddress>(&StoredPubkey::default()).unwrap().is_some());
    assert!(store.get::<TracksByBlobKey>(&Hash::default()).unwrap().is_some());
    assert!(store.get::<SlicesData>(&slice_key).unwrap().is_some());
    assert!(store.get::<SlicesMeta>(&slice_key).unwrap().is_some());
    assert!(store.get::<SlicesState>(&slice_key).unwrap().is_some());
    assert!(store.get::<AssignmentStatusCF>(&SpoolKey(42)).unwrap().is_some());
    assert!(store.get::<AssignmentProgressCF>(&SpoolKey(42)).unwrap().is_some());
    assert!(store.get::<CommitteeByEpoch>(&EpochNumber(1)).unwrap().is_some());
    assert!(store.get::<PendingRecover>(&recovery_key).unwrap().is_some());
    assert!(store.get::<GcIndex>(&gc_key).unwrap().is_some());
}

#[test]
fn large_slice_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test with a 2 MiB slice - large enough to trigger BlobDB (threshold is 1 MiB)
    // but small enough to avoid wincode preallocation limits
    let large_data = vec![0xAB; 2 * 1024 * 1024];
    let slice_key = SliceKey::new(TrackNumber(1), 0);

    store.put::<SlicesData>(&slice_key, &large_data).unwrap();
    let retrieved = store.get::<SlicesData>(&slice_key).unwrap();
    assert_eq!(retrieved, Some(large_data));
}
