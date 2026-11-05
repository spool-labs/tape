#![cfg(feature = "rocks")]

//! Integration tests for TapeStore with RocksDB backend

use tape_core::bls::BlsSignature;
use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::spooler::{GroupFold, GroupIndex};
use tape_core::system::{VoteCandidate, VoteKind};
use tape_core::track::blob::BlobEncoding;
use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
use tape_core::types::{
    ChunkNumber, ContentType, EpochNumber, SlotNumber, VirtualGroup, SpoolIndex, StorageUnits,
    StripeCount, TapeNumber, TrackNumber,
};
use tape_crypto::address::Address;
use tape_crypto::bls12254::min_sig::G1CompressedPoint;
use tape_crypto::hash::{hash, Hash};
use tape_recover::state::SpoolState;
use tape_recover::track::MissingTrack;
use tape_recover::time::WallTime;
use tape_store::{ops::*, types::*, TapeStore};
use tempfile::TempDir;

fn sample_track(tape: Address) -> CompressedTrack {
    CompressedTrack {
        tape,
        tape_number: TapeNumber(1),
        key: Hash::new_unique(),
        track_number: TrackNumber(0),
        kind: TrackKind::Coded as u64,
        state: TrackState::Certified as u64,
        flags: TrackFlags::NONE,
        size: StorageUnits::from_bytes(1024),
        group: VirtualGroup(3),
        value_hash: Hash::new_unique(),
    }
}

#[test]
fn open_primary() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let track_address = Address::new_unique();
    let info = sample_track(Address::new_unique());

    store.put_track(track_address, info).unwrap();
    let retrieved = store.get_track(track_address).unwrap();
    assert_eq!(retrieved, Some(info));
}

#[test]
fn open_primary_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let track_address = Address::new_unique();
    let tape_address = Address::new_unique();

    // Write data
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let info = sample_track(tape_address);
        store.put_track(track_address, info).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = TapeStore::open_primary(&db_path).unwrap();
        let retrieved = store.get_track(track_address).unwrap();
        assert!(retrieved.is_some());
        let info = retrieved.unwrap();
        assert_eq!(info.tape, tape_address);
    }
}

#[test]
fn all_column_families() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Tracks
    let track_address = Address::new_unique();
    let tape_address = Address::new_unique();
    let mut track_info = sample_track(tape_address);
    track_info.size = StorageUnits::from_bytes(1024 * 1024);
    store.put_track(track_address, track_info).unwrap();

    // Tape info
    let tape_info = TapeInfo {
        id: TapeNumber(1),
        flags: 0,
        end_epoch: EpochNumber(150),
        next_track_number: TrackNumber(0),
        tree_start: TrackNumber(0),
        authority: Address::default(),
        delegate: Address::default(),
        nonce: Hash::default(),
    };
    store.put_tape(tape_address, tape_info).unwrap();

    // Object info
    let object_address = Address::new_unique();
    let object_info = ObjectInfo::Valid {
        track_address,
        registered_epoch: EpochNumber(5),
        certified_epoch: Some(EpochNumber(6)),
        slot: SlotNumber(50),
    };
    store
        .put_object_info(object_address, object_info)
        .unwrap();

    // Spool row and missing_list (NOT epoch-namespaced)
    let spool = SpoolIndex(42);
    store.put_spool_states(&[(spool, SpoolState::new_scanning())]).unwrap();
    let missing = MissingTrack::new_handover(spool, track_address, WallTime(1));
    store.put_missing_track(&missing).unwrap();

    // Slices
    store
        .put_slice(spool, track_address, vec![0u8; 1024])
        .unwrap();

    // Verify we can read everything back
    assert!(store.get_track(track_address).unwrap().is_some());
    assert!(store.get_tape(tape_address).unwrap().is_some());
    assert!(store.get_object_info(object_address).unwrap().is_some());
    assert!(store.get_spool_state(spool).unwrap().unwrap().is_scanning);
    assert!(store.has_missing_track(spool, track_address).unwrap());
    assert!(store.get_slice(spool, track_address).unwrap().is_some());
}

#[test]
fn large_slice_data() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Test with a 2 MiB slice - large enough to trigger BlobDB (threshold is 256 KiB)
    let large_data = vec![0xAB; 2 * 1024 * 1024];
    let track_address = Address::new_unique();

    store
        .put_slice(SpoolIndex(0), track_address, large_data.clone())
        .unwrap();

    let retrieved = store.get_slice(SpoolIndex(0), track_address).unwrap().unwrap();
    assert_eq!(retrieved, large_data);
}

#[test]
fn spool_prefix_iteration() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    // Add slices to multiple spools
    let spool_42_tracks: Vec<Address> = (0..10).map(|_| Address::new_unique()).collect();
    let spool_100_tracks: Vec<Address> = (0..5).map(|_| Address::new_unique()).collect();

    for track in &spool_42_tracks {
        store.put_slice(SpoolIndex(42), *track, vec![0u8; 100]).unwrap();
    }

    for track in &spool_100_tracks {
        store.put_slice(SpoolIndex(100), *track, vec![0u8; 100]).unwrap();
    }

    // Query spool 42 only
    let spool_42_slices = store.iter_slices_by_spool(SpoolIndex(42)).unwrap();
    assert_eq!(spool_42_slices.len(), 10);

    // Query spool 100 only
    let spool_100_slices = store.iter_slices_by_spool(SpoolIndex(100)).unwrap();
    assert_eq!(spool_100_slices.len(), 5);

    // Verify empty spool returns empty
    let spool_999_slices = store.iter_slices_by_spool(SpoolIndex(999)).unwrap();
    assert_eq!(spool_999_slices.len(), 0);
}

#[test]
fn track_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();
    let track = Address::new_unique();

    let info = sample_track(Address::new_unique());
    store.put_track(track, info).unwrap();

    let retrieved = store.get_track(track).unwrap().unwrap();
    assert_eq!(retrieved, info);

    store.delete_track(track).unwrap();
    assert!(store.get_track(track).unwrap().is_none());
}

// spool rows write, list and delete by stored spool
#[test]
fn spool_ops() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let lost = SpoolState { lost_at: EpochNumber(3), ..SpoolState::new_scanning() };
    store
        .put_spool_states(&[(SpoolIndex(42), SpoolState::new_scanning()), (SpoolIndex(43), lost)])
        .unwrap();

    assert!(!store.get_spool_state(SpoolIndex(42)).unwrap().unwrap().is_lost());
    assert!(store.get_spool_state(SpoolIndex(43)).unwrap().unwrap().is_lost());
    assert_eq!(store.get_spool_states(None, STORED_SPOOL_COUNT).unwrap().len(), 2);

    store.delete_spool_state(SpoolIndex(42)).unwrap();
    assert!(store.get_spool_state(SpoolIndex(42)).unwrap().is_none());
}

// a live spool's missing_list reads back in key order on rocks
#[test]
fn missing_list() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let spool = VirtualGroup(2).spool_at(4);
    let mut listed = Vec::new();
    for byte in 1..=3u8 {
        let track = Address::new([byte; 32]);
        let mut missing = MissingTrack::new_upload(spool, track, WallTime(1));
        if byte == 3 {
            missing = MissingTrack::new_handover(spool, track, WallTime(1));
        }
        store.put_missing_track(&missing).unwrap();
        listed.push(missing);
    }

    let live = GroupIndex(0).spool_at(4);
    let fold = GroupFold::new(1).unwrap();
    let page = store.get_missing_tracks(live, fold, None, 10).unwrap();
    assert_eq!(page, listed);
    assert!(store.has_handover_tracks(live, fold).unwrap());

    store.delete_missing_track(&listed[2]).unwrap();
    assert!(!store.has_handover_tracks(live, fold).unwrap());
}

#[test]
fn slice_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let store = TapeStore::open_primary(&db_path).unwrap();

    let spool_id = SpoolIndex(42);
    let track = Address::new_unique();

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

    let addr = Address::new_unique();

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
        track_address: Address::new_unique(),
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

    let tape = Address::new_unique();
    let info = TapeInfo {
        id: TapeNumber(1),
        flags: 0,
        end_epoch: EpochNumber(200),
        next_track_number: TrackNumber(0),
        tree_start: TrackNumber(0),
        authority: Address::default(),
        delegate: Address::default(),
        nonce: Hash::default(),
    };

    store.put_tape(tape, info.clone()).unwrap();
    assert_eq!(store.get_tape(tape).unwrap(), Some(info));

    store.delete_tape(tape).unwrap();
    assert!(store.get_tape(tape).unwrap().is_none());
}

// a live spool's handover entries are still found once flushed to disk
#[test]
fn entries_flushed() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let spool = VirtualGroup(9).spool_at(2);
    let missing = MissingTrack::new_handover(spool, Address::new_unique(), WallTime(1));
    store.put_missing_track(&missing).expect("missing track");
    store.inner().inner().flush().expect("flush");

    let live = GroupIndex(0).spool_at(2);
    let fold = GroupFold::new(1).expect("fold");

    assert!(store.has_handover_tracks(live, fold).expect("handover"));
}

// deleting one voting epoch's signatures leaves the next epoch's in place
#[test]
fn vote_epoch_delete() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let group = GroupIndex(4);
    let signature = BlsSignature(G1CompressedPoint([1; 32]));
    let candidate = |voting_epoch: u64| VoteCandidate {
        kind: VoteKind::Snapshot,
        voting_epoch: EpochNumber(voting_epoch),
        target_epoch: EpochNumber(voting_epoch - 1),
        hash: Hash::from([0xAA; 32]),
    };
    for voting_epoch in [11, 12] {
        store
            .put_vote_sig(candidate(voting_epoch), group, Address::new_unique(), &signature)
            .expect("put vote sig");
    }
    store.inner().inner().flush().expect("flush");

    store.delete_vote_epoch(EpochNumber(11)).expect("delete epoch");

    assert!(store.iter_vote_sigs(candidate(11), group).expect("epoch 11").is_empty());
    assert_eq!(store.iter_vote_sigs(candidate(12), group).expect("epoch 12").len(), 1);
}

// deleting one epoch's snapshot artifacts leaves the next epoch's in place
#[test]
fn snapshot_epoch_delete() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let group = VirtualGroup(2);
    let artifact = SnapshotArtifact {
        blob: BlobEncoding {
            size: StorageUnits::from_bytes(64),
            commitment: Hash::from([7; 32]),
            stripe_size: StorageUnits::from_bytes(64),
            stripe_count: StripeCount(1),
            leaves: [Hash::from([7; 32]); MAX_GROUP_SIZE],
        },
        spool_index: SpoolIndex(7),
        slice: vec![7; 32],
    };
    for epoch in [9, 10] {
        store
            .put_snapshot_artifact(EpochNumber(epoch), group, ChunkNumber(0), &artifact)
            .expect("put artifact");
    }
    store.inner().inner().flush().expect("flush");

    store.delete_snapshot_epoch(EpochNumber(9)).expect("delete epoch");

    assert!(store.iter_snapshot_artifacts(EpochNumber(9), group).expect("epoch 9").is_empty());
    assert_eq!(store.iter_snapshot_artifacts(EpochNumber(10), group).expect("epoch 10").len(), 1);
}

// the pending-write queue a gateway reads at startup is there, and empty
#[test]
fn pending_write_queue() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");

    let entries = store.pending_write_entries().expect("entries");
    let has_data = store
        .has_pending_write_data(Address::new_unique(), b"key")
        .expect("pending data");

    assert!(entries.is_empty());
    assert!(!has_data);
}

// a compaction rate of 0 opens an unpaced store that writes and flushes
#[test]
fn unpaced_store() {
    let temp_dir = TempDir::new().expect("tempdir");
    let meta = temp_dir.path().join("meta");
    let bulk = temp_dir.path().join("bulk");
    let store = TapeStore::open_primary_split(&meta, &bulk, 0, 0).expect("open");
    let spool = SpoolIndex(5);
    let track = Address::new_unique();

    store.put_slice(spool, track, vec![7u8; 300_000]).expect("put slice");
    store.inner().inner().flush().expect("flush");

    assert!(store.has_slice(spool, track).expect("has slice"));
}

// a spool's slice count and stored bytes follow writes and a spool-wide delete
#[test]
fn slice_totals() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let spool = SpoolIndex(5);
    let kept = Address::new_unique();
    let stored = slice::fuse(&[7u8; 300_000], None).len() as u64;
    store.put_slice(spool, kept, vec![7u8; 300_000]).expect("put slice");
    store
        .put_slice(spool, Address::new_unique(), vec![7u8; 300_000])
        .expect("put slice");
    store
        .put_slice(SpoolIndex(6), Address::new_unique(), vec![7u8; 300_000])
        .expect("put slice");
    store.inner().inner().flush().expect("flush");

    let (count, bytes) = store.slice_totals_by_spool(spool).expect("totals");
    assert_eq!(count, 2);
    assert!(bytes.expect("bytes").as_u64() >= 2 * stored);
    assert!(store.has_slice(spool, kept).expect("has slice"));
    assert_eq!(store.iter_slice_keys_by_spool(spool).expect("keys").len(), 2);

    store.delete_all_slices_for_spool(spool).expect("delete spool");

    assert_eq!(store.slice_totals_by_spool(spool).expect("totals").0, 0);
    assert!(!store.has_slice(spool, kept).expect("has slice"));
    assert_eq!(store.slice_totals().expect("totals").0, 1);
}

// a slice written through the awaited put reads back
#[tokio::test]
async fn awaited_slice() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let spool = SpoolIndex(5);
    let track = Address::new_unique();
    let payload = vec![7u8; 64];

    store
        .put_fused_slice_wait(spool, track, slice::fuse(&payload, None))
        .await
        .expect("put slice");

    assert_eq!(store.get_slice(spool, track).expect("get slice"), Some(payload));
}

// a queued S3 write and its bytes read back after the awaited enqueue
#[tokio::test]
async fn pending_write_roundtrip() {
    let temp_dir = TempDir::new().expect("tempdir");
    let store = TapeStore::open_primary(temp_dir.path()).expect("open");
    let tape = Address::new_unique();
    let data = vec![3u8; 1024];
    let write = PendingWrite {
        seq: 1,
        op: PendingOp::Put {
            content_type: ContentType::TextPlain,
            etag: hash(&data),
            size: data.len() as u64,
            block_time: 1_700_000_000,
            old_track: None,
        },
        state: PendingState::Queued,
    };

    store
        .put_pending_write(tape, b"key", &write, Some(data.clone()))
        .await
        .expect("enqueue");

    assert_eq!(store.get_pending_write(tape, b"key").expect("entry"), Some(write));
    assert_eq!(store.get_pending_write_data(tape, b"key").expect("data"), Some(data));
}
