//! Block processor event handlers
//!
//! Handlers for tapedrive instructions detected in Solana blocks.

use store::Store;
use tape_api::event::TrackRegistered;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_store::error::Result;
use tape_store::ops::{ObjectInfoOps, TrackOps};
use tape_store::types::{ObjectInfo, Pubkey, TapeInfo, TrackInfo};
use tape_store::TapeStore;
use tracing::debug;

/// Handle RegisterTrack instruction.
///
/// Persists TrackInfo to local store so the sign endpoint can look up track metadata.
pub fn handle_register_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    event: &TrackRegistered,
) -> Result<()> {
    let spool_group = u64::from_le_bytes(event.spool_group);

    debug!(
        track = %bs58::encode(&track).into_string(),
        tape = %bs58::encode(event.tape.as_ref()).into_string(),
        epoch = event.epoch.as_u64(),
        spool_group = spool_group,
        "RegisterTrack"
    );

    let track_info = TrackInfo {
        tape_address: Pubkey(event.tape.to_bytes()),
        spool_group,
        original_size: event.size.as_u64(),
        stripe_size: u64::from_le_bytes(event.stripe_size),
        stripe_count: u64::from_le_bytes(event.stripe_count),
        encoding_type: event.profile.encoding,
        encoding_params: event.profile.params,
        commitment: event.leaves.to_vec(),
    };

    let track_pubkey = Pubkey(track);
    store.put_track(track_pubkey, track_info)?;

    // Initialize ObjectInfo for this track
    store.put_object_info(
        track_pubkey,
        ObjectInfo::Valid {
            is_stored: false,
            track_address: track_pubkey,
            registered_epoch: event.epoch,
            certified_epoch: None,
            slot: tape_core::types::SlotNumber(0),
        },
    )?;

    Ok(())
}

/// Handle CertifyTrack instruction.
///
/// Updates ObjectInfo with the certification epoch, enabling recovery
/// to filter by certification status.
pub fn handle_certify_track<S: Store>(
    store: &TapeStore<S>,
    track_address: Pubkey,
    epoch: EpochNumber,
) -> Result<()> {
    match store.get_object_info(track_address)? {
        Some(ObjectInfo::Valid {
            is_stored,
            track_address: ta,
            registered_epoch,
            slot,
            ..
        }) => {
            store.put_object_info(
                track_address,
                ObjectInfo::Valid {
                    is_stored,
                    track_address: ta,
                    registered_epoch,
                    certified_epoch: Some(epoch),
                    slot,
                },
            )?;
            debug!(
                track = %track_address,
                epoch = epoch.as_u64(),
                "CertifyTrack persisted"
            );
        }
        _ => {
            // Create ObjectInfo if missing (e.g., node started after registration)
            store.put_object_info(
                track_address,
                ObjectInfo::Valid {
                    is_stored: false,
                    track_address,
                    registered_epoch: epoch,
                    certified_epoch: Some(epoch),
                    slot: tape_core::types::SlotNumber(0),
                },
            )?;
            debug!(
                track = %track_address,
                epoch = epoch.as_u64(),
                "CertifyTrack: created ObjectInfo"
            );
        }
    }
    Ok(())
}

/// Handle DeleteTrack instruction.
///
/// Deletes all data for the track: slices from owned spools, object info, and
/// track metadata. Unlike InvalidateTrack, no Invalid marker is left behind.
pub fn handle_delete_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
    owned_spools: &[SpoolIndex],
) -> Result<()> {
    use tape_store::ops::SliceOps;

    let track_address = Pubkey(track);

    for &spool in owned_spools {
        if let Err(e) = store.delete_slice(spool, track_address) {
            debug!(
                track = %track_address,
                spool,
                error = %e,
                "failed to delete slice during track deletion"
            );
        }
    }

    store.delete_object_info(track_address)?;
    store.delete_track(track_address)?;

    debug!(
        track = %track_address,
        epoch = current_epoch.as_u64(),
        spools_cleaned = owned_spools.len(),
        "DeleteTrack: cleaned up"
    );
    Ok(())
}

/// Handle InvalidateTrack instruction.
///
/// Deletes the track metadata and associated slices from owned spools.
/// This is the GC path for invalidated tracks.
pub fn handle_invalidate_track<S: Store>(
    store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
    owned_spools: &[tape_core::spooler::SpoolIndex],
) -> Result<()> {
    use tape_store::ops::SliceOps;

    let track_address = Pubkey(track);

    // Mark object as invalid before deleting data
    store.put_object_info(
        track_address,
        ObjectInfo::Invalid {
            epoch: current_epoch,
            slot: tape_core::types::SlotNumber(0),
        },
    )?;

    // Delete slices from all owned spools
    for &spool in owned_spools {
        if let Err(e) = store.delete_slice(spool, track_address) {
            debug!(
                track = %track_address,
                spool,
                error = %e,
                "failed to delete slice during invalidation"
            );
        }
    }

    // Delete track metadata
    store.delete_track(track_address)?;

    debug!(
        track = %track_address,
        epoch = current_epoch.as_u64(),
        spools_cleaned = owned_spools.len(),
        "InvalidateTrack: cleaned up"
    );
    Ok(())
}

/// Handle ReserveTape instruction.
///
/// Persists TapeInfo so AdvanceEpoch GC can find expired tapes.
pub fn handle_reserve_tape<S: Store>(
    store: &TapeStore<S>,
    tape: [u8; 32],
    _authority: [u8; 32],
    _active_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Result<()> {
    use tape_store::ops::TapeOps;

    let tape_address = Pubkey(tape);
    store.put_tape(tape_address, TapeInfo { end_epoch: expiry_epoch })?;
    debug!(tape = %tape_address, expiry = expiry_epoch.as_u64(), "ReserveTape: stored");
    Ok(())
}

/// Handle DestroyTape instruction.
///
/// Deletes all tracks belonging to the tape (including slices and object info),
/// then deletes the tape itself.
pub fn handle_destroy_tape<S: Store>(
    store: &TapeStore<S>,
    tape: [u8; 32],
    current_epoch: EpochNumber,
    owned_spools: &[SpoolIndex],
) -> Result<()> {
    use tape_store::ops::TapeOps;

    let tape_address = Pubkey(tape);
    let deleted = destroy_tape_tracks(store, tape_address, owned_spools)?;
    store.delete_tape(tape_address)?;

    debug!(
        tape = %tape_address,
        epoch = current_epoch.as_u64(),
        tracks_deleted = deleted,
        "DestroyTape: cleaned up"
    );
    Ok(())
}

/// Handle AdvanceEpoch instruction.
///
/// GC pass: deletes all tapes whose end_epoch < new_epoch, along with their
/// tracks, slices, and object info.
pub fn handle_advance_epoch<S: Store>(
    store: &TapeStore<S>,
    _old_epoch: EpochNumber,
    new_epoch: EpochNumber,
    owned_spools: &[SpoolIndex],
) -> Result<()> {
    use tape_store::ops::TapeOps;

    let tapes = store.iter_all_tapes()?;
    let mut gc_count = 0usize;

    for (tape_address, tape_info) in &tapes {
        if tape_info.end_epoch >= new_epoch {
            continue;
        }
        destroy_tape_tracks(store, *tape_address, owned_spools)?;
        store.delete_tape(*tape_address)?;
        gc_count += 1;
    }

    if gc_count > 0 {
        debug!(
            new_epoch = new_epoch.as_u64(),
            tapes_gc = gc_count,
            "AdvanceEpoch: GC expired tapes"
        );
    }
    Ok(())
}

/// Delete all tracks belonging to a tape, including their slices and object info.
///
/// Uses paginated iteration to avoid unbounded memory usage.
fn destroy_tape_tracks<S: Store>(
    store: &TapeStore<S>,
    tape_address: Pubkey,
    owned_spools: &[SpoolIndex],
) -> Result<usize> {
    use tape_store::ops::SliceOps;

    let mut cursor: Option<Pubkey> = None;
    let mut deleted = 0usize;

    loop {
        let batch = store.iter_tracks_from(cursor, 1000)?;
        if batch.is_empty() {
            break;
        }
        for (track_address, track_info) in &batch {
            cursor = Some(*track_address);
            if track_info.tape_address != tape_address {
                continue;
            }
            for &spool in owned_spools {
                if let Err(e) = store.delete_slice(spool, *track_address) {
                    debug!(track = %track_address, spool, error = %e,
                           "failed to delete slice during tape destruction");
                }
            }
            store.delete_object_info(*track_address)?;
            store.delete_track(*track_address)?;
            deleted += 1;
        }
    }

    Ok(deleted)
}

// --- Sync cursor operations (functional) ---

pub fn get_sync_cursor<S: Store>(
    store: &TapeStore<S>,
) -> Result<Option<tape_core::types::SlotNumber>> {
    use tape_store::ops::MetaOps;
    Ok(store.get_sync_cursor()?)
}

pub fn set_sync_cursor<S: Store>(
    store: &TapeStore<S>,
    slot: tape_core::types::SlotNumber,
) -> Result<()> {
    use tape_store::ops::MetaOps;
    store.set_sync_cursor(slot)?;
    Ok(())
}

// --- Cluster hash operations (functional) ---

pub fn get_cluster_hash<S: Store>(
    store: &TapeStore<S>,
) -> Result<Option<tape_crypto::Hash>> {
    use tape_store::ops::MetaOps;
    Ok(store.get_cluster_hash()?)
}

pub fn set_cluster_hash<S: Store>(
    store: &TapeStore<S>,
    hash: tape_crypto::Hash,
) -> Result<()> {
    use tape_store::ops::MetaOps;
    store.set_cluster_hash(hash)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::SPOOL_GROUP_SIZE;
    use tape_core::types::StorageUnits;
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn test_event() -> TrackRegistered {
        TrackRegistered {
            track: solana_program::pubkey::Pubkey::new_unique(),
            tape: solana_program::pubkey::Pubkey::new_unique(),
            key: tape_crypto::Hash::default(),
            size: StorageUnits(100),
            commitment: tape_crypto::Hash::default(),
            epoch: EpochNumber(42),
            profile: EncodingProfile::clay_default(),
            spool_group: 5u64.to_le_bytes(),
            stripe_size: 0u64.to_le_bytes(),
            stripe_count: 0u64.to_le_bytes(),
            leaves: [tape_crypto::Hash::default(); SPOOL_GROUP_SIZE],
        }
    }

    #[test]
    fn test_register_track_persists() {
        let store = test_store();
        let track = [1u8; 32];
        let event = test_event();
        let tape_pubkey = event.tape;

        handle_register_track(&store, track, &event).unwrap();

        let info = store.get_track(Pubkey(track)).unwrap().expect("track should exist");
        assert_eq!(info.tape_address, Pubkey(tape_pubkey.to_bytes()));
        assert_eq!(info.spool_group, 5);
        assert_eq!(info.original_size, 100);

        // ObjectInfo should also be created
        let obj = store.get_object_info(Pubkey(track)).unwrap().expect("object info should exist");
        match obj {
            ObjectInfo::Valid { certified_epoch, registered_epoch, .. } => {
                assert_eq!(registered_epoch, EpochNumber(42));
                assert!(certified_epoch.is_none());
            }
            _ => panic!("expected ObjectInfo::Valid"),
        }
    }

    #[test]
    fn test_certify_track_persists() {
        let store = test_store();
        let track = Pubkey([1u8; 32]);
        let event = test_event();

        handle_register_track(&store, track.0, &event).unwrap();

        // Before certification
        match store.get_object_info(track).unwrap().unwrap() {
            ObjectInfo::Valid { certified_epoch, .. } => assert!(certified_epoch.is_none()),
            _ => panic!("expected Valid"),
        }

        handle_certify_track(&store, track, EpochNumber(100)).unwrap();

        // After certification
        match store.get_object_info(track).unwrap().unwrap() {
            ObjectInfo::Valid { certified_epoch, .. } => {
                assert_eq!(certified_epoch, Some(EpochNumber(100)));
            }
            _ => panic!("expected Valid"),
        }
    }

    #[test]
    fn test_certify_track_missing() {
        let store = test_store();
        let track = Pubkey([99u8; 32]);
        handle_certify_track(&store, track, EpochNumber(100)).unwrap();

        // Should create ObjectInfo even without prior registration
        let obj = store.get_object_info(track).unwrap().expect("should create ObjectInfo");
        match obj {
            ObjectInfo::Valid { certified_epoch, .. } => {
                assert_eq!(certified_epoch, Some(EpochNumber(100)));
            }
            _ => panic!("expected Valid"),
        }
    }

    #[test]
    fn test_invalidate_track_sets_object_info_and_cleans_up() {
        use tape_core::erasure::group_start;
        use tape_store::ops::SliceOps;

        let store = test_store();
        let track = [2u8; 32];
        let event = test_event();

        // Register track first (creates ObjectInfo::Valid)
        handle_register_track(&store, track, &event).unwrap();

        // Store a slice so we can verify cleanup
        let spool = group_start(5) + 0;
        store.put_slice(spool, Pubkey(track), vec![1, 2, 3]).unwrap();
        assert!(store.has_slice(spool, Pubkey(track)).unwrap());

        // Invalidate
        let owned_spools = vec![spool];
        handle_invalidate_track(&store, track, EpochNumber(50), &owned_spools).unwrap();

        // ObjectInfo should be Invalid
        let obj = store.get_object_info(Pubkey(track)).unwrap().expect("object info should exist");
        match obj {
            ObjectInfo::Invalid { epoch, .. } => {
                assert_eq!(epoch, EpochNumber(50));
            }
            _ => panic!("expected ObjectInfo::Invalid, got {:?}", obj),
        }

        // Track metadata should be deleted
        assert!(store.get_track(Pubkey(track)).unwrap().is_none());

        // Slice should be deleted
        assert!(!store.has_slice(spool, Pubkey(track)).unwrap());
    }

    #[test]
    fn test_sync_cursor_roundtrip() {
        let store = test_store();
        assert!(get_sync_cursor(&store).unwrap().is_none());

        let slot = tape_core::types::SlotNumber(123456);
        set_sync_cursor(&store, slot).unwrap();
        assert_eq!(get_sync_cursor(&store).unwrap(), Some(slot));
    }

    #[test]
    fn test_cluster_hash_roundtrip() {
        let store = test_store();
        assert!(get_cluster_hash(&store).unwrap().is_none());

        let hash = tape_crypto::Hash::new_unique();
        set_cluster_hash(&store, hash).unwrap();
        assert_eq!(get_cluster_hash(&store).unwrap(), Some(hash));
    }

    #[test]
    fn test_delete_track_cleans_up() {
        use tape_core::erasure::group_start;
        use tape_store::ops::SliceOps;

        let store = test_store();
        let track = [3u8; 32];
        let event = test_event();

        handle_register_track(&store, track, &event).unwrap();

        let spool = group_start(5);
        store.put_slice(spool, Pubkey(track), vec![1, 2, 3]).unwrap();
        assert!(store.has_slice(spool, Pubkey(track)).unwrap());

        let owned_spools = vec![spool];
        handle_delete_track(&store, track, EpochNumber(50), &owned_spools).unwrap();

        // Track metadata gone
        assert!(store.get_track(Pubkey(track)).unwrap().is_none());
        // Object info gone (not Invalid marker like invalidate)
        assert!(store.get_object_info(Pubkey(track)).unwrap().is_none());
        // Slice gone
        assert!(!store.has_slice(spool, Pubkey(track)).unwrap());
    }

    #[test]
    fn test_reserve_tape_stores_info() {
        use tape_store::ops::TapeOps;

        let store = test_store();
        let tape = [4u8; 32];
        let authority = [5u8; 32];

        handle_reserve_tape(&store, tape, authority, EpochNumber(10), EpochNumber(200)).unwrap();

        let info = store.get_tape(Pubkey(tape)).unwrap().expect("tape should exist");
        assert_eq!(info.end_epoch, EpochNumber(200));
    }

    #[test]
    fn test_destroy_tape_deletes_all_tracks() {
        use tape_core::erasure::group_start;
        use tape_store::ops::{SliceOps, TapeOps};

        let store = test_store();
        let tape_a = Pubkey([10u8; 32]);
        let tape_b = Pubkey([11u8; 32]);
        let spool = group_start(5);

        // Store tapes
        store.put_tape(tape_a, TapeInfo { end_epoch: EpochNumber(100) }).unwrap();
        store.put_tape(tape_b, TapeInfo { end_epoch: EpochNumber(200) }).unwrap();

        // 3 tracks on tape A
        let tracks_a: Vec<Pubkey> = (0..3).map(|_| Pubkey::new_unique()).collect();
        for &addr in &tracks_a {
            store.put_track(addr, TrackInfo {
                tape_address: tape_a,
                spool_group: 5,
                original_size: 100,
                stripe_size: 0,
                stripe_count: 0,
                encoding_type: 0,
                encoding_params: 0,
                commitment: vec![],
            }).unwrap();
            store.put_object_info(addr, ObjectInfo::Valid {
                is_stored: true,
                track_address: addr,
                registered_epoch: EpochNumber(1),
                certified_epoch: None,
                slot: tape_core::types::SlotNumber(0),
            }).unwrap();
            store.put_slice(spool, addr, vec![1, 2, 3]).unwrap();
        }

        // 1 track on tape B
        let track_b = Pubkey::new_unique();
        store.put_track(track_b, TrackInfo {
            tape_address: tape_b,
            spool_group: 5,
            original_size: 200,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }).unwrap();
        store.put_slice(spool, track_b, vec![4, 5, 6]).unwrap();

        // Destroy tape A
        let owned_spools = vec![spool];
        handle_destroy_tape(&store, tape_a.0, EpochNumber(50), &owned_spools).unwrap();

        // Tape A and its tracks gone
        assert!(store.get_tape(tape_a).unwrap().is_none());
        for &addr in &tracks_a {
            assert!(store.get_track(addr).unwrap().is_none());
            assert!(store.get_object_info(addr).unwrap().is_none());
            assert!(!store.has_slice(spool, addr).unwrap());
        }

        // Tape B and its track remain
        assert!(store.get_tape(tape_b).unwrap().is_some());
        assert!(store.get_track(track_b).unwrap().is_some());
        assert!(store.has_slice(spool, track_b).unwrap());
    }

    #[test]
    fn test_advance_epoch_gc_expired_tapes() {
        use tape_store::ops::TapeOps;

        let store = test_store();

        // Tape expiring at epoch 50
        let tape_expired = Pubkey([20u8; 32]);
        store.put_tape(tape_expired, TapeInfo { end_epoch: EpochNumber(50) }).unwrap();
        let track_exp = Pubkey::new_unique();
        store.put_track(track_exp, TrackInfo {
            tape_address: tape_expired,
            spool_group: 0,
            original_size: 100,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }).unwrap();

        // Tape expiring at epoch 200
        let tape_valid = Pubkey([21u8; 32]);
        store.put_tape(tape_valid, TapeInfo { end_epoch: EpochNumber(200) }).unwrap();
        let track_val = Pubkey::new_unique();
        store.put_track(track_val, TrackInfo {
            tape_address: tape_valid,
            spool_group: 0,
            original_size: 100,
            stripe_size: 0,
            stripe_count: 0,
            encoding_type: 0,
            encoding_params: 0,
            commitment: vec![],
        }).unwrap();

        // Advance to epoch 100
        handle_advance_epoch(&store, EpochNumber(99), EpochNumber(100), &[]).unwrap();

        // Expired tape and its track should be gone
        assert!(store.get_tape(tape_expired).unwrap().is_none());
        assert!(store.get_track(track_exp).unwrap().is_none());

        // Valid tape and its track should remain
        assert!(store.get_tape(tape_valid).unwrap().is_some());
        assert!(store.get_track(track_val).unwrap().is_some());
    }
}
