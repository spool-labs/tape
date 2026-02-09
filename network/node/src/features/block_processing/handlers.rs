//! Block processor event handlers
//!
//! Handlers for tapedrive instructions detected in Solana blocks.

use store::Store;
use tape_api::event::TrackRegistered;
use tape_core::types::EpochNumber;
use tape_store::error::Result;
use tape_store::ops::TrackOps;
use tape_store::types::{Pubkey, SpoolAllocation, TrackInfo};
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
        spool_allocation: SpoolAllocation::SpoolGroup(spool_group),
        original_size: event.size.as_u64(),
        // Stripe metadata is determined by the slicer during encoding, not stored on-chain.
        // These fields are populated when slices arrive (from SliceMetadata headers).
        stripe_size: 0,
        stripe_count: 0,
        encoding_type: event.profile.encoding,
        encoding_params: event.profile.params,
        commitment: vec![],
        commitment_hash: event.commitment,
    };

    store.put_track(Pubkey(track), track_info)?;
    Ok(())
}

/// Handle CertifyTrack instruction.
pub fn handle_certify_track<S: Store>(
    _store: &TapeStore<S>,
    track: [u8; 32],
    epoch: EpochNumber,
) -> Result<()> {
    debug!(
        track = %bs58::encode(&track).into_string(),
        epoch = epoch.as_u64(),
        "CertifyTrack (no-op)"
    );
    Ok(())
}

/// Handle DeleteTrack instruction.
pub fn handle_delete_track<S: Store>(
    _store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        track = %bs58::encode(&track).into_string(),
        epoch = current_epoch.as_u64(),
        "DeleteTrack (no-op)"
    );
    Ok(())
}

/// Handle InvalidateTrack instruction.
pub fn handle_invalidate_track<S: Store>(
    _store: &TapeStore<S>,
    track: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        track = %bs58::encode(&track).into_string(),
        epoch = current_epoch.as_u64(),
        "InvalidateTrack (no-op)"
    );
    Ok(())
}

/// Handle ReserveTape instruction.
pub fn handle_reserve_tape<S: Store>(
    _store: &TapeStore<S>,
    tape: [u8; 32],
    authority: [u8; 32],
    active_epoch: EpochNumber,
    expiry_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        tape = %bs58::encode(&tape).into_string(),
        authority = %bs58::encode(&authority).into_string(),
        active_epoch = active_epoch.as_u64(),
        expiry_epoch = expiry_epoch.as_u64(),
        "ReserveTape (no-op)"
    );
    Ok(())
}

/// Handle DestroyTape instruction.
pub fn handle_destroy_tape<S: Store>(
    _store: &TapeStore<S>,
    tape: [u8; 32],
    current_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        tape = %bs58::encode(&tape).into_string(),
        epoch = current_epoch.as_u64(),
        "DestroyTape (no-op)"
    );
    Ok(())
}

/// Handle AdvanceEpoch instruction.
pub fn handle_advance_epoch<S: Store>(
    _store: &TapeStore<S>,
    old_epoch: EpochNumber,
    new_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        old_epoch = old_epoch.as_u64(),
        new_epoch = new_epoch.as_u64(),
        "AdvanceEpoch (no-op)"
    );
    Ok(())
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
    use tape_core::types::StorageUnits;
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_register_track_persists() {
        let store = test_store();
        let track = [1u8; 32];
        let tape_pubkey = solana_program::pubkey::Pubkey::new_unique();

        let event = TrackRegistered {
            track: solana_program::pubkey::Pubkey::new_from_array(track),
            tape: tape_pubkey,
            key: tape_crypto::Hash::default(),
            size: StorageUnits(100),
            commitment: tape_crypto::Hash::default(),
            epoch: EpochNumber(42),
            profile: EncodingProfile::clay_default(),
            spool_group: 5u64.to_le_bytes(),
        };

        handle_register_track(&store, track, &event).unwrap();

        let info = store.get_track(Pubkey(track)).unwrap().expect("track should exist");
        assert_eq!(info.tape_address, Pubkey(tape_pubkey.to_bytes()));
        assert_eq!(info.spool_allocation, SpoolAllocation::SpoolGroup(5u64));
        assert_eq!(info.original_size, 100);
    }

    #[test]
    fn test_certify_track_noop() {
        let store = test_store();
        let track = [1u8; 32];
        handle_certify_track(&store, track, EpochNumber(100)).unwrap();
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
}
