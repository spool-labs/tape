//! Block processor event handlers
//!
//! Handlers for tapedrive instructions detected in Solana blocks.
//!
//! NOTE: Most handlers are currently no-ops pending redesign of the
//! storage layer. They log the event but don't persist data.

use store::Store;
use tape_core::types::EpochNumber;
use tape_store::error::Result;
use tape_store::TapeStore;
use tracing::debug;

/// Handle RegisterTrack instruction.
pub fn handle_register_track<S: Store>(
    _store: &TapeStore<S>,
    track: [u8; 32],
    tape: [u8; 32],
    registered_epoch: EpochNumber,
) -> Result<()> {
    debug!(
        track = %bs58::encode(&track).into_string(),
        tape = %bs58::encode(&tape).into_string(),
        epoch = registered_epoch.as_u64(),
        "RegisterTrack (no-op)"
    );
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
    use tape_store::TapeStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_register_track_noop() {
        let store = test_store();
        let track = [1u8; 32];
        let tape = [2u8; 32];
        handle_register_track(&store, track, tape, EpochNumber(100)).unwrap();
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
