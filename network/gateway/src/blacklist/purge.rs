//! Deletes local copies of content a gateway no longer serves

use std::fmt::Display;

use tape_store_api::Store;
use tape_blacklist::blacklists::Changes;
use tape_crypto::address::Address;
use tape_store::TapeStore;
use tape_store::ops::{TapeTrackScan, TrackDataOps, TrackOps};

use crate::cache::{GatewayCacheError, GatewaySliceCache};

/// Tracks read per batch while walking an unlisted tape
const PURGE_SCAN_BATCH: usize = 256;

/// Delete cached slices and stored data for everything the changes newly deny
///
/// Safe to repeat. Targets allowed again need nothing: their content refills
/// from storage nodes on the next read.
pub fn purge_denied<Db: Store>(
    store: &TapeStore<Db>,
    slice_cache: &GatewaySliceCache<Db>,
    changes: &Changes,
) -> Result<(), GatewayCacheError> {
    for target in &changes.denied {
        match (target.is_track(), target.is_tape()) {
            (true, _) => purge_track(store, slice_cache, target.target)?,
            (false, true) => purge_tape(store, slice_cache, target.target)?,
            (false, false) => {}
        }
    }
    Ok(())
}

fn purge_track<Db: Store>(
    store: &TapeStore<Db>,
    slice_cache: &GatewaySliceCache<Db>,
    track_address: Address,
) -> Result<(), GatewayCacheError> {
    if let Some(track) = store.get_track(track_address).map_err(store_error)? {
        slice_cache.purge_track(track_address, track.group)?;
    }
    store.delete_track_data(track_address).map_err(store_error)
}

fn purge_tape<Db: Store>(
    store: &TapeStore<Db>,
    slice_cache: &GatewaySliceCache<Db>,
    tape: Address,
) -> Result<(), GatewayCacheError> {
    let mut scan = TapeTrackScan::new(tape, PURGE_SCAN_BATCH);
    while let Some(scanned) = scan.next_track(store).map_err(store_error)? {
        slice_cache.purge_track(scanned.address, scanned.track.group)?;
        if scanned.data.is_some() {
            store.delete_track_data(scanned.address).map_err(store_error)?;
        }
    }
    Ok(())
}

fn store_error(error: impl Display) -> GatewayCacheError {
    GatewayCacheError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::track_pda;
    use tape_core::spooler::GroupIndex;
    use tape_core::system::BlacklistEntry;
    use tape_core::track::data::BlobData;
    use tape_core::track::types::CompressedTrack;
    use tape_core::types::{StorageUnits, TrackNumber};
    use tape_crypto::Hash;
    use tape_node::config::gateway::GatewayCacheConfig;
    use tape_store::ops::SliceOps;

    use super::*;
    use crate::blacklist::denials::GatewayBlacklists;

    fn put_track(store: &TapeStore<MemoryStore>, tape: Address) -> Address {
        let track = CompressedTrack {
            tape,
            track_number: TrackNumber(0),
            key: Hash::new_unique(),
            kind: 0,
            state: 1,
            visibility: 0,
            size: StorageUnits::from_bytes(3),
            group: GroupIndex(0),
            value_hash: Hash::new_unique(),
        };
        let address = track_pda(tape, track.track_number).0;
        store.put_track(address, track).expect("put track");
        store
            .put_track_data(address, BlobData::Inline(vec![1, 2, 3]))
            .expect("put track data");
        store
            .put_slice(track.group.spool_at(0), address, vec![4, 5, 6])
            .expect("put slice");
        address
    }

    fn is_held(store: &TapeStore<MemoryStore>, address: Address) -> bool {
        let slice = store
            .get_slice(GroupIndex(0).spool_at(0), address)
            .expect("read slice");
        let data = store.get_track_data(address).expect("read data");
        slice.is_some() || data.is_some()
    }

    // a removed track and every track on an unlisted tape lose their local copies; others keep theirs
    #[test]
    fn purged_content() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let unlisted = Address::new_unique();
        let removed = put_track(&store, Address::new_unique());
        let on_unlisted = put_track(&store, unlisted);
        let kept = put_track(&store, Address::new_unique());
        let config = GatewayCacheConfig {
            max_bytes: 1024,
            eviction_batch: 16,
            reclaim_after_deleted_slices: 0,
        };
        let cache = GatewaySliceCache::new(store.clone(), config, Arc::new(GatewayBlacklists::default()))
            .expect("open cache");
        let changes = Changes {
            denied: vec![BlacklistEntry::track(removed), BlacklistEntry::tape(unlisted)],
            allowed: Vec::new(),
        };

        purge_denied(&store, &cache, &changes).expect("purge");

        assert!(!is_held(&store, removed));
        assert!(!is_held(&store, on_unlisted));
        assert!(is_held(&store, kept));
    }
}
