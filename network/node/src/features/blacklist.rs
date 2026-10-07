use core::mem::size_of;
use std::collections::HashSet;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bytemuck::try_from_bytes;
use tape_store_api::Store;
use tape_api::program::tapedrive::blacklist_pda;
use tape_core::system::BlacklistEntry;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, StorageUnits};
use tape_crypto::Address;
use tape_store::ops::{ObjectInfoOps, TapeOps, TapeTrackScan};
use tape_store::types::{ObjectInfo, SystemObjectKind, TapeInfo};
use tape_store::TapeStore;

use crate::core::error::NodeError;

const BLACKLIST_SCAN_BATCH: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlacklistEntries {
    Ready(HashSet<BlacklistEntry>),
    Pending { track: Address },
}

/// This node's refusal set, held per epoch and rebuilt when its blacklist tape moves
#[derive(Default)]
pub struct BlacklistCache {
    inner: RwLock<Option<Built>>,
}

#[derive(Clone)]
struct Built {
    node: Address,
    epoch: EpochNumber,
    tape: Option<TapeInfo>,
    entries: Arc<HashSet<BlacklistEntry>>,
}

impl BlacklistCache {
    /// This node's refusal set for the epoch
    pub fn entries<Db: Store>(
        &self,
        store: &TapeStore<Db>,
        node: Address,
        epoch: EpochNumber,
    ) -> Result<Arc<HashSet<BlacklistEntry>>, NodeError> {
        let built = self.built_for(node, epoch);
        // The pda derives from the node, so a cached build holds this same one.
        let blacklist = blacklist_pda(node).0;

        let tape = store
            .get_tape(blacklist)
            .map_err(|error| NodeError::Store(format!("blacklist tape lookup: {error}")))?;

        match built {
            Some(built) if built.tape == tape => Ok(built.entries),
            Some(_) | None => self.build(store, node, epoch, blacklist, tape),
        }
    }

    fn built_for(&self, node: Address, epoch: EpochNumber) -> Option<Built> {
        let guard = self.read();
        match guard.as_ref() {
            Some(built) if built.node == node && built.epoch == epoch => Some(built.clone()),
            Some(_) | None => None,
        }
    }

    fn build<Db: Store>(
        &self,
        store: &TapeStore<Db>,
        node: Address,
        epoch: EpochNumber,
        blacklist: Address,
        tape: Option<TapeInfo>,
    ) -> Result<Arc<HashSet<BlacklistEntry>>, NodeError> {
        let entries = match collect_blacklist_entries(store, blacklist, tape.as_ref(), epoch, None)? {
            BlacklistEntries::Ready(entries) => Arc::new(entries),
            BlacklistEntries::Pending { track } => {
                return Err(NodeError::Store(format!(
                    "missing blacklist track data for {track}"
                )))
            }
        };

        *self.write() = Some(Built {
            node,
            epoch,
            tape,
            entries: entries.clone(),
        });

        Ok(entries)
    }

    fn read(&self) -> RwLockReadGuard<'_, Option<Built>> {
        self.inner.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, Option<Built>> {
        self.inner.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub fn refuses_object<Db: Store>(
    store: &TapeStore<Db>,
    cache: &BlacklistCache,
    node: Address,
    epoch: EpochNumber,
    track: Address,
    tape: Address,
) -> Result<bool, NodeError> {
    let entries = cache.entries(store, node, epoch)?;
    Ok(entries.contains(&BlacklistEntry::track(track)) || entries.contains(&BlacklistEntry::tape(tape)))
}

pub fn blacklist_entries_for_assignment<Db: Store>(
    store: &TapeStore<Db>,
    node: Address,
    voting_epoch: EpochNumber,
    target_epoch: EpochNumber,
) -> Result<BlacklistEntries, NodeError> {
    let blacklist = blacklist_pda(node).0;
    let tape = store
        .get_tape(blacklist)
        .map_err(|error| NodeError::Store(format!("blacklist tape lookup: {error}")))?;

    collect_blacklist_entries(store, blacklist, tape.as_ref(), target_epoch, Some(voting_epoch))
}

fn collect_blacklist_entries<Db: Store>(
    store: &TapeStore<Db>,
    blacklist: Address,
    tape: Option<&TapeInfo>,
    active_epoch: EpochNumber,
    cutoff_epoch: Option<EpochNumber>,
) -> Result<BlacklistEntries, NodeError> {
    let Some(tape) = tape.filter(|tape| tape.end_epoch > active_epoch) else {
        return Ok(BlacklistEntries::Ready(HashSet::new()));
    };

    let mut scan = TapeTrackScan::new(blacklist, tape.id, BLACKLIST_SCAN_BATCH);
    let mut entries = HashSet::new();
    while let Some(scanned) = scan
        .next_track(store)
        .map_err(|error| NodeError::Store(format!("blacklist track scan: {error}")))?
    {
        let is_in_window = match cutoff_epoch {
            Some(cutoff_epoch) => {
                blacklist_track_is_before_cutoff(store, scanned.address, cutoff_epoch)?
            }
            // A removed entry stops refusing as soon as the set is rebuilt.
            None => {
                let info = store.get_object_info(scanned.address).map_err(|error| {
                    NodeError::Store(format!("blacklist object info lookup: {error}"))
                })?;
                !matches!(info, Some(ObjectInfo::Deleted { .. }))
            }
        };

        if !is_in_window {
            continue;
        }

        let Some(data) = &scanned.data else {
            return Ok(BlacklistEntries::Pending {
                track: scanned.address,
            });
        };

        let entry = decode_blacklist_entry(scanned.address, blacklist, &scanned.track, data)?;
        entries.insert(entry);
    }

    Ok(BlacklistEntries::Ready(entries))
}

pub fn decode_blacklist_entry(
    track_address: Address,
    blacklist: Address,
    track: &CompressedTrack,
    data: &BlobData,
) -> Result<BlacklistEntry, NodeError> {
    if track.tape != blacklist || !track.is_inline() || !track.is_certified() {
        return Err(NodeError::Store(format!(
            "invalid blacklist track metadata for {track_address}"
        )));
    }

    let expected_size = StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64);
    if track.size != expected_size {
        return Err(NodeError::Store(format!(
            "invalid blacklist track size {} for {track_address}",
            track.size.0
        )));
    }

    let BlobData::Inline(bytes) = data else {
        return Err(NodeError::Store(format!("invalid blacklist track data for {track_address}")));
    };

    let entry = try_from_bytes::<BlacklistEntry>(bytes).map_err(|_| {
        NodeError::Store(format!("invalid blacklist entry data for {track_address}"))
    })?;
    if !entry.is_valid() || track.key != entry.key() || track.value_hash != entry.key() {
        return Err(NodeError::Store(format!(
            "invalid blacklist entry hash for {track_address}"
        )));
    }

    Ok(*entry)
}

fn blacklist_track_is_before_cutoff<Db: Store>(
    store: &TapeStore<Db>,
    track: Address,
    cutoff_epoch: EpochNumber,
) -> Result<bool, NodeError> {
    let Some(info) = store
        .get_object_info(track)
        .map_err(|error| NodeError::Store(format!("blacklist object info lookup: {error}")))?
    else {
        return Err(NodeError::Store(format!(
            "missing blacklist object info for {track}"
        )));
    };

    let (track_address, registered_epoch, certified_epoch, removed_epoch) = match info {
        ObjectInfo::Valid {
            track_address,
            registered_epoch,
            certified_epoch,
            ..
        }
        | ObjectInfo::System {
            kind: SystemObjectKind::Blacklist,
            track_address,
            registered_epoch,
            certified_epoch,
            ..
        } => (track_address, registered_epoch, certified_epoch, None),
        ObjectInfo::Deleted {
            kind: None | Some(SystemObjectKind::Blacklist),
            track_address,
            registered_epoch,
            certified_epoch,
            deleted_epoch,
        } => (track_address, registered_epoch, Some(certified_epoch), Some(deleted_epoch)),
        _ => {
            return Err(NodeError::Store(format!(
                "invalid blacklist object info for {track}"
            )))
        }
    };

    if track_address != track {
        return Err(NodeError::Store(format!(
            "blacklist object info points at a different track for {track}"
        )));
    }

    let Some(certified_epoch) = certified_epoch else {
        return Err(NodeError::Store(format!(
            "blacklist track is not certified for {track}"
        )));
    };

    // An entry removed in the cutoff epoch still counts in that epoch's round,
    // since the removal reaches each node at its own moment.
    let is_removed = removed_epoch.is_some_and(|removed_epoch| removed_epoch < cutoff_epoch);

    Ok(registered_epoch < cutoff_epoch && certified_epoch < cutoff_epoch && !is_removed)
}

#[cfg(test)]
mod tests {
    use bytemuck::bytes_of;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::{TrackFlags, TrackKind, TrackState};
    use tape_core::types::{TapeNumber, TrackNumber};
    use tape_store::ops::{TrackDataOps, TrackOps};
    use tape_store_memory::MemoryStore;

    use super::*;

    const EPOCH: EpochNumber = EpochNumber(4);

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    /// Writes one entry onto the node's blacklist tape the way a replayed add leaves it.
    fn seed_entry(
        store: &TapeStore<MemoryStore>,
        node: Address,
        track_number: TrackNumber,
        entry: BlacklistEntry,
    ) -> Address {
        let blacklist = blacklist_pda(node).0;
        let track = CompressedTrack {
            tape: blacklist,
            tape_number: TapeNumber(1),
            track_number,
            key: entry.key(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(size_of::<BlacklistEntry>() as u64),
            group: VirtualGroup(0),
            value_hash: entry.key(),
        };
        let track_address = track.address();

        store
            .put_tape(
                blacklist,
                TapeInfo::new(
                    TapeNumber(1),
                    0,
                    EpochNumber(u64::MAX),
                    TrackNumber(track_number.0 + 1),
                ),
            )
            .expect("seed blacklist tape");

        store
            .put_track(track_address, track)
            .expect("seed blacklist track");

        store
            .put_track_data(track_address, BlobData::Inline(bytes_of(&entry).to_vec()))
            .expect("seed blacklist track data");

        track_address
    }

    fn refuses(
        store: &TapeStore<MemoryStore>,
        cache: &BlacklistCache,
        node: Address,
        epoch: EpochNumber,
        track: Address,
    ) -> Result<bool, NodeError> {
        refuses_object(store, cache, node, epoch, track, Address::new_unique())
    }

    // a second call in the same epoch answers without reading the entry's track data
    #[test]
    fn holds_the_set() {
        let store = test_store();
        let node = Address::new_unique();
        let target = Address::new_unique();
        let entry_track = seed_entry(&store, node, TrackNumber(0), BlacklistEntry::track(target));
        let cache = BlacklistCache::default();

        assert!(refuses(&store, &cache, node, EPOCH, target).expect("first call"));

        store
            .delete_track_data(entry_track)
            .expect("drop the entry data");

        assert!(refuses(&store, &cache, node, EPOCH, target).expect("second call"));
        assert!(
            refuses(&store, &BlacklistCache::default(), node, EPOCH, target).is_err(),
            "the entry data is what a fresh cache has to read"
        );
    }

    // an entry written after the first call refuses on the next one
    #[test]
    fn new_entry() {
        let store = test_store();
        let node = Address::new_unique();
        let first = Address::new_unique();
        let second = Address::new_unique();
        seed_entry(&store, node, TrackNumber(0), BlacklistEntry::track(first));
        let cache = BlacklistCache::default();

        assert!(!refuses(&store, &cache, node, EPOCH, second).expect("first call"));

        seed_entry(&store, node, TrackNumber(1), BlacklistEntry::track(second));

        assert!(refuses(&store, &cache, node, EPOCH, second).expect("second call"));
        assert!(refuses(&store, &cache, node, EPOCH, first).expect("earlier entry"));
    }

    // the epoch advancing builds the set again
    #[test]
    fn next_epoch() {
        let store = test_store();
        let node = Address::new_unique();
        let target = Address::new_unique();
        let entry_track = seed_entry(&store, node, TrackNumber(0), BlacklistEntry::track(target));
        let cache = BlacklistCache::default();

        assert!(refuses(&store, &cache, node, EPOCH, target).expect("first epoch"));

        store.delete_track(entry_track).expect("drop the entry");
        store
            .delete_track_data(entry_track)
            .expect("drop the entry data");

        assert!(
            !refuses(&store, &cache, node, EpochNumber(EPOCH.0 + 1), target).expect("next epoch"),
            "the set was answered from the epoch before"
        );
    }

    // a removed entry stops refusing once the set is rebuilt, and still counts
    // in the assignment round of the epoch it was removed in
    #[test]
    fn removed_entry() {
        let store = test_store();
        let node = Address::new_unique();
        let target = Address::new_unique();
        let entry_track = seed_entry(&store, node, TrackNumber(0), BlacklistEntry::track(target));
        let next = EpochNumber(EPOCH.0 + 1);

        store
            .put_object_info(
                entry_track,
                ObjectInfo::Deleted {
                    kind: Some(SystemObjectKind::Blacklist),
                    track_address: entry_track,
                    registered_epoch: EpochNumber(2),
                    certified_epoch: EpochNumber(2),
                    deleted_epoch: EPOCH,
                },
            )
            .expect("flag the entry");

        assert!(
            !refuses(&store, &BlacklistCache::default(), node, EPOCH, target).expect("refusal"),
        );

        let counted = blacklist_entries_for_assignment(&store, node, EPOCH, next)
            .expect("round of the removal");
        assert_eq!(
            counted,
            BlacklistEntries::Ready(HashSet::from([BlacklistEntry::track(target)]))
        );

        let later = blacklist_entries_for_assignment(&store, node, next, EpochNumber(next.0 + 1))
            .expect("next round");
        assert_eq!(later, BlacklistEntries::Ready(HashSet::new()));
    }

    // a tape entry refuses every track on that tape, and an empty set refuses nothing
    #[test]
    fn tape_entry() {
        let store = test_store();
        let node = Address::new_unique();
        let tape = Address::new_unique();
        seed_entry(&store, node, TrackNumber(0), BlacklistEntry::tape(tape));
        let cache = BlacklistCache::default();

        let track = Address::new_unique();
        assert!(refuses_object(&store, &cache, node, EPOCH, track, tape).expect("tape entry"));
        assert!(
            !refuses_object(&store, &cache, node, EPOCH, track, Address::new_unique())
                .expect("other tape")
        );

        let other_node = Address::new_unique();
        assert!(!refuses(&store, &cache, other_node, EPOCH, track).expect("node with no tape"));
    }
}
