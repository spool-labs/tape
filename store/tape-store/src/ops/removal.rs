//! Tapes closed or rewound in the current epoch, and the tracks that left with them.

use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, TapeNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_store_api::{Column, Store};

use crate::columns::RemovalLogCol;
use crate::error::{Result, TapeStoreError};
use crate::types::{ObjectListEntry, RemovalKey, TapeInfo};
use crate::TapeStore;

/// Whether a track has left its tape: the tape was closed, or it was rewound
/// or renewed after the track was written. `tape` is the row at the track's
/// tape address, if there is one.
///
/// A close deletes the row, a tape reserved again at the address has a new
/// number, and a rewind or a renew moves the tree's start past every old track.
pub fn track_has_left(tape: Option<&TapeInfo>, track: &CompressedTrack) -> bool {
    match tape {
        Some(tape) => tape.id != track.tape_number || track.track_number < tape.tree_start,
        None => true,
    }
}

/// Whether a listed object's track has left its tape, by the rule of
/// `track_has_left`. `tape` is the row at the address the object is listed
/// under. The entry stays in the listing until the sweep takes the track.
pub fn object_has_left(tape: Option<&TapeInfo>, entry: &ObjectListEntry) -> bool {
    match tape {
        Some(tape) => tape.id != entry.tape_number || entry.track_number < tape.tree_start,
        None => true,
    }
}

/// Operations for the log of tapes closed or rewound in an epoch.
///
/// A track that has left still counts in the assignment round of the epoch it
/// left in. The log is what tells such a track from one that left earlier.
pub trait RemovalOps {
    /// Record that a tape was closed or rewound in an epoch. `tree_start` is
    /// the tape's tree start before the event. The first record for a tape and
    /// epoch stays, so the entry covers every track the tape held in that epoch.
    fn record_removal(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
        tree_start: TrackNumber,
    ) -> Result<()>;

    /// The tree start a tape had before its first removal in an epoch.
    fn get_removal(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
    ) -> Result<Option<TrackNumber>>;

    /// The tree starts recorded for a tape in `epoch` and every later one.
    fn removals_from(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
    ) -> Result<Vec<TrackNumber>>;

    /// Drop every entry from before an epoch, returning how many went.
    fn prune_removals_before(&self, epoch: EpochNumber) -> Result<usize>;
}

impl<S: Store> RemovalOps for TapeStore<S> {
    fn record_removal(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
        tree_start: TrackNumber,
    ) -> Result<()> {
        let key = RemovalKey::new(tape, tape_number, epoch);
        if self.contains::<RemovalLogCol>(&key)? {
            return Ok(());
        }
        self.put::<RemovalLogCol>(&key, &tree_start)?;
        Ok(())
    }

    fn get_removal(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
    ) -> Result<Option<TrackNumber>> {
        Ok(self.get::<RemovalLogCol>(&RemovalKey::new(tape, tape_number, epoch))?)
    }

    fn removals_from(
        &self,
        tape: Address,
        tape_number: TapeNumber,
        epoch: EpochNumber,
    ) -> Result<Vec<TrackNumber>> {
        let start_key = encode_key(&RemovalKey::new(tape, tape_number, epoch))?;
        let iter = self
            .inner()
            .inner()
            .iter_from(RemovalLogCol::CF_NAME, &start_key, tape_store_api::Direction::Asc)?;

        let mut tree_starts = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key = decode_key(&key_bytes)?;
            if key.tape != tape || key.tape_number != tape_number {
                break;
            }
            let tree_start: TrackNumber = wincode::deserialize(&value_bytes)
                .map_err(|e| TapeStoreError::Serialization(format!("removal value: {}", e)))?;
            tree_starts.push(tree_start);
        }

        Ok(tree_starts)
    }

    fn prune_removals_before(&self, epoch: EpochNumber) -> Result<usize> {
        // The log holds a row per tape closed or rewound in an epoch, so a
        // walk of the whole column is short.
        let iter = self
            .inner()
            .inner()
            .iter_from(RemovalLogCol::CF_NAME, &[], tape_store_api::Direction::Asc)?;

        let mut stale = Vec::new();
        for (key_bytes, _) in iter {
            let key = decode_key(&key_bytes)?;
            if key.epoch < epoch {
                stale.push(key);
            }
        }

        for key in &stale {
            self.delete::<RemovalLogCol>(key)?;
        }

        Ok(stale.len())
    }
}

fn encode_key(key: &RemovalKey) -> Result<Vec<u8>> {
    wincode::serialize(key)
        .map_err(|e| TapeStoreError::Serialization(format!("removal key: {}", e)))
}

fn decode_key(bytes: &[u8]) -> Result<RemovalKey> {
    wincode::deserialize(bytes)
        .map_err(|e| TapeStoreError::Serialization(format!("removal key: {}", e)))
}

#[cfg(test)]
mod tests {
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::types::StorageUnits;
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    use super::*;

    const TAPE_NUMBER: TapeNumber = TapeNumber(7);
    const EPOCH: EpochNumber = EpochNumber(5);

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn track(tape: Address, tape_number: TapeNumber, track_number: u64) -> CompressedTrack {
        CompressedTrack {
            tape,
            tape_number,
            track_number: TrackNumber(track_number),
            key: Hash::new_unique(),
            kind: 0,
            state: 1,
            flags: 0,
            size: StorageUnits(1024),
            group: VirtualGroup(0),
            value_hash: Hash::new_unique(),
        }
    }

    fn tape_row(tree_start: u64) -> TapeInfo {
        TapeInfo {
            tree_start: TrackNumber(tree_start),
            ..TapeInfo::new(TAPE_NUMBER, TapeFlags::NONE, EpochNumber(10), TrackNumber(9))
        }
    }

    // a track is on its tape while the row has its tape number and a tree
    // start at or below its number
    #[test]
    fn has_left() {
        let tape = Address::new_unique();
        let row = tape_row(4);

        assert!(!track_has_left(Some(&row), &track(tape, TAPE_NUMBER, 4)));
        assert!(!track_has_left(Some(&row), &track(tape, TAPE_NUMBER, 8)));

        // rewound or renewed past the track
        assert!(track_has_left(Some(&row), &track(tape, TAPE_NUMBER, 3)));
        // closed and reserved again under a new number
        assert!(track_has_left(Some(&row), &track(tape, TapeNumber(6), 4)));
        // closed
        assert!(track_has_left(None, &track(tape, TAPE_NUMBER, 4)));
    }

    // the first record for a tape and epoch stays
    #[test]
    fn first_record_stays() {
        let store = test_store();
        let tape = Address::new_unique();

        store.record_removal(tape, TAPE_NUMBER, EPOCH, TrackNumber(0)).unwrap();
        store.record_removal(tape, TAPE_NUMBER, EPOCH, TrackNumber(41)).unwrap();

        assert_eq!(store.get_removal(tape, TAPE_NUMBER, EPOCH).unwrap(), Some(TrackNumber(0)));
        assert_eq!(store.get_removal(tape, TAPE_NUMBER, EPOCH.next()).unwrap(), None);
        assert_eq!(store.get_removal(tape, TapeNumber(8), EPOCH).unwrap(), None);
        assert_eq!(store.get_removal(Address::new_unique(), TAPE_NUMBER, EPOCH).unwrap(), None);
    }

    // a read from an epoch on returns that epoch's entry and later ones, for
    // the one tape asked about
    #[test]
    fn from_an_epoch() {
        let store = test_store();
        let tape = Address::new_unique();

        store.record_removal(tape, TAPE_NUMBER, EpochNumber(4), TrackNumber(0)).unwrap();
        store.record_removal(tape, TAPE_NUMBER, EpochNumber(5), TrackNumber(10)).unwrap();
        store.record_removal(tape, TAPE_NUMBER, EpochNumber(6), TrackNumber(20)).unwrap();
        store.record_removal(tape, TapeNumber(8), EpochNumber(6), TrackNumber(30)).unwrap();
        store
            .record_removal(Address::new_unique(), TAPE_NUMBER, EpochNumber(6), TrackNumber(40))
            .unwrap();

        assert_eq!(
            store.removals_from(tape, TAPE_NUMBER, EpochNumber(5)).unwrap(),
            vec![TrackNumber(10), TrackNumber(20)]
        );
        assert!(store.removals_from(tape, TAPE_NUMBER, EpochNumber(7)).unwrap().is_empty());
    }

    // pruning drops the entries of earlier epochs and keeps the rest
    #[test]
    fn prune() {
        let store = test_store();
        let old = Address::new_unique();
        let current = Address::new_unique();

        store.record_removal(old, TAPE_NUMBER, EpochNumber(4), TrackNumber(0)).unwrap();
        store.record_removal(current, TAPE_NUMBER, EPOCH, TrackNumber(0)).unwrap();

        assert_eq!(store.prune_removals_before(EPOCH).unwrap(), 1);
        assert_eq!(store.get_removal(old, TAPE_NUMBER, EpochNumber(4)).unwrap(), None);
        assert_eq!(store.get_removal(current, TAPE_NUMBER, EPOCH).unwrap(), Some(TrackNumber(0)));
        assert_eq!(store.prune_removals_before(EPOCH).unwrap(), 0);
    }
}
