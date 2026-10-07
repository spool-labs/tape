//! The gateway's store as blacklists read it

use std::fmt::Display;

use tape_store_api::Store;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_node::core::error::NodeError;
use tape_rack::source::{TapeSource, Track};
use tape_store::TapeStore;
use tape_store::ops::{TapeOps, TrackDataOps, TrackOps, tape_track_numbers, track_has_left};

/// Track numbers read per batch while listing a tape
const LISTING_BATCH: usize = 4_096;

/// Tapes and tracks as the gateway's store holds them, at the epoch the gateway has replayed to
pub struct StoreSource<'store, Db: Store> {
    store: &'store TapeStore<Db>,
    epoch: EpochNumber,
}

impl<'store, Db: Store> StoreSource<'store, Db> {
    pub fn new(store: &'store TapeStore<Db>, epoch: EpochNumber) -> Self {
        Self { store, epoch }
    }
}

impl<Db: Store> TapeSource for StoreSource<'_, Db> {
    type Error = NodeError;

    fn is_live(&self, tape: Address) -> Result<bool, NodeError> {
        let info = self.store.get_tape(tape).map_err(store_error)?;
        Ok(info.is_some_and(|info| info.end_epoch > self.epoch))
    }

    fn track(&self, tape: Address, track_number: TrackNumber) -> Result<Track, NodeError> {
        // The row has the number of the tape at this address now.
        let Some(info) = self.store.get_tape(tape).map_err(store_error)? else {
            return Ok(Track::Absent);
        };
        let Some(address) = self
            .store
            .track_address_by_number(tape, info.id, track_number)
            .map_err(store_error)?
        else {
            return Ok(Track::Absent);
        };
        let Some(track) = self.store.get_track(address).map_err(store_error)? else {
            return Ok(Track::Absent);
        };
        // A track from before a rewind or a renew is gone, and the sweep has
        // yet to take its row.
        if track_has_left(Some(&info), &track) || track.is_invalidated() {
            return Ok(Track::Absent);
        }
        if !track.is_inline() {
            return Ok(Track::Opaque);
        }
        match self.store.get_track_data(address).map_err(store_error)? {
            Some(BlobData::Inline(bytes)) => Ok(Track::Written { address, bytes }),
            Some(BlobData::Coded(_)) => Ok(Track::Opaque),
            None => Ok(Track::Pending { address }),
        }
    }

    fn track_numbers(&self, tape: Address) -> Result<Vec<TrackNumber>, NodeError> {
        let mut numbers = Vec::new();
        let Some(info) = self.store.get_tape(tape).map_err(store_error)? else {
            return Ok(numbers);
        };
        // The tracks from the tree's start on are the ones the tape holds.
        let mut after = info.tree_start.checked_prev();
        loop {
            let batch = tape_track_numbers(self.store, tape, info.id, after, LISTING_BATCH)
                .map_err(store_error)?;
            let Some(last) = batch.last().copied() else {
                return Ok(numbers);
            };
            let is_last_batch = batch.len() < LISTING_BATCH;
            numbers.extend(batch);
            if is_last_batch {
                return Ok(numbers);
            }
            after = Some(last);
        }
    }
}

fn store_error(error: impl Display) -> NodeError {
    NodeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_store_memory::MemoryStore;
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::spooler::VirtualGroup;
    use tape_core::tape::TapeFlags;
    use tape_core::track::types::{CompressedTrack, TrackFlags, TrackKind, TrackState};
    use tape_core::types::{StorageUnits, TapeNumber};
    use tape_crypto::Hash;
    use tape_store::types::TapeInfo;

    use super::*;

    const BYTES: &[u8] = b"entry";

    fn put_track(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        tape_number: TapeNumber,
        track_number: TrackNumber,
    ) -> Address {
        let track = CompressedTrack {
            tape,
            tape_number,
            track_number,
            key: Hash::new_unique(),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            flags: TrackFlags::NONE,
            size: StorageUnits::from_bytes(BYTES.len() as u64),
            group: VirtualGroup(0),
            value_hash: Hash::new_unique(),
        };
        let address = track.address();
        store.put_track(address, track).expect("put track");
        store
            .put_track_data(address, BlobData::Inline(BYTES.to_vec()))
            .expect("put track data");
        address
    }

    // a tape reserved again at its old address reads as its new tracks alone,
    // and an address with no tape row has no tracks
    #[test]
    fn tracks_by_tape_number() {
        let store = TapeStore::new(MemoryStore::new());
        let tape = Address::new_unique();
        let info = TapeInfo::new(TapeNumber(2), TapeFlags::NONE, EpochNumber(10), TrackNumber(1));
        store.put_tape(tape, info).expect("put tape");
        put_track(&store, tape, TapeNumber(1), TrackNumber(0));
        put_track(&store, tape, TapeNumber(1), TrackNumber(1));
        let address = put_track(&store, tape, TapeNumber(2), TrackNumber(0));
        let source = StoreSource::new(&store, EpochNumber(1));

        assert_eq!(source.track_numbers(tape).expect("numbers"), vec![TrackNumber(0)]);
        assert_eq!(
            source.track(tape, TrackNumber(0)).expect("track"),
            Track::Written { address, bytes: BYTES.to_vec() }
        );
        assert_eq!(source.track(tape, TrackNumber(1)).expect("track"), Track::Absent);

        let unknown = Address::new_unique();
        assert!(source.track_numbers(unknown).expect("numbers").is_empty());
        assert_eq!(source.track(unknown, TrackNumber(0)).expect("track"), Track::Absent);
    }

    // a rewound tape reads as the tracks written since the rewind
    #[test]
    fn tracks_from_tree_start() {
        let store = TapeStore::new(MemoryStore::new());
        let tape = Address::new_unique();
        let info = TapeInfo {
            tree_start: TrackNumber(2),
            ..TapeInfo::new(TapeNumber(1), TapeFlags::NONE, EpochNumber(10), TrackNumber(3))
        };
        store.put_tape(tape, info).expect("put tape");
        put_track(&store, tape, TapeNumber(1), TrackNumber(0));
        put_track(&store, tape, TapeNumber(1), TrackNumber(1));
        let address = put_track(&store, tape, TapeNumber(1), TrackNumber(2));
        let source = StoreSource::new(&store, EpochNumber(1));

        assert_eq!(source.track_numbers(tape).expect("numbers"), vec![TrackNumber(2)]);
        assert_eq!(source.track(tape, TrackNumber(0)).expect("track"), Track::Absent);
        assert_eq!(
            source.track(tape, TrackNumber(2)).expect("track"),
            Track::Written { address, bytes: BYTES.to_vec() }
        );
    }
}
