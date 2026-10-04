//! Ordered scan of a tape's tracks with their locally held payloads

use std::collections::VecDeque;

use tape_store_api::Store;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;

use crate::error::{Result, TapeStoreError};
use crate::ops::track::tape_track_addresses;
use crate::ops::{TrackDataOps, TrackOps};
use crate::TapeStore;

/// A track on a tape with its payload, `None` when this store does not hold it
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannedTrack {
    pub address: Address,
    pub track: CompressedTrack,
    pub data: Option<BlobData>,
}

/// Resumable walk of one tape's tracks in track-number order, read in batches
///
/// Holds no borrow of the store, so a caller can await between tracks.
#[derive(Clone, Debug)]
pub struct TapeTrackScan {
    tape: Address,
    batch_size: usize,
    cursor: Option<TrackNumber>,
    buffered: VecDeque<ScannedTrack>,
    is_exhausted: bool,
}

impl TapeTrackScan {
    pub fn new(tape: Address, batch_size: usize) -> Self {
        Self {
            tape,
            batch_size: batch_size.max(1),
            cursor: None,
            buffered: VecDeque::new(),
            is_exhausted: false,
        }
    }

    /// The next track in order, or `None` once the tape has no more
    pub fn next_track<S: Store>(&mut self, store: &TapeStore<S>) -> Result<Option<ScannedTrack>> {
        if self.buffered.is_empty() && !self.is_exhausted {
            self.read_batch(store)?;
        }
        Ok(self.buffered.pop_front())
    }

    fn read_batch<S: Store>(&mut self, store: &TapeStore<S>) -> Result<()> {
        let addresses = tape_track_addresses(store, self.tape, self.cursor, self.batch_size)?;
        if addresses.is_empty() {
            self.is_exhausted = true;
            return Ok(());
        }

        let tracks = store.get_tracks(&addresses)?;
        let datas = store.get_track_datas(&addresses)?;
        for ((address, track), data) in addresses.into_iter().zip(tracks).zip(datas) {
            let track = track.ok_or_else(|| {
                TapeStoreError::Serialization(format!("missing track {address} for lookup index"))
            })?;
            self.cursor = Some(track.track_number);
            self.buffered.push_back(ScannedTrack { address, track, data });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tape_store_memory::MemoryStore;
    use tape_core::spooler::GroupIndex;
    use tape_core::types::StorageUnits;
    use tape_crypto::Hash;

    use super::*;

    const BATCH_SIZE: usize = 2;
    const PAYLOAD: &[u8] = b"payload";

    fn put_track(store: &TapeStore<MemoryStore>, tape: Address, number: u64, is_held: bool) -> Address {
        let track = CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(number),
            kind: 0,
            state: 1,
            flags: 0,
            size: StorageUnits::from_bytes(PAYLOAD.len() as u64),
            group: GroupIndex(0),
            value_hash: Hash::new_unique(),
        };
        let address = Address::new_unique();
        store.put_track(address, track).expect("put track");
        if is_held {
            store
                .put_track_data(address, BlobData::Inline(PAYLOAD.to_vec()))
                .expect("put track data");
        }
        address
    }

    fn scan_all(store: &TapeStore<MemoryStore>, tape: Address) -> Vec<ScannedTrack> {
        let mut scan = TapeTrackScan::new(tape, BATCH_SIZE);
        let mut scanned = Vec::new();
        while let Some(track) = scan.next_track(store).expect("scan") {
            scanned.push(track);
        }
        scanned
    }

    // tracks come back in track-number order across batches and stay on their tape
    #[test]
    fn batch_boundaries() {
        let store = TapeStore::new(MemoryStore::new());
        let tape = Address::new_unique();
        let mut addresses = HashMap::new();
        for number in [3, 0, 4, 1, 2] {
            addresses.insert(number, put_track(&store, tape, number, true));
        }
        put_track(&store, Address::new_unique(), 0, true);

        let scanned = scan_all(&store, tape);

        let mut numbers = Vec::new();
        for track in &scanned {
            assert_eq!(Some(&track.address), addresses.get(&track.track.track_number.0));
            assert_eq!(track.data, Some(BlobData::Inline(PAYLOAD.to_vec())));
            numbers.push(track.track.track_number.0);
        }
        assert_eq!(numbers, vec![0, 1, 2, 3, 4]);
    }

    // a track whose payload is not held is reported without data and the scan continues
    #[test]
    fn missing_data() {
        let store = TapeStore::new(MemoryStore::new());
        let tape = Address::new_unique();
        put_track(&store, tape, 0, true);
        let missing = put_track(&store, tape, 1, false);
        put_track(&store, tape, 2, true);

        let scanned = scan_all(&store, tape);

        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[1].address, missing);
        assert!(scanned[1].data.is_none());
        assert!(scanned[2].data.is_some());
    }

    // a tape the store has never seen scans empty, and stays empty
    #[test]
    fn unknown_tape() {
        let store = TapeStore::new(MemoryStore::new());
        put_track(&store, Address::new_unique(), 0, true);
        let mut scan = TapeTrackScan::new(Address::new_unique(), BATCH_SIZE);

        let first = scan.next_track(&store).expect("scan");
        let second = scan.next_track(&store).expect("scan");

        assert!(first.is_none());
        assert!(second.is_none());
    }
}
