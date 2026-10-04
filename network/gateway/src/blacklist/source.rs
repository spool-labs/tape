//! The gateway's store as blacklists read it

use std::fmt::Display;

use tape_store_api::Store;
use tape_core::track::data::BlobData;
use tape_core::types::{EpochNumber, TrackNumber};
use tape_crypto::address::Address;
use tape_node::core::error::NodeError;
use tape_rack::source::{TapeSource, Track};
use tape_store::TapeStore;
use tape_store::ops::{TapeOps, TrackDataOps, TrackOps, tape_track_numbers};

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
        let Some(address) = self
            .store
            .track_address_by_number(tape, track_number)
            .map_err(store_error)?
        else {
            return Ok(Track::Absent);
        };
        let Some(track) = self.store.get_track(address, track_number).map_err(store_error)? else {
            return Ok(Track::Absent);
        };
        if track.is_invalidated() {
            return Ok(Track::Absent);
        }
        if !track.is_inline() {
            return Ok(Track::Opaque);
        }
        match self.store.get_track_data(address, track_number).map_err(store_error)? {
            Some(BlobData::Inline(bytes)) => Ok(Track::Written { address, bytes }),
            Some(BlobData::Coded(_)) => Ok(Track::Opaque),
            None => Ok(Track::Pending { address }),
        }
    }

    fn track_numbers(&self, tape: Address) -> Result<Vec<TrackNumber>, NodeError> {
        let mut numbers = Vec::new();
        let mut after = None;
        loop {
            let batch = tape_track_numbers(self.store, tape, after, LISTING_BATCH).map_err(store_error)?;
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
