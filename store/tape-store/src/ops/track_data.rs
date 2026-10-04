//! Track-data operations for locally stored payloads.

use tape_store_api::Store;
use tape_crypto::address::Address;
use tape_core::track::data::BlobData;
use tape_core::types::TrackNumber;

use crate::columns::TrackDataCol;
use crate::error::Result;
use crate::types::TrackKey;
use crate::TapeStore;

pub trait TrackDataOps {
    fn get_track_data(
        &self,
        track_address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<BlobData>>;

    /// Track data for several tracks, answered in the order asked.
    fn get_track_datas(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<BlobData>>>;
    fn put_track_data(
        &self,
        track_address: Address,
        track_number: TrackNumber,
        data: BlobData,
    ) -> Result<()>;
    fn delete_track_data(&self, track_address: Address, track_number: TrackNumber) -> Result<()>;
    fn has_track_data(&self, track_address: Address, track_number: TrackNumber) -> Result<bool>;
}

impl<S: Store> TrackDataOps for TapeStore<S> {
    fn get_track_data(
        &self,
        track_address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<BlobData>> {
        Ok(self.get::<TrackDataCol>(&TrackKey::new(track_address, track_number))?)
    }

    fn get_track_datas(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<BlobData>>> {
        let keys: Vec<TrackKey> = tracks
            .iter()
            .map(|(track_address, track_number)| TrackKey::new(*track_address, *track_number))
            .collect();
        Ok(self.get_many::<TrackDataCol>(&keys)?)
    }

    fn put_track_data(
        &self,
        track_address: Address,
        track_number: TrackNumber,
        data: BlobData,
    ) -> Result<()> {
        self.put::<TrackDataCol>(&TrackKey::new(track_address, track_number), &data)?;
        Ok(())
    }

    fn delete_track_data(&self, track_address: Address, track_number: TrackNumber) -> Result<()> {
        self.delete::<TrackDataCol>(&TrackKey::new(track_address, track_number))?;
        Ok(())
    }

    fn has_track_data(&self, track_address: Address, track_number: TrackNumber) -> Result<bool> {
        Ok(self.contains::<TrackDataCol>(&TrackKey::new(track_address, track_number))?)
    }
}
