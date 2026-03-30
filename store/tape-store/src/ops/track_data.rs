//! Track-data operations for locally stored payloads.

use crate::columns::TrackDataCol;
use crate::error::Result;
use crate::types::{Pubkey, TrackData};
use crate::TapeStore;
use store::Store;

pub trait TrackDataOps {
    fn get_track_data(&self, track_address: Pubkey) -> Result<Option<TrackData>>;
    fn put_track_data(&self, track_address: Pubkey, data: TrackData) -> Result<()>;
    fn delete_track_data(&self, track_address: Pubkey) -> Result<()>;
    fn has_track_data(&self, track_address: Pubkey) -> Result<bool>;
}

impl<S: Store> TrackDataOps for TapeStore<S> {
    fn get_track_data(&self, track_address: Pubkey) -> Result<Option<TrackData>> {
        Ok(self.get::<TrackDataCol>(&track_address)?)
    }

    fn put_track_data(&self, track_address: Pubkey, data: TrackData) -> Result<()> {
        self.put::<TrackDataCol>(&track_address, &data)?;
        Ok(())
    }

    fn delete_track_data(&self, track_address: Pubkey) -> Result<()> {
        self.delete::<TrackDataCol>(&track_address)?;
        Ok(())
    }

    fn has_track_data(&self, track_address: Pubkey) -> Result<bool> {
        Ok(self.contains::<TrackDataCol>(&track_address)?)
    }
}
