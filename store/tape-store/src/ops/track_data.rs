//! Track-data operations for locally stored payloads.

use store::Store;
use tape_crypto::address::Address;
use tape_core::track::data::BlobData;

use crate::columns::TrackDataCol;
use crate::error::Result;
use crate::TapeStore;

pub trait TrackDataOps {
    fn get_track_data(&self, track_address: Address) -> Result<Option<BlobData>>;

    /// Track data for several addresses, answered in the order asked.
    fn get_track_datas(&self, track_addresses: &[Address]) -> Result<Vec<Option<BlobData>>>;
    fn put_track_data(&self, track_address: Address, data: BlobData) -> Result<()>;
    fn delete_track_data(&self, track_address: Address) -> Result<()>;
    fn has_track_data(&self, track_address: Address) -> Result<bool>;
}

impl<S: Store> TrackDataOps for TapeStore<S> {
    fn get_track_data(&self, track_address: Address) -> Result<Option<BlobData>> {
        Ok(self.get::<TrackDataCol>(&track_address)?)
    }

    fn get_track_datas(&self, track_addresses: &[Address]) -> Result<Vec<Option<BlobData>>> {
        Ok(self.get_many::<TrackDataCol>(track_addresses)?)
    }

    fn put_track_data(&self, track_address: Address, data: BlobData) -> Result<()> {
        self.put::<TrackDataCol>(&track_address, &data)?;
        Ok(())
    }

    fn delete_track_data(&self, track_address: Address) -> Result<()> {
        self.delete::<TrackDataCol>(&track_address)?;
        Ok(())
    }

    fn has_track_data(&self, track_address: Address) -> Result<bool> {
        Ok(self.contains::<TrackDataCol>(&track_address)?)
    }
}
