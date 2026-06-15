//! Track-data column family for locally stored track payloads.

use store::Column;
use tape_crypto::address::Address;
use tape_core::track::data::BlobData;

/// Local track payload data indexed by track address.
///
/// Key: Address (track_address, 32 bytes)
/// Value: BlobData (raw bytes or blob metadata)
pub struct TrackDataCol;

impl Column for TrackDataCol {
    const CF_NAME: &'static str = "track_data";
    type Key = Address;
    type Value = BlobData;
}
