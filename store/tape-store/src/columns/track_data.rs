//! Track-data column family for locally stored track payloads.

use tape_store_api::Column;
use tape_core::track::data::BlobData;

use crate::types::TrackKey;

/// Local track payload data indexed by track address and track number.
///
/// Key: TrackKey (track_address, track_number BE, 40 bytes)
/// Value: BlobData (raw bytes or blob metadata)
pub struct TrackDataCol;

impl Column for TrackDataCol {
    const CF_NAME: &'static str = "track_data";
    type Key = TrackKey;
    type Value = BlobData;
}
