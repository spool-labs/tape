//! Track-data column family for locally stored track payloads.

use crate::types::{Pubkey, TrackData};
use store::Column;

/// Local track payload data indexed by track address.
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: TrackData (raw bytes or blob metadata)
pub struct TrackDataCol;

impl Column for TrackDataCol {
    const CF_NAME: &'static str = "track_data";
    type Key = Pubkey;
    type Value = TrackData;
}
