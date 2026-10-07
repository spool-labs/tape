//! Track lookup index for tape-local ordered scans.

use crate::types::TrackLookupKey;
use tape_crypto::address::Address;
use tape_store_api::Column;

/// Tape-local ordered track lookup index.
///
/// Key: TrackLookupKey (tape, tape_number, track_number)
/// Value: the track's address (main track data lives in `track`)
pub struct TrackLookupCol;

impl Column for TrackLookupCol {
    const CF_NAME: &'static str = "track_lookup";
    type Key = TrackLookupKey;
    type Value = Address;
}
