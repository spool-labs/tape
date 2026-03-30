//! Track lookup index for tape-local ordered scans.

use crate::types::{TrackLookupKey, UnitKey};
use store::Column;

/// Tape-local ordered track lookup index.
///
/// Key: TrackLookupKey (tape, track_number, key)
/// Value: UnitKey (marker only; main track data lives in `track`)
pub struct TrackLookupCol;

impl Column for TrackLookupCol {
    const CF_NAME: &'static str = "track_lookup";
    type Key = TrackLookupKey;
    type Value = UnitKey;
}
