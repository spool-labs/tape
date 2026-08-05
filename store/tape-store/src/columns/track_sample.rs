//! The challenge sample set, one row per track in a group
//!
//! Key structure: [group BE 8 bytes][track 32 bytes]

use store::Column;

use crate::types::{TrackSample, TrackSampleKey};

/// What a round draws from, derived entirely from replayed state
///
/// Written when a registration replays, marked when a deletion replays, never
/// touched by what a node holds. Group-first keys make one group's set a prefix
/// scan, already in the track order the draw is defined over.
///
/// Key: group and track address (40 bytes)
/// Value: slice length, registration slot, and deletion slot once deleted
pub struct TrackSampleCol;

impl Column for TrackSampleCol {
    const CF_NAME: &'static str = "track_sample";
    type Key = TrackSampleKey;
    type Value = TrackSample;
}
