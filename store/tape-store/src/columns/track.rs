//! Track column family for canonical compressed-track metadata.

use tape_store_api::Column;
use tape_core::track::types::PackedTrack;

use crate::types::TrackKey;

/// Track catalog indexed by track address and track number.
///
/// Key: TrackKey (track_address, track_number BE, 40 bytes)
/// Value: PackedTrack (`CompressedTrack` bytes)
pub struct TrackCol;

impl Column for TrackCol {
    const CF_NAME: &'static str = "track";
    type Key = TrackKey;
    type Value = PackedTrack;
}
