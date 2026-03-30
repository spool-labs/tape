//! Track column family for canonical compressed-track metadata.

use crate::types::{PackedTrack, Pubkey};
use store::Column;

/// Track catalog indexed by track address.
///
/// Key: Pubkey (track_address, 32 bytes)
/// Value: PackedTrack (`CompressedTrack` bytes)
pub struct TrackCol;

impl Column for TrackCol {
    const CF_NAME: &'static str = "track";
    type Key = Pubkey;
    type Value = PackedTrack;
}
