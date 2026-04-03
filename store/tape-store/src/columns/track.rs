//! Track column family for canonical compressed-track metadata.

use store::Column;
use tape_crypto::address::Address;

use crate::types::PackedTrack;

/// Track catalog indexed by track address.
///
/// Key: Address (track_address, 32 bytes)
/// Value: PackedTrack (`CompressedTrack` bytes)
pub struct TrackCol;

impl Column for TrackCol {
    const CF_NAME: &'static str = "track";
    type Key = Address;
    type Value = PackedTrack;
}
