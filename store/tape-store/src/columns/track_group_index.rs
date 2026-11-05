//! Tracks by virtual group, so a spool's track list reads a page at a time

use tape_store_api::Column;

/// Bytes a track group index key takes: virtual group BE, then the track
/// address
pub const TRACK_GROUP_INDEX_KEY_LEN: usize = 34;

/// Every track the store holds, by the virtual group it is stamped with
///
/// Key: virtual group BE (2 bytes), track address (32 bytes)
/// Value: () (the key carries the row)
pub struct TrackGroupIndexCol;

impl Column for TrackGroupIndexCol {
    const CF_NAME: &'static str = "track_group_index";
    type Key = [u8; TRACK_GROUP_INDEX_KEY_LEN];
    type Value = ();
}
