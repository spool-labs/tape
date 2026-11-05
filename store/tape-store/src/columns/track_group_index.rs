//! Tracks by virtual group, so a spool's track list reads a page at a time

use tape_store_api::Column;

use crate::types::keys::TrackGroupKey;

/// Every track the store holds, by the virtual group it is stamped with
///
/// Key: TrackGroupKey (34 bytes)
/// Value: () (the key carries the row)
pub struct TrackGroupIndexCol;

impl Column for TrackGroupIndexCol {
    const CF_NAME: &'static str = "track_group_index";
    type Key = [u8; TrackGroupKey::SIZE];
    type Value = ();
}
