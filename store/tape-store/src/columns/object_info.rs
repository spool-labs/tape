//! ObjectInfo column family for tracked object metadata

use tape_store_api::Column;

use crate::types::{ObjectInfo, TrackKey};

/// Object info indexed by track address and track number
///
/// Key: TrackKey (track_address, track_number BE, 40 bytes)
/// Value: ObjectInfo (Blacklisted, Invalid, or Valid)
pub struct ObjectInfoCol;

impl Column for ObjectInfoCol {
    const CF_NAME: &'static str = "object_info";
    type Key = TrackKey;
    type Value = ObjectInfo;
}
