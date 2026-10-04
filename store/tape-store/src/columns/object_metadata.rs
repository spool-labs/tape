//! Object metadata reverse lookup column family.

use tape_store_api::Column;

use crate::types::{ObjectMetadata, TrackKey};

/// Object metadata indexed by the object's track address and track number.
///
/// Key: TrackKey (track_address, track_number BE, 40 bytes)
/// Value: ObjectMetadata (plaintext name and hot content-type)
pub struct ObjectMetadataCol;

impl Column for ObjectMetadataCol {
    const CF_NAME: &'static str = "object_metadata";
    type Key = TrackKey;
    type Value = ObjectMetadata;
}
