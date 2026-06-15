//! Object metadata reverse lookup column family.

use store::Column;
use tape_crypto::address::Address;

use crate::types::ObjectMetadata;

/// Object metadata indexed by object track address.
///
/// Key: Address (object track address, 32 bytes)
/// Value: ObjectMetadata (plaintext name and hot content-type)
pub struct ObjectMetadataCol;

impl Column for ObjectMetadataCol {
    const CF_NAME: &'static str = "object_metadata";
    type Key = Address;
    type Value = ObjectMetadata;
}
