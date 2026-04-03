//! ObjectInfo column family for tracked object metadata

use store::Column;
use tape_crypto::address::Address;

use crate::types::ObjectInfo;

/// Object info indexed by object address
///
/// Key: Address (object address, 32 bytes)
/// Value: ObjectInfo (Blacklisted, Invalid, or Valid)
pub struct ObjectInfoCol;

impl Column for ObjectInfoCol {
    const CF_NAME: &'static str = "object_info";
    type Key = Address;
    type Value = ObjectInfo;
}
