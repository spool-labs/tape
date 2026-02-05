//! ObjectInfo column family for tracked object metadata

use crate::types::{ObjectInfo, Pubkey};
use store::Column;

/// Object info indexed by object address
///
/// Key: Pubkey (object address, 32 bytes)
/// Value: ObjectInfo (Blacklisted, Invalid, or Valid)
pub struct ObjectInfoCol;

impl Column for ObjectInfoCol {
    const CF_NAME: &'static str = "object_info";
    type Key = Pubkey;
    type Value = ObjectInfo;
}
