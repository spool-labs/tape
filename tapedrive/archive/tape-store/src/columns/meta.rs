//! Meta column family for schema version, cursors, and epoch pointers

use store::Column;

/// Meta column for storing miscellaneous metadata
/// Key: String (e.g., "schema_version", "chain_cursor", "current_epoch")
/// Value: Vec<u8> (arbitrary data)
pub struct Meta;

impl Column for Meta {
    const CF_NAME: &'static str = "meta";
    type Key = String;
    type Value = Vec<u8>;
}
