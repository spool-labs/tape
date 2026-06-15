//! Per-bucket object listing index column family.

use store::Column;

use crate::types::{ObjectListEntry, ObjectListKey};

/// Per-bucket, name-ordered index for S3 `ListObjects`.
///
/// Key: `ObjectListKey` (`[bucket 32B][name]`), ordered lexicographically by name.
/// Value: `ObjectListEntry` (size, etag, time, and a pointer to the object track).
pub struct ObjectListCol;

impl Column for ObjectListCol {
    const CF_NAME: &'static str = "object_list";
    type Key = ObjectListKey;
    type Value = ObjectListEntry;
}
