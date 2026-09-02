//! Queued S3 write column families.

use store::Column;

use crate::types::{PendingWrite, PendingWriteData, PendingWriteKey};

/// Queued S3 writes, keyed by `(tape, object key)` so one bucket's queue scans
/// together in object-key order.
pub struct S3PendingWriteCol;

impl Column for S3PendingWriteCol {
    const CF_NAME: &'static str = "s3_pending_write";
    type Key = PendingWriteKey;
    type Value = PendingWrite;
}

/// Queued object payloads, keyed identically to their queue entries.
pub struct S3PendingWriteDataCol;

impl Column for S3PendingWriteDataCol {
    const CF_NAME: &'static str = "s3_pending_write_data";
    type Key = PendingWriteKey;
    type Value = PendingWriteData;
}
