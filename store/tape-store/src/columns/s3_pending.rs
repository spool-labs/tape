//! Queued S3 write column families.

use store::Column;

use crate::types::{PendingWrite, PendingWriteChunk, PendingWriteChunkKey, PendingWriteKey};

/// Queued S3 writes, keyed by tape and object key so one bucket scans together
pub struct S3PendingWriteCol;

impl Column for S3PendingWriteCol {
    const CF_NAME: &'static str = "s3_pending_write";
    type Key = PendingWriteKey;
    type Value = PendingWrite;
}

/// Queued object payloads, split into fixed-size chunks keyed by
/// `(tape, key, chunk_index)` so no stored value has to fit one segment.
pub struct S3PendingWriteDataCol;

impl Column for S3PendingWriteDataCol {
    const CF_NAME: &'static str = "s3_pending_write_data";
    type Key = PendingWriteChunkKey;
    type Value = PendingWriteChunk;
}
