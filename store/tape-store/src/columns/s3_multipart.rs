//! S3 multipart upload column families.

use store::Column;

use crate::types::{
    MultipartPart, MultipartPartChunk, MultipartPartChunkKey, MultipartPartKey, MultipartUpload,
};

/// In-flight multipart uploads, keyed by opaque upload id.
pub struct S3MultipartUploadCol;

impl Column for S3MultipartUploadCol {
    const CF_NAME: &'static str = "s3_multipart_upload";
    type Key = String;
    type Value = MultipartUpload;
}

/// Buffered multipart part metadata, keyed by `(upload, part_number)` so an
/// upload's parts scan together without reading any payload bytes.
pub struct S3MultipartPartCol;

impl Column for S3MultipartPartCol {
    const CF_NAME: &'static str = "s3_multipart_part";
    type Key = MultipartPartKey;
    type Value = MultipartPart;
}

/// Buffered multipart part payloads, split into fixed-size chunks keyed by
/// `(upload, part_number, chunk_index)` so no stored value has to fit one segment.
pub struct S3MultipartPartDataCol;

impl Column for S3MultipartPartDataCol {
    const CF_NAME: &'static str = "s3_multipart_part_data";
    type Key = MultipartPartChunkKey;
    type Value = MultipartPartChunk;
}
