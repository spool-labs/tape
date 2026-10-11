//! Durable S3 multipart upload state machine

use std::collections::HashMap;
use std::io;
use std::mem::take;
use std::slice;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream;
use hex::encode;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tokio_util::io::StreamReader;

use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_crypto::hash::hashv;
use tape_crypto::Hash;
use tape_store::ops::MultipartOps;
use tape_store::types::{MULTIPART_CHUNK_BYTES, MultipartPart, MultipartUpload};

use super::clock::now_unix;
use super::error::S3Error;

/// Smallest valid S3 part number
pub const MIN_PART_NUMBER: u32 = 1;
/// Largest valid S3 part number
pub const MAX_PART_NUMBER: u32 = 10_000;
/// S3's minimum size for every multipart part except the last
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;

/// Monotonic counter mixed into minted upload ids for uniqueness
static UPLOAD_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One lock per part being uploaded, so two uploads of one part number never
/// interleave their chunks
static PART_LOCKS: LazyLock<Mutex<HashMap<(String, u32), Arc<AsyncMutex<()>>>>> =
    LazyLock::new(Default::default);

/// One part as named in a CompleteMultipartUpload request body
pub struct CompletedPartRef {
    /// Part number the client is asking to assemble
    pub part_number: u32,
    /// Normalized (unquoted, lowercase) ETag the client received from UploadPart
    pub etag: String,
}

/// A multipart upload checked for completion, its part bytes still in the store
pub struct ValidatedUpload {
    /// Object key (the on-chain track name)
    pub key: String,
    /// Content type to apply to the written object
    pub content_type: ContentType,
    /// The requested parts in part-number order
    pub parts: Vec<MultipartPart>,
    /// Object size in bytes, the sum of the parts
    pub size: u64,
}

/// A snapshot of one persisted part, for ListParts rendering
pub struct PartListing {
    /// Part number
    pub part_number: u32,
    /// Part ETag (hex of the placeholder content hash)
    pub etag: String,
    /// Part size in bytes
    pub size: u64,
    /// Upload time (unix seconds)
    pub last_modified: i64,
}

/// A snapshot of a multipart upload, for ListParts rendering
pub struct UploadListing {
    /// Object key
    pub key: String,
    /// Persisted parts in ascending part-number order
    pub parts: Vec<PartListing>,
}

/// Begin a multipart upload to `(bucket, key)` and return its opaque upload id
/// (CreateMultipartUpload)
pub fn create_upload(
    store: &impl MultipartOps,
    bucket: Address,
    key: String,
    content_type: ContentType,
    principal: Address,
) -> Result<String, S3Error> {
    let upload_id = mint_upload_id(&bucket, &key);
    let upload = MultipartUpload {
        bucket,
        key,
        content_type,
        initiated: now_unix(),
        principal,
    };
    store
        .put_multipart_upload(&upload_id, &upload)
        .map_err(store_error)?;
    Ok(upload_id)
}

/// Count the in-flight uploads owned by `principal`, for the per-principal
/// concurrent-multipart budget. 
pub fn count_open_uploads(
    store: &impl MultipartOps,
    principal: Address,
) -> Result<usize, S3Error> {
    let count = store
        .list_multipart_uploads()
        .map_err(store_error)?
        .into_iter()
        .filter(|(_, upload)| upload.principal == principal)
        .count();
    Ok(count)
}

/// Refuse a part number outside S3's range
pub fn check_part_number(part_number: u32) -> Result<(), S3Error> {
    if !(MIN_PART_NUMBER..=MAX_PART_NUMBER).contains(&part_number) {
        return Err(S3Error::InvalidRequest(format!(
            "partNumber must be between {MIN_PART_NUMBER} and {MAX_PART_NUMBER}"
        )));
    }
    Ok(())
}

/// A part whose chunks are in the store, waiting to be published or discarded
pub struct StagedPart {
    pub part_number: u32,
    /// SHA-256 of the part's bytes, which is its ETag
    pub etag: Hash,
    pub size: u64,
    /// Held until the part is published or discarded
    _guard: PartGuard,
}

/// Stream one part's bytes into the store under `upload_id` (UploadPart), one
/// `MULTIPART_CHUNK_BYTES` row at a time with a running SHA-256
///
/// Memory stays at one chunk whatever the part size. The upload's staged bytes
/// are capped at `max_object_bytes` as the bytes arrive, so a caller cannot
/// stage more than the largest object it could complete. The part reads as
/// absent until [`publish_part`].
pub async fn stage_part<Reader: AsyncRead + Unpin>(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    part_number: u32,
    body: Reader,
    max_object_bytes: u64,
    principal: Address,
) -> Result<StagedPart, S3Error> {
    check_part_number(part_number)?;
    load_upload(store, upload_id, bucket, key, principal)?;
    let guard = lock_part(upload_id, part_number).await;

    // A re-upload of this part number replaces it, so exclude it from the
    // already-staged total before admitting the new bytes.
    let mut staged: u64 = 0;
    for part in &store.list_multipart_parts(upload_id).map_err(store_error)? {
        if part.part_number != part_number {
            staged = staged.saturating_add(part.size);
        }
    }

    store.retire_multipart_part(upload_id, part_number).map_err(store_error)?;
    let written = write_chunks(store, upload_id, part_number, body, staged, max_object_bytes).await;
    match written {
        Ok((etag, size)) => Ok(StagedPart {
            part_number,
            etag,
            size,
            _guard: guard,
        }),
        Err(error) => {
            drop_part(store, upload_id, part_number);
            Err(error)
        }
    }
}

/// Write a part's body as chunks and return its SHA-256 and size, refusing
/// once `staged` plus the part passes `max_object_bytes`
async fn write_chunks<Reader: AsyncRead + Unpin>(
    store: &impl MultipartOps,
    upload_id: &str,
    part_number: u32,
    mut body: Reader,
    staged: u64,
    max_object_bytes: u64,
) -> Result<(Hash, u64), S3Error> {
    let mut digest = Sha256::new();
    let mut size: u64 = 0;
    let mut chunk_index: u32 = 0;
    loop {
        let mut chunk = Vec::with_capacity(MULTIPART_CHUNK_BYTES);
        while chunk.len() < MULTIPART_CHUNK_BYTES {
            let read = body
                .read_buf(&mut chunk)
                .await
                .map_err(|error| S3Error::InvalidRequest(format!("request body: {error}")))?;
            if read == 0 {
                break;
            }
        }
        let read = chunk.len();
        size = size.saturating_add(read as u64);
        let projected = staged.saturating_add(size);
        if projected > max_object_bytes {
            return Err(S3Error::EntityTooLarge(format!(
                "this upload's staged bytes ({projected}) would exceed the maximum object size of {max_object_bytes} bytes"
            )));
        }
        // An empty part still takes one chunk, so it reads back as present.
        if read == 0 && chunk_index > 0 {
            break;
        }
        digest.update(&chunk);
        store
            .put_multipart_part_chunk(upload_id, part_number, chunk_index, chunk)
            .map_err(store_error)?;
        chunk_index += 1;
        if read < MULTIPART_CHUNK_BYTES {
            break;
        }
    }
    let digest: [u8; 32] = digest.finalize().into();
    Ok((Hash(digest), size))
}

/// Publish a staged part once its body checked out, and return its ETag
///
/// `signed` is the body SHA-256 the request signed, if any. An upload aborted
/// while the part streamed takes the part with it.
pub fn publish_part(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    principal: Address,
    staged: StagedPart,
    signed: Option<[u8; 32]>,
) -> Result<Hash, S3Error> {
    if signed.is_some_and(|signed| Hash(signed) != staged.etag) {
        discard_part(store, upload_id, staged);
        return Err(S3Error::ContentSha256Mismatch);
    }
    if let Err(error) = load_upload(store, upload_id, bucket, key, principal) {
        discard_part(store, upload_id, staged);
        return Err(error);
    }
    let part = MultipartPart {
        part_number: staged.part_number,
        etag: staged.etag,
        last_modified: now_unix(),
        size: staged.size,
    };
    if let Err(error) = store.publish_multipart_part(upload_id, &part) {
        discard_part(store, upload_id, staged);
        return Err(store_error(error));
    }
    Ok(part.etag)
}

/// Drop a staged part's chunks, for a body that failed after it was read
pub fn discard_part(store: &impl MultipartOps, upload_id: &str, staged: StagedPart) {
    drop_part(store, upload_id, staged.part_number);
}

/// Delete one part's rows. A failure only leaks rows the upload's Complete or
/// Abort reclaims, so it is logged.
fn drop_part(store: &impl MultipartOps, upload_id: &str, part_number: u32) {
    if let Err(error) = store.delete_multipart_part(upload_id, part_number) {
        tracing::warn!(%error, part_number, "s3 UploadPart: failed to drop a part's rows");
    }
}

/// Holds one part's upload lock, and drops the lock's entry once nothing waits on it
struct PartGuard {
    key: (String, u32),
    guard: Option<OwnedMutexGuard<()>>,
}

impl Drop for PartGuard {
    fn drop(&mut self) {
        self.guard = None;
        let mut locks = PART_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
        if locks.get(&self.key).is_some_and(|lock| Arc::strong_count(lock) == 1) {
            locks.remove(&self.key);
        }
    }
}

/// Wait for and take the upload lock of one part
async fn lock_part(upload_id: &str, part_number: u32) -> PartGuard {
    let key = (upload_id.to_string(), part_number);
    let lock = {
        let mut locks = PART_LOCKS.lock().unwrap_or_else(PoisonError::into_inner);
        locks.entry(key.clone()).or_default().clone()
    };
    PartGuard {
        key,
        guard: Some(lock.lock_owned().await),
    }
}

/// Snapshot the persisted parts of `upload_id` for ListParts
pub fn list_parts(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    principal: Address,
) -> Result<UploadListing, S3Error> {
    let upload = load_upload(store, upload_id, bucket, key, principal)?;
    let stored = store.list_multipart_parts(upload_id).map_err(store_error)?;

    let mut parts = Vec::with_capacity(stored.len());
    for part in &stored {
        parts.push(PartListing {
            part_number: part.part_number,
            etag: encode(part.etag),
            size: part.size,
            last_modified: part.last_modified,
        });
    }
    Ok(UploadListing {
        key: upload.key,
        parts,
    })
}

/// Discard `upload_id` and its persisted parts (AbortMultipartUpload)
///
/// Only the principal that opened the upload may abort it, so a stranger holding
/// the upload id cannot destroy another tenant's in-flight upload.
pub fn abort(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    principal: Address,
) -> Result<(), S3Error> {
    load_upload(store, upload_id, bucket, key, principal)?;
    store.delete_multipart_upload(upload_id).map_err(store_error)
}

/// Validate the client's part list against the persisted parts (the read side
/// of CompleteMultipartUpload). No part bytes are read here.
pub fn validate(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    requested: &[CompletedPartRef],
    max_object_bytes: usize,
    principal: Address,
) -> Result<ValidatedUpload, S3Error> {
    if requested.is_empty() {
        return Err(S3Error::InvalidRequest(
            "CompleteMultipartUpload must list at least one part".into(),
        ));
    }

    let upload = load_upload(store, upload_id, bucket, key, principal)?;
    let stored = store.list_multipart_parts(upload_id).map_err(store_error)?;
    let by_number: HashMap<u32, &MultipartPart> =
        stored.iter().map(|part| (part.part_number, part)).collect();

    // Validate structure first (ascending order, existence, ETag), so a structural
    // error always wins over a size error below.
    let mut previous: Option<u32> = None;
    let mut ordered: Vec<MultipartPart> = Vec::with_capacity(requested.len());
    for part in requested {
        if let Some(previous) = previous {
            if part.part_number <= previous {
                return Err(S3Error::InvalidRequest(
                    "CompleteMultipartUpload parts must be in ascending order".into(),
                ));
            }
        }
        previous = Some(part.part_number);

        let stored_part = by_number.get(&part.part_number).ok_or_else(|| {
            S3Error::InvalidRequest(format!(
                "part {} was not uploaded for this upload id",
                part.part_number
            ))
        })?;
        if encode(stored_part.etag) != part.etag {
            return Err(S3Error::InvalidRequest(format!(
                "ETag mismatch for part {}",
                part.part_number
            )));
        }
        ordered.push(**stored_part);
    }

    // Every part except the last must meet S3's minimum part size, and the sum
    // must fit the object ceiling. Both come from metadata sizes alone.
    let last_index = ordered.len() - 1;
    let mut total: u64 = 0;
    for (index, stored_part) in ordered.iter().enumerate() {
        if index != last_index && stored_part.size < MIN_PART_SIZE as u64 {
            return Err(S3Error::EntityTooSmall(format!(
                "part {} is {} bytes; every part except the last must be at least {MIN_PART_SIZE} bytes",
                stored_part.part_number, stored_part.size
            )));
        }
        total = total.saturating_add(stored_part.size);
    }
    if total > max_object_bytes as u64 {
        return Err(S3Error::EntityTooLarge(format!(
            "assembled object size {total} exceeds the maximum of {max_object_bytes} bytes"
        )));
    }

    Ok(ValidatedUpload {
        key: upload.key,
        content_type: upload.content_type,
        parts: ordered,
        size: total,
    })
}

/// Read a validated upload's object into memory, for an object the queue buffers
pub fn read_object(
    store: &impl MultipartOps,
    upload_id: &str,
    upload: &ValidatedUpload,
) -> Result<Vec<u8>, S3Error> {
    let mut data = Vec::with_capacity(upload.size as usize);
    for chunk in PartChunks::new(store, upload_id, &upload.parts) {
        let chunk = chunk.map_err(|error| S3Error::Internal(error.to_string()))?;
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

/// Read a validated upload's object in part order, one stored chunk at a time
///
/// Memory stays at one `MULTIPART_CHUNK_BYTES` chunk whatever size the client
/// chose for its parts.
pub fn part_reader<'upload, Ops>(
    store: &'upload Ops,
    upload_id: &'upload str,
    parts: &'upload [MultipartPart],
) -> impl AsyncRead + Unpin + 'upload
where
    Ops: MultipartOps,
{
    StreamReader::new(stream::iter(PartChunks::new(store, upload_id, parts)))
}

/// The stored chunks of an upload's parts in order, each part checked against
/// the ETag the client completed it with
struct PartChunks<'upload, Ops> {
    store: &'upload Ops,
    upload_id: &'upload str,
    parts: slice::Iter<'upload, MultipartPart>,
    /// The part being read, if one is open
    cursor: Option<PartCursor>,
}

/// Read progress through one part
struct PartCursor {
    part: MultipartPart,
    /// The next chunk to read
    chunk_index: u32,
    /// Bytes of the part not read yet
    remaining: u64,
    /// SHA-256 of the bytes read so far
    digest: Sha256,
}

impl<'upload, Ops: MultipartOps> PartChunks<'upload, Ops> {
    fn new(store: &'upload Ops, upload_id: &'upload str, parts: &'upload [MultipartPart]) -> Self {
        Self {
            store,
            upload_id,
            parts: parts.iter(),
            cursor: None,
        }
    }

    /// The next chunk of the object, or `None` once every part is read
    fn next_chunk(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            let cursor = match &mut self.cursor {
                Some(cursor) => cursor,
                None => match self.parts.next() {
                    Some(part) => self.cursor.insert(PartCursor {
                        part: *part,
                        chunk_index: 0,
                        remaining: part.size,
                        digest: Sha256::new(),
                    }),
                    None => return Ok(None),
                },
            };
            let part_number = cursor.part.part_number;

            let mut chunk = None;
            if cursor.remaining > 0 {
                let expected = cursor.remaining.min(MULTIPART_CHUNK_BYTES as u64);
                let data = self
                    .store
                    .get_multipart_part_chunk(self.upload_id, part_number, cursor.chunk_index)
                    .map_err(|error| read_error(format!("multipart store: {error}")))?
                    .ok_or_else(|| {
                        read_error(format!(
                            "multipart part {part_number} chunk {} missing",
                            cursor.chunk_index
                        ))
                    })?;
                if data.len() as u64 != expected {
                    return Err(read_error(format!(
                        "multipart part {part_number} chunk {} is {} bytes, expected {expected}",
                        cursor.chunk_index,
                        data.len()
                    )));
                }
                cursor.digest.update(&data);
                cursor.remaining -= expected;
                cursor.chunk_index += 1;
                chunk = Some(Bytes::from(data));
            }

            // The part's last chunk goes out only once the part still matches
            // its ETag, so an UploadPart that replaced it mid-stream fails the write.
            if cursor.remaining == 0 {
                let digest: [u8; 32] = take(&mut cursor.digest).finalize().into();
                if Hash(digest) != cursor.part.etag {
                    return Err(read_error(format!(
                        "multipart part {part_number} changed after CompleteMultipartUpload began"
                    )));
                }
                self.cursor = None;
            }

            if let Some(chunk) = chunk {
                return Ok(Some(chunk));
            }
        }
    }
}

impl<Ops: MultipartOps> Iterator for PartChunks<'_, Ops> {
    type Item = io::Result<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(chunk) => chunk.map(Ok),
            Err(error) => {
                // A failed read ends the object, so nothing after it is read.
                self.parts = [].iter();
                self.cursor = None;
                Some(Err(error))
            }
        }
    }
}

/// An I/O error for a failed read of stored part bytes. The detail is logged
/// here, since the stream writer reports it only as an I/O failure.
fn read_error(detail: String) -> io::Error {
    tracing::warn!("{detail}");
    io::Error::other(detail)
}

/// Drop a completed upload and its parts (called after the assembled object is
/// written)
pub fn remove(store: &impl MultipartOps, upload_id: &str) -> Result<(), S3Error> {
    store.delete_multipart_upload(upload_id).map_err(store_error)
}

/// A snapshot of one in-flight upload, for ListMultipartUploads
pub struct UploadSummary {
    /// Opaque upload id
    pub upload_id: String,
    /// Object key the upload targets
    pub key: String,
    /// Initiation time (unix seconds)
    pub initiated: i64,
}

/// List `principal`'s in-flight uploads targeting `bucket`
/// (ListMultipartUploads), sorted by key then upload id as S3 reports them
///
/// Two principals can share a bucket, and this listing hands out the upload ids
/// every other multipart op keys on, so it is scoped to the caller.
pub fn list_uploads(
    store: &impl MultipartOps,
    bucket: Address,
    principal: Address,
) -> Result<Vec<UploadSummary>, S3Error> {
    let mut summaries = Vec::new();
    for (upload_id, upload) in store.list_multipart_uploads().map_err(store_error)? {
        if upload.bucket == bucket && upload.principal == principal {
            summaries.push(UploadSummary {
                upload_id,
                key: upload.key,
                initiated: upload.initiated,
            });
        }
    }
    summaries.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.upload_id.cmp(&right.upload_id))
    });
    Ok(summaries)
}

/// Load an upload by id, requiring its target `(bucket, key)` to match the
/// request path and `principal` to be the one that opened it.
///
/// An upload id is not a capability, so ownership is what separates two
/// principals sharing a bucket. Every multipart op settles it here.
fn load_upload(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    principal: Address,
) -> Result<MultipartUpload, S3Error> {
    let upload = store
        .get_multipart_upload(upload_id)
        .map_err(store_error)?
        .ok_or(S3Error::NoSuchUpload)?;
    if upload.bucket != bucket || upload.key != key {
        return Err(S3Error::NoSuchUpload);
    }
    if upload.principal != principal {
        // Indistinguishable from a wrong id, so a stranger learns nothing.
        return Err(S3Error::NoSuchUpload);
    }
    Ok(upload)
}

/// Mint an opaque, URL-safe upload id bound (by hash) to the target object.
fn mint_upload_id(bucket: &Address, key: &str) -> String {
    let sequence = UPLOAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or(0);
    let digest = hashv(&[
        &sequence.to_le_bytes(),
        &nanoseconds.to_le_bytes(),
        bucket.as_ref(),
        key.as_bytes(),
    ]);
    encode(digest)
}

/// Map a multipart store error to the S3 surface. The backend detail is logged
/// here (never sent to the client) so the cause survives even when the caller
/// swallows or rewrites the error before it renders a response.
fn store_error(error: impl std::fmt::Display) -> S3Error {
    let detail = format!("multipart store: {error}");
    tracing::warn!("{detail}");
    S3Error::Internal(detail)
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use tape_crypto::hash::hash;
    use tape_store_memory::MemoryStore;
    use tape_store::TapeStore;

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn bucket() -> Address {
        Address::new_unique()
    }

    fn principal() -> Address {
        Address::new_unique()
    }

    /// The principal the helpers below act as, so they own what they open.
    fn owner() -> Address {
        Address::new([7u8; 32])
    }

    fn completed(part_number: u32, etag: Hash) -> CompletedPartRef {
        CompletedPartRef {
            part_number,
            etag: encode(etag),
        }
    }

    // A part that meets the 5 MiB minimum, for non-final parts in assembly tests.
    fn big_part() -> Vec<u8> {
        vec![0xAB; MIN_PART_SIZE]
    }

    // Object ceiling for the assembly/staging tests; comfortably above the bytes
    // any test stages.
    const TEST_MAX_OBJECT: usize = 64 * 1024 * 1024;

    fn put_part(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
        part_number: u32,
        data: Vec<u8>,
    ) -> Result<Hash, S3Error> {
        put_part_as(store, upload_id, bucket, key, part_number, data, TEST_MAX_OBJECT, owner())
    }

    // stage a part from memory and publish it, as UploadPart does with a body
    fn put_part_as(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
        part_number: u32,
        data: Vec<u8>,
        max_object_bytes: usize,
        principal: Address,
    ) -> Result<Hash, S3Error> {
        block_on(async {
            let staged = stage_part(
                store,
                upload_id,
                bucket,
                key,
                part_number,
                data.as_slice(),
                max_object_bytes as u64,
                principal,
            )
            .await?;
            publish_part(store, upload_id, bucket, key, principal, staged, None)
        })
    }

    fn validate(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
        requested: &[CompletedPartRef],
    ) -> Result<ValidatedUpload, S3Error> {
        super::validate(store, upload_id, bucket, key, requested, TEST_MAX_OBJECT, owner())
    }

    // validate, then read the object whole
    fn assemble(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
        requested: &[CompletedPartRef],
    ) -> Result<Vec<u8>, S3Error> {
        let upload = validate(store, upload_id, bucket, key, requested)?;
        read_object(store, upload_id, &upload)
    }

    fn abort(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
    ) -> Result<(), S3Error> {
        super::abort(store, upload_id, bucket, key, owner())
    }

    fn list_parts(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
    ) -> Result<UploadListing, S3Error> {
        super::list_parts(store, upload_id, bucket, key, owner())
    }

    fn create(
        store: &TapeStore<MemoryStore>,
        bucket: Address,
        key: &str,
        content_type: ContentType,
    ) -> String {
        create_upload(store, bucket, key.into(), content_type, owner()).expect("create upload")
    }

    // create, upload parts, then assemble round-trips the bytes
    #[test]
    fn round_trip() {
        let store = store();
        let bucket = bucket();
        let upload_id = create(&store, bucket, "obj", ContentType::TextPlain);

        let head = big_part();
        let etag1 = put_part(&store, &upload_id, bucket, "obj", 1, head.clone()).expect("part 1");
        let etag2 =
            put_part(&store, &upload_id, bucket, "obj", 2, b"world".to_vec()).expect("part 2");

        let requested = [completed(1, etag1), completed(2, etag2)];
        let upload = validate(&store, &upload_id, bucket, "obj", &requested).expect("validate");
        assert_eq!(upload.key, "obj");
        assert_eq!(upload.size, (MIN_PART_SIZE + 5) as u64);
        let assembled = assemble(&store, &upload_id, bucket, "obj", &requested).expect("assemble");
        let mut expected = head;
        expected.extend_from_slice(b"world");
        assert_eq!(assembled, expected);

        // Assemble retains the upload until the caller removes it.
        remove(&store, &upload_id).expect("remove");
        assert!(matches!(
            list_parts(&store, &upload_id, bucket, "obj"),
            Err(S3Error::NoSuchUpload)
        ));
    }

    // a part longer than one store chunk round-trips through assembly
    #[test]
    fn chunked_part() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, owner()).expect("create");
        let mut part = Vec::with_capacity(MULTIPART_CHUNK_BYTES + 7);
        for index in 0..MULTIPART_CHUNK_BYTES + 7 {
            part.push((index % 251) as u8);
        }

        let etag = put_part(&store, &upload_id, bucket, "obj", 1, part.clone()).expect("p1");

        let assembled = assemble(&store, &upload_id, bucket, "obj", &[completed(1, etag)])
            .expect("assemble");
        assert_eq!(assembled, part);
    }

    // abort drops every chunk of the upload's parts
    #[test]
    fn abort_clears_chunks() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, owner()).expect("create");
        put_part(&store, &upload_id, bucket, "obj", 1, vec![7; MULTIPART_CHUNK_BYTES + 7])
            .expect("p1");

        super::abort(&store, &upload_id, bucket, "obj", owner()).expect("abort");

        assert!(store.get_multipart_part_data(&upload_id, 1).expect("data").is_none());
        assert!(matches!(
            list_parts(&store, &upload_id, bucket, "obj"),
            Err(S3Error::NoSuchUpload)
        ));
    }

    // a part past the object ceiling is EntityTooLarge, not an internal error
    #[test]
    fn part_too_large() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, owner()).expect("create");

        assert!(matches!(
            put_part_as(&store, &upload_id, bucket, "obj", 1, vec![0; 9], 8, owner()),
            Err(S3Error::EntityTooLarge(_))
        ));
        assert!(store.get_multipart_part_data(&upload_id, 1).expect("data").is_none());
    }

    // a non-final part below the 5 MiB minimum is rejected
    #[test]
    fn part_too_small() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        let etag1 = put_part(&store, &upload_id, bucket, "obj", 1, b"small".to_vec()).expect("p1");
        let etag2 = put_part(&store, &upload_id, bucket, "obj", 2, b"tail".to_vec()).expect("p2");
        assert!(matches!(
            assemble(
                &store,
                &upload_id,
                bucket,
                "obj",
                &[completed(1, etag1), completed(2, etag2)]
            ),
            Err(S3Error::EntityTooSmall(_))
        ));
    }

    // a single part below the minimum is allowed (the last part is exempt)
    #[test]
    fn single_small_part() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        let etag = put_part(&store, &upload_id, bucket, "obj", 1, b"tiny".to_vec()).expect("p1");

        let assembled = assemble(&store, &upload_id, bucket, "obj", &[completed(1, etag)])
            .expect("single small part is allowed");
        assert_eq!(assembled, b"tiny");
    }

    // an unknown upload id is NoSuchUpload
    #[test]
    fn unknown_id() {
        let store = store();
        let bucket = bucket();
        assert!(matches!(
            put_part(&store, "missing", bucket, "obj", 1, b"x".to_vec()),
            Err(S3Error::NoSuchUpload)
        ));
        assert!(matches!(
            abort(&store, "missing", bucket, "obj"),
            Err(S3Error::NoSuchUpload)
        ));
    }

    // a mismatched key for an upload id is NoSuchUpload
    #[test]
    fn wrong_key() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        assert!(matches!(
            put_part(&store, &upload_id, bucket, "other", 1, b"x".to_vec()),
            Err(S3Error::NoSuchUpload)
        ));
    }

    // an out-of-range part number is rejected
    #[test]
    fn part_range() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        assert!(matches!(
            put_part(&store, &upload_id, bucket, "obj", 0, b"x".to_vec()),
            Err(S3Error::InvalidRequest(_))
        ));
        assert!(matches!(
            put_part(&store, &upload_id, bucket, "obj", MAX_PART_NUMBER + 1, b"x".to_vec()),
            Err(S3Error::InvalidRequest(_))
        ));
    }

    // completion rejects parts not in ascending order
    #[test]
    fn unordered_parts() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        let etag1 = put_part(&store, &upload_id, bucket, "obj", 1, b"a".to_vec()).expect("p1");
        let etag2 = put_part(&store, &upload_id, bucket, "obj", 2, b"b".to_vec()).expect("p2");
        assert!(matches!(
            assemble(
                &store,
                &upload_id,
                bucket,
                "obj",
                &[completed(2, etag2), completed(1, etag1)]
            ),
            Err(S3Error::InvalidRequest(_))
        ));
    }

    // completion rejects a part whose ETag does not match
    #[test]
    fn etag_mismatch() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        put_part(&store, &upload_id, bucket, "obj", 1, b"a".to_vec()).expect("p1");
        assert!(matches!(
            assemble(
                &store,
                &upload_id,
                bucket,
                "obj",
                &[completed(1, hash(b"different"))]
            ),
            Err(S3Error::InvalidRequest(_))
        ));
    }

    // completion rejects a part that was never uploaded
    #[test]
    fn missing_part() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        let etag1 = put_part(&store, &upload_id, bucket, "obj", 1, big_part()).expect("p1");
        assert!(matches!(
            assemble(
                &store,
                &upload_id,
                bucket,
                "obj",
                &[completed(1, etag1), completed(2, hash(b"b"))]
            ),
            Err(S3Error::InvalidRequest(_))
        ));
    }

    // ListParts reports persisted parts in part-number order
    #[test]
    fn list_order() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create(&store, bucket, "obj", ContentType::Unknown);
        put_part(&store, &upload_id, bucket, "obj", 2, b"bb".to_vec()).expect("p2");
        put_part(&store, &upload_id, bucket, "obj", 1, b"a".to_vec()).expect("p1");
        let listing = list_parts(&store, &upload_id, bucket, "obj").expect("list");
        let numbers: Vec<u32> = listing.parts.iter().map(|part| part.part_number).collect();
        assert_eq!(numbers, vec![1, 2]);
        assert_eq!(listing.parts[0].size, 1);
        assert_eq!(listing.parts[1].size, 2);
    }

    // concurrent creates mint distinct upload ids
    #[test]
    fn unique_ids() {
        let store = store();
        let bucket = bucket();
        let a = create(&store, bucket, "obj", ContentType::Unknown);
        let b = create(&store, bucket, "obj", ContentType::Unknown);
        assert_ne!(a, b);
    }

    // a second principal holding an upload id can neither read, extend,
    // complete nor destroy that upload
    #[test]
    fn stranger_is_refused() {
        let store = store();
        let bucket = bucket();
        let stranger = principal();
        let upload_id = create(&store, bucket, "obj", ContentType::Unknown);
        let etag = put_part(&store, &upload_id, bucket, "obj", 1, b"mine".to_vec()).expect("p1");

        assert!(matches!(
            put_part_as(
                &store,
                &upload_id,
                bucket,
                "obj",
                2,
                b"theirs".to_vec(),
                TEST_MAX_OBJECT,
                stranger,
            ),
            Err(S3Error::NoSuchUpload)
        ));
        assert!(matches!(
            super::list_parts(&store, &upload_id, bucket, "obj", stranger),
            Err(S3Error::NoSuchUpload)
        ));
        assert!(matches!(
            super::validate(
                &store,
                &upload_id,
                bucket,
                "obj",
                &[completed(1, etag)],
                TEST_MAX_OBJECT,
                stranger,
            ),
            Err(S3Error::NoSuchUpload)
        ));
        assert!(matches!(
            super::abort(&store, &upload_id, bucket, "obj", stranger),
            Err(S3Error::NoSuchUpload)
        ));

        // and none of that disturbed the upload
        let listing = list_parts(&store, &upload_id, bucket, "obj").expect("list");
        assert_eq!(listing.parts.len(), 1);
    }

    // the bucket listing shows a principal only its own uploads
    #[test]
    fn listing_scoped() {
        let store = store();
        let bucket = bucket();
        let stranger = principal();
        create(&store, bucket, "mine", ContentType::Unknown);
        create_upload(&store, bucket, "theirs".into(), ContentType::Unknown, stranger)
            .expect("create");

        let mine = list_uploads(&store, bucket, owner()).expect("list");
        let keys: Vec<&str> = mine.iter().map(|upload| upload.key.as_str()).collect();
        assert_eq!(keys, vec!["mine"]);

        let theirs = list_uploads(&store, bucket, stranger).expect("list");
        let keys: Vec<&str> = theirs.iter().map(|upload| upload.key.as_str()).collect();
        assert_eq!(keys, vec!["theirs"]);
    }

    // the open-upload count is per-principal and drops when a record is removed
    #[test]
    fn open_upload_count() {
        let store = store();
        let alice = principal();
        let bob = principal();
        let alice_first =
            create_upload(&store, bucket(), "a1".into(), ContentType::Unknown, alice).expect("create");
        create_upload(&store, bucket(), "a2".into(), ContentType::Unknown, alice).expect("create");
        create_upload(&store, bucket(), "b1".into(), ContentType::Unknown, bob).expect("create");

        assert_eq!(count_open_uploads(&store, alice).expect("count"), 2);
        assert_eq!(count_open_uploads(&store, bob).expect("count"), 1);

        // Complete/Abort delete the record, so the count self-corrects.
        remove(&store, &alice_first).expect("remove");
        assert_eq!(count_open_uploads(&store, alice).expect("count"), 1);
        assert_eq!(count_open_uploads(&store, bob).expect("count"), 1);
    }

    // Bytes that vary with position and seed, so a dropped or reordered chunk shows up.
    fn pattern(size: usize, seed: u8) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        for index in 0..size {
            data.push((index % 251) as u8 ^ seed);
        }
        data
    }

    // Three parts that end on a chunk boundary, past one, and empty, uploaded
    // and validated. Returns the upload id, the validated upload and the object.
    fn staged_upload(store: &TapeStore<MemoryStore>, bucket: Address) -> (String, ValidatedUpload, Vec<u8>) {
        let upload_id = create(store, bucket, "obj", ContentType::Unknown);
        let parts = [
            pattern(MULTIPART_CHUNK_BYTES * 2, 1),
            pattern(MULTIPART_CHUNK_BYTES + 7, 2),
            Vec::new(),
        ];
        let mut requested = Vec::new();
        let mut object = Vec::new();
        for (index, part) in parts.into_iter().enumerate() {
            object.extend_from_slice(&part);
            let part_number = index as u32 + 1;
            let etag = put_part(store, &upload_id, bucket, "obj", part_number, part).expect("part");
            requested.push(completed(part_number, etag));
        }
        let upload = validate(store, &upload_id, bucket, "obj", &requested).expect("validate");
        (upload_id, upload, object)
    }

    // the streamed reader returns the parts byte for byte, one stored chunk at a time
    #[tokio::test]
    async fn streamed_parts() {
        let store = store();
        let bucket = bucket();
        let (upload_id, upload, object) = staged_upload(&store, bucket);
        assert_eq!(upload.size, object.len() as u64);

        let mut streamed = Vec::new();
        part_reader(&store, &upload_id, &upload.parts)
            .read_to_end(&mut streamed)
            .await
            .expect("stream parts");
        assert_eq!(streamed, object);

        let sizes: Vec<usize> = PartChunks::new(&store, upload_id.as_str(), &upload.parts)
            .map(|chunk| chunk.expect("chunk").len())
            .collect();
        assert_eq!(
            sizes,
            vec![MULTIPART_CHUNK_BYTES, MULTIPART_CHUNK_BYTES, MULTIPART_CHUNK_BYTES, 7]
        );

        assert_eq!(read_object(&store, &upload_id, &upload).expect("read"), object);
    }

    // a part replaced after validation fails the read before the object ends,
    // and the upload stays for a retry
    #[tokio::test]
    async fn replaced_part() {
        let store = store();
        let bucket = bucket();
        let (upload_id, upload, _) = staged_upload(&store, bucket);
        put_part(&store, &upload_id, bucket, "obj", 2, pattern(MULTIPART_CHUNK_BYTES + 7, 9))
            .expect("replace part 2");

        let mut streamed = Vec::new();
        let read = part_reader(&store, &upload_id, &upload.parts)
            .read_to_end(&mut streamed)
            .await;
        assert!(read.is_err());
        // part 1 and the first chunk of part 2 went out, and the short last chunk was held back
        assert_eq!(streamed.len(), MULTIPART_CHUNK_BYTES * 3);
        assert!(matches!(
            read_object(&store, &upload_id, &upload),
            Err(S3Error::Internal(_))
        ));

        let listing = list_parts(&store, &upload_id, bucket, "obj").expect("list");
        assert_eq!(listing.parts.len(), 3);
    }

    // an upload aborted after validation fails the read at the first missing chunk
    #[tokio::test]
    async fn aborted_upload() {
        let store = store();
        let bucket = bucket();
        let (upload_id, upload, _) = staged_upload(&store, bucket);
        abort(&store, &upload_id, bucket, "obj").expect("abort");

        let mut streamed = Vec::new();
        let read = part_reader(&store, &upload_id, &upload.parts)
            .read_to_end(&mut streamed)
            .await;
        assert!(read.is_err());
        assert!(streamed.is_empty());
    }

    // number of stored chunk rows of one part
    fn part_chunks(store: &TapeStore<MemoryStore>, upload_id: &str, part_number: u32) -> u32 {
        let mut count = 0;
        while store
            .get_multipart_part_chunk(upload_id, part_number, count)
            .expect("chunk")
            .is_some()
        {
            count += 1;
        }
        count
    }

    // a streamed part lands one row per chunk with its SHA-256 as the ETag, and
    // reads as absent until it is published
    #[tokio::test]
    async fn staged_rows() {
        let store = store();
        let bucket = bucket();
        let upload_id = create(&store, bucket, "obj", ContentType::Unknown);
        let data = pattern(MULTIPART_CHUNK_BYTES * 2 + 7, 3);

        let staged = stage_part(
            &store, &upload_id, bucket, "obj", 1, data.as_slice(), TEST_MAX_OBJECT as u64, owner(),
        )
        .await
        .expect("stage");
        assert_eq!(staged.etag, hash(&data));
        assert_eq!(staged.size, data.len() as u64);
        assert_eq!(part_chunks(&store, &upload_id, 1), 3);
        assert!(list_parts(&store, &upload_id, bucket, "obj").expect("list").parts.is_empty());

        let etag = publish_part(&store, &upload_id, bucket, "obj", owner(), staged, Some(hash(&data).0))
            .expect("publish");
        assert_eq!(etag, hash(&data));
        let listing = list_parts(&store, &upload_id, bucket, "obj").expect("list");
        assert_eq!(listing.parts.len(), 1);
        assert_eq!(listing.parts[0].size, data.len() as u64);
        assert_eq!(store.get_multipart_part_data(&upload_id, 1).expect("data"), Some(data));
    }

    // the staged-bytes cap stops a part as its bytes arrive, and its rows go
    #[tokio::test]
    async fn staged_cap() {
        let store = store();
        let bucket = bucket();
        let upload_id = create(&store, bucket, "obj", ContentType::Unknown);
        put_part_as(&store, &upload_id, bucket, "obj", 1, vec![1; 5], 16, owner()).expect("p1");

        let data = vec![2u8; 12];
        let staged = stage_part(&store, &upload_id, bucket, "obj", 2, data.as_slice(), 16, owner()).await;
        assert!(matches!(staged, Err(S3Error::EntityTooLarge(_))));
        assert_eq!(part_chunks(&store, &upload_id, 2), 0);
        assert_eq!(list_parts(&store, &upload_id, bucket, "obj").expect("list").parts.len(), 1);
    }

    // a body missing its signed hash, or an upload aborted mid-part, drops the part
    #[tokio::test]
    async fn staged_refused() {
        let store = store();
        let bucket = bucket();
        let upload_id = create(&store, bucket, "obj", ContentType::Unknown);
        let data = vec![4u8; 1000];

        let staged = stage_part(
            &store, &upload_id, bucket, "obj", 1, data.as_slice(), TEST_MAX_OBJECT as u64, owner(),
        )
        .await
        .expect("stage");
        let published = publish_part(&store, &upload_id, bucket, "obj", owner(), staged, Some([0; 32]));
        assert!(matches!(published, Err(S3Error::ContentSha256Mismatch)));
        assert_eq!(part_chunks(&store, &upload_id, 1), 0);

        let staged = stage_part(
            &store, &upload_id, bucket, "obj", 1, data.as_slice(), TEST_MAX_OBJECT as u64, owner(),
        )
        .await
        .expect("stage");
        abort(&store, &upload_id, bucket, "obj").expect("abort");
        let published = publish_part(&store, &upload_id, bucket, "obj", owner(), staged, None);
        assert!(matches!(published, Err(S3Error::NoSuchUpload)));
        assert_eq!(part_chunks(&store, &upload_id, 1), 0);
    }

    // a second upload of one part number waits until the first is published
    #[tokio::test]
    async fn same_part_waits() {
        let store = store();
        let bucket = bucket();
        let upload_id = create(&store, bucket, "obj", ContentType::Unknown);
        let first = vec![1u8; 100];
        let second = vec![2u8; 50];

        let staged = stage_part(
            &store, &upload_id, bucket, "obj", 1, first.as_slice(), TEST_MAX_OBJECT as u64, owner(),
        )
        .await
        .expect("stage first");
        let waiting = stage_part(
            &store, &upload_id, bucket, "obj", 1, second.as_slice(), TEST_MAX_OBJECT as u64, owner(),
        );
        tokio::pin!(waiting);
        assert!(futures::poll!(&mut waiting).is_pending());

        publish_part(&store, &upload_id, bucket, "obj", owner(), staged, None).expect("publish first");
        let staged = waiting.await.expect("stage second");
        publish_part(&store, &upload_id, bucket, "obj", owner(), staged, None).expect("publish second");
        assert_eq!(store.get_multipart_part_data(&upload_id, 1).expect("data"), Some(second.clone()));
        assert!(PART_LOCKS.lock().expect("locks").get(&(upload_id.clone(), 1)).is_none());
    }
}
