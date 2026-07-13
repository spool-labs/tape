//! Durable S3 multipart upload state machine

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use hex::encode;

use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_crypto::hash::{hash, hashv};
use tape_crypto::Hash;
use tape_store::ops::MultipartOps;
use tape_store::types::{MultipartPart, MultipartUpload};

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

/// One part as named in a CompleteMultipartUpload request body
pub struct CompletedPartRef {
    /// Part number the client is asking to assemble
    pub part_number: u32,
    /// Normalized (unquoted, lowercase) ETag the client received from UploadPart
    pub etag: String,
}

/// A multipart upload assembled and ready to write
pub struct AssembledUpload {
    /// Object key (the on-chain track name)
    pub key: String,
    /// Content type to apply to the written object
    pub content_type: ContentType,
    /// Concatenated part bytes in part-number order
    pub data: Vec<u8>,
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

/// Persist one part's bytes under `upload_id` (UploadPart) and return its ETag.
///
/// The upload's total staged bytes are capped at `max_object_bytes`: buffered
/// parts never exceed the largest object that could be assembled from them, so a
/// caller cannot stage unbounded data ahead of completion.
pub fn put_part(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    part_number: u32,
    data: Vec<u8>,
    max_object_bytes: usize,
) -> Result<Hash, S3Error> {
    if !(MIN_PART_NUMBER..=MAX_PART_NUMBER).contains(&part_number) {
        return Err(S3Error::InvalidRequest(format!(
            "partNumber must be between {MIN_PART_NUMBER} and {MAX_PART_NUMBER}"
        )));
    }

    load_upload(store, upload_id, bucket, key)?;

    // A re-upload of this part number replaces it, so exclude it from the
    // already-staged total before admitting the new bytes.
    let mut staged: u64 = 0;
    for part in &store.list_multipart_parts(upload_id).map_err(store_error)? {
        if part.part_number != part_number {
            staged = staged.saturating_add(part.size);
        }
    }
    let projected = staged.saturating_add(data.len() as u64);
    if projected > max_object_bytes as u64 {
        return Err(S3Error::EntityTooLarge(format!(
            "this upload's staged bytes ({projected}) would exceed the maximum object size of {max_object_bytes} bytes"
        )));
    }

    let etag = hash(&data);
    let part = MultipartPart {
        part_number,
        etag,
        last_modified: now_unix(),
        size: data.len() as u64,
    };
    store.put_multipart_part(upload_id, &part, data).map_err(store_error)?;
    Ok(etag)
}

/// Snapshot the persisted parts of `upload_id` for ListParts
pub fn list_parts(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
) -> Result<UploadListing, S3Error> {
    let upload = load_upload(store, upload_id, bucket, key)?;
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
    let upload = load_upload(store, upload_id, bucket, key)?;
    if upload.principal != principal {
        return Err(S3Error::AccessDenied(
            "this multipart upload belongs to another principal".to_string(),
        ));
    }
    store.delete_multipart_upload(upload_id).map_err(store_error)
}

/// Validate the client's part list against the persisted parts and assemble the
/// object bytes (the read side of CompleteMultipartUpload).
pub fn assemble(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
    requested: &[CompletedPartRef],
    max_object_bytes: usize,
) -> Result<AssembledUpload, S3Error> {
    if requested.is_empty() {
        return Err(S3Error::InvalidRequest(
            "CompleteMultipartUpload must list at least one part".into(),
        ));
    }

    let upload = load_upload(store, upload_id, bucket, key)?;
    let stored = store.list_multipart_parts(upload_id).map_err(store_error)?;
    let by_number: HashMap<u32, &MultipartPart> =
        stored.iter().map(|part| (part.part_number, part)).collect();

    // Validate structure first (ascending order, existence, ETag), so a structural
    // error always wins over a size error below.
    let mut previous: Option<u32> = None;
    let mut ordered: Vec<&MultipartPart> = Vec::with_capacity(requested.len());
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
        ordered.push(stored_part);
    }

    // Every part except the last must meet S3's minimum part size. Validate from
    // metadata sizes (no payloads read yet) so the object ceiling is enforced
    // before any part bytes are loaded into memory.
    let last_index = ordered.len() - 1;
    let mut total: usize = 0;
    for (index, stored_part) in ordered.iter().enumerate() {
        let part_size = stored_part.size as usize;
        if index != last_index && part_size < MIN_PART_SIZE {
            return Err(S3Error::EntityTooSmall(format!(
                "part {} is {} bytes; every part except the last must be at least {MIN_PART_SIZE} bytes",
                stored_part.part_number, part_size
            )));
        }
        total = total.saturating_add(part_size);
    }
    if total > max_object_bytes {
        return Err(S3Error::EntityTooLarge(format!(
            "assembled object size {total} exceeds the maximum of {max_object_bytes} bytes"
        )));
    }

    let mut data = Vec::with_capacity(total);
    for stored_part in &ordered {
        let payload = store
            .get_multipart_part_data(upload_id, stored_part.part_number)
            .map_err(store_error)?
            .ok_or_else(|| {
                S3Error::Internal(format!(
                    "multipart part {} payload missing",
                    stored_part.part_number
                ))
            })?;
        data.extend_from_slice(&payload);
    }

    Ok(AssembledUpload {
        key: upload.key,
        content_type: upload.content_type,
        data,
    })
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

/// List the in-flight uploads targeting `bucket` (ListMultipartUploads), sorted
/// by key then upload id as S3 reports them
pub fn list_uploads(
    store: &impl MultipartOps,
    bucket: Address,
) -> Result<Vec<UploadSummary>, S3Error> {
    let mut summaries = Vec::new();
    for (upload_id, upload) in store.list_multipart_uploads().map_err(store_error)? {
        if upload.bucket == bucket {
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

/// Load an upload by id, requiring its target `(bucket, key)` to match the request
/// path.
fn load_upload(
    store: &impl MultipartOps,
    upload_id: &str,
    bucket: Address,
    key: &str,
) -> Result<MultipartUpload, S3Error> {
    let upload = store
        .get_multipart_upload(upload_id)
        .map_err(store_error)?
        .ok_or(S3Error::NoSuchUpload)?;
    if upload.bucket != bucket || upload.key != key {
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
/// (never sent to the client) via S3Error::Internal's response rendering.
fn store_error(error: impl std::fmt::Display) -> S3Error {
    S3Error::Internal(format!("multipart store: {error}"))
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
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
        super::put_part(store, upload_id, bucket, key, part_number, data, TEST_MAX_OBJECT)
    }

    fn assemble(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
        requested: &[CompletedPartRef],
    ) -> Result<AssembledUpload, S3Error> {
        super::assemble(store, upload_id, bucket, key, requested, TEST_MAX_OBJECT)
    }

    fn abort(
        store: &TapeStore<MemoryStore>,
        upload_id: &str,
        bucket: Address,
        key: &str,
    ) -> Result<(), S3Error> {
        super::abort(store, upload_id, bucket, key, Address::new_unique())
    }

    // create, upload parts, then assemble round-trips the bytes
    #[test]
    fn round_trip() {
        let store = store();
        let bucket = bucket();
        let upload_id = create_upload(&store, bucket, "obj".into(), ContentType::TextPlain, principal())
            .expect("create upload");

        let head = big_part();
        let etag1 = put_part(&store, &upload_id, bucket, "obj", 1, head.clone()).expect("part 1");
        let etag2 =
            put_part(&store, &upload_id, bucket, "obj", 2, b"world".to_vec()).expect("part 2");

        let assembled = assemble(
            &store,
            &upload_id,
            bucket,
            "obj",
            &[completed(1, etag1), completed(2, etag2)],
        )
        .expect("assemble");
        let mut expected = head;
        expected.extend_from_slice(b"world");
        assert_eq!(assembled.data, expected);
        assert_eq!(assembled.key, "obj");

        // Assemble retains the upload until the caller removes it.
        remove(&store, &upload_id).expect("remove");
        assert!(matches!(
            list_parts(&store, &upload_id, bucket, "obj"),
            Err(S3Error::NoSuchUpload)
        ));
    }

    // a non-final part below the 5 MiB minimum is rejected
    #[test]
    fn part_too_small() {
        let store = store();
        let bucket = bucket();
        let upload_id =
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
        let etag = put_part(&store, &upload_id, bucket, "obj", 1, b"tiny".to_vec()).expect("p1");

        let assembled = assemble(&store, &upload_id, bucket, "obj", &[completed(1, etag)])
            .expect("single small part is allowed");
        assert_eq!(assembled.data, b"tiny");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
            create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
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
        let a = create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
        let b = create_upload(&store, bucket, "obj".into(), ContentType::Unknown, principal()).expect("create");
        assert_ne!(a, b);
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
}
