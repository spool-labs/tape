//! Durable S3 multipart upload operations.

use store::{Column, Store, WriteBatch};
use tape_crypto::hash::{hash, Hash};

use crate::columns::{S3MultipartPartCol, S3MultipartPartDataCol, S3MultipartUploadCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{MultipartPart, MultipartPartData, MultipartPartKey, MultipartUpload};
use crate::TapeStore;

/// Digest of an opaque upload id, used as the fixed-width key prefix shared by
/// all of an upload's parts.
fn upload_digest(upload_id: &str) -> Hash {
    hash(upload_id.as_bytes())
}

/// The `(upload, part_number)` key for one part of an upload.
fn part_key(upload_id: &str, part_number: u32) -> MultipartPartKey {
    MultipartPartKey::new(upload_digest(upload_id), part_number)
}

/// Serialize a value to raw bytes for a write batch.
fn encode<Value>(value: &Value, what: &str) -> Result<Vec<u8>>
where
    Value: wincode::SchemaWrite<Src = Value>,
{
    wincode::serialize(value)
        .map_err(|error| TapeStoreError::Serialization(format!("{what}: {error}")))
}

/// Operations for the durable S3 multipart upload store
pub trait MultipartOps {
    /// Insert or overwrite the metadata for `upload_id`
    fn put_multipart_upload(&self, upload_id: &str, upload: &MultipartUpload) -> Result<()>;

    /// Fetch the metadata for `upload_id`, if present
    fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>>;

    /// Insert or overwrite one part of `upload_id` (re-upload overwrites). The
    /// metadata and payload are written together in one atomic batch.
    fn put_multipart_part(
        &self,
        upload_id: &str,
        part: &MultipartPart,
        data: Vec<u8>,
    ) -> Result<()>;

    /// Every buffered part's metadata for `upload_id`, in ascending part-number
    /// order, without reading any payload bytes
    fn list_multipart_parts(&self, upload_id: &str) -> Result<Vec<MultipartPart>>;

    /// The buffered payload of one part of `upload_id`, if present
    fn get_multipart_part_data(&self, upload_id: &str, part_number: u32) -> Result<Option<Vec<u8>>>;

    /// Every in-flight upload as `(upload_id, metadata)`, for ListMultipartUploads
    fn list_multipart_uploads(&self) -> Result<Vec<(String, MultipartUpload)>>;

    /// Delete `upload_id` and all of its buffered parts (Complete or Abort)
    fn delete_multipart_upload(&self, upload_id: &str) -> Result<()>;
}

impl<Backend: Store> MultipartOps for TapeStore<Backend> {
    fn put_multipart_upload(&self, upload_id: &str, upload: &MultipartUpload) -> Result<()> {
        self.put::<S3MultipartUploadCol>(&upload_id.to_string(), upload)?;
        Ok(())
    }

    fn get_multipart_upload(&self, upload_id: &str) -> Result<Option<MultipartUpload>> {
        Ok(self.get::<S3MultipartUploadCol>(&upload_id.to_string())?)
    }

    fn put_multipart_part(
        &self,
        upload_id: &str,
        part: &MultipartPart,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = encode(&part_key(upload_id, part.part_number), "multipart part key")?;
        let metadata = encode(part, "multipart part metadata")?;
        let payload = encode(&MultipartPartData { data }, "multipart part payload")?;

        let mut batch = WriteBatch::new();
        batch.put(S3MultipartPartCol::CF_NAME, &key, &metadata);
        batch.put(S3MultipartPartDataCol::CF_NAME, &key, &payload);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn list_multipart_parts(&self, upload_id: &str) -> Result<Vec<MultipartPart>> {
        let prefix = MultipartPartKey::upload_prefix(upload_digest(upload_id));

        // The 32-byte upload prefix scopes the scan to this upload alone, and the
        // part-number suffix orders the results, so no cross-upload data is read.
        let mut parts = Vec::new();
        for (_key, value) in self
            .inner()
            .inner()
            .iter_prefix(S3MultipartPartCol::CF_NAME, &prefix)?
        {
            let part: MultipartPart = wincode::deserialize(&value).map_err(|error| {
                TapeStoreError::Serialization(format!("multipart part metadata: {error}"))
            })?;
            parts.push(part);
        }
        Ok(parts)
    }

    fn get_multipart_part_data(&self, upload_id: &str, part_number: u32) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get::<S3MultipartPartDataCol>(&part_key(upload_id, part_number))?
            .map(|payload| payload.data))
    }

    fn list_multipart_uploads(&self) -> Result<Vec<(String, MultipartUpload)>> {
        Ok(self.iter::<S3MultipartUploadCol>()?)
    }

    fn delete_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let raw = self.inner().inner();
        let prefix = MultipartPartKey::upload_prefix(upload_digest(upload_id));

        let metadata_keys = raw.iter_keys_prefix(S3MultipartPartCol::CF_NAME, &prefix)?;
        let payload_keys = raw.iter_keys_prefix(S3MultipartPartDataCol::CF_NAME, &prefix)?;

        let mut batch = WriteBatch::new();
        for key in &metadata_keys {
            batch.delete(S3MultipartPartCol::CF_NAME, key);
        }
        for key in &payload_keys {
            batch.delete(S3MultipartPartDataCol::CF_NAME, key);
        }
        raw.write_batch(batch)?;

        self.delete::<S3MultipartUploadCol>(&upload_id.to_string())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::ContentType;
    use tape_crypto::address::Address;
    use tape_crypto::hash::hash;

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn upload() -> MultipartUpload {
        MultipartUpload {
            bucket: Address::new_unique(),
            key: "obj".to_string(),
            content_type: ContentType::TextPlain,
            initiated: 1_000,
            principal: Address::new_unique(),
        }
    }

    fn put_part(store: &TapeStore<MemoryStore>, upload_id: &str, part_number: u32, data: &[u8]) {
        let part = MultipartPart {
            part_number,
            etag: hash(data),
            last_modified: 2_000,
            size: data.len() as u64,
        };
        store
            .put_multipart_part(upload_id, &part, data.to_vec())
            .expect("put part");
    }

    // an upload's metadata reads back unchanged
    #[test]
    fn upload_round_trip() {
        let store = store();
        assert!(store.get_multipart_upload("u1").expect("get").is_none());

        let upload = upload();
        store.put_multipart_upload("u1", &upload).expect("put upload");
        assert_eq!(store.get_multipart_upload("u1").expect("get"), Some(upload));
    }

    // parts list back in ascending part-number order regardless of insert order
    #[test]
    fn parts_ordered() {
        let store = store();
        put_part(&store, "u1", 2, b"two");
        put_part(&store, "u1", 1, b"one");
        put_part(&store, "u1", 10, b"ten");

        let numbers: Vec<u32> = store
            .list_multipart_parts("u1")
            .expect("list parts")
            .into_iter()
            .map(|part| part.part_number)
            .collect();
        assert_eq!(numbers, vec![1, 2, 10]);
    }

    // a part scan is scoped to its own upload id
    #[test]
    fn parts_scoped() {
        let store = store();
        put_part(&store, "u1", 1, b"a");
        put_part(&store, "u2", 1, b"b");

        assert_eq!(store.list_multipart_parts("u1").expect("list").len(), 1);
        assert_eq!(store.list_multipart_parts("u2").expect("list").len(), 1);
    }

    // part metadata records its payload size, and the payload reads back separately
    #[test]
    fn part_size_and_payload() {
        let store = store();
        put_part(&store, "u1", 1, b"hello");

        let parts = store.list_multipart_parts("u1").expect("list");
        assert_eq!(parts[0].size, 5);
        assert_eq!(
            store.get_multipart_part_data("u1", 1).expect("data"),
            Some(b"hello".to_vec())
        );
        assert!(store.get_multipart_part_data("u1", 2).expect("data").is_none());
    }

    // re-uploading a part number overwrites it
    #[test]
    fn part_overwrite() {
        let store = store();
        put_part(&store, "u1", 1, b"old");
        put_part(&store, "u1", 1, b"new");

        let parts = store.list_multipart_parts("u1").expect("list parts");
        assert_eq!(parts.len(), 1);
        assert_eq!(
            store.get_multipart_part_data("u1", 1).expect("data"),
            Some(b"new".to_vec())
        );
    }

    // delete removes the upload and all of its parts, leaving others intact
    #[test]
    fn delete_clears_parts() {
        let store = store();
        store.put_multipart_upload("u1", &upload()).expect("put upload");
        put_part(&store, "u1", 1, b"a");
        put_part(&store, "u1", 2, b"b");
        store.put_multipart_upload("u2", &upload()).expect("put upload");
        put_part(&store, "u2", 1, b"c");

        store.delete_multipart_upload("u1").expect("delete");

        assert!(store.get_multipart_upload("u1").expect("get").is_none());
        assert!(store.list_multipart_parts("u1").expect("list").is_empty());
        assert!(store.get_multipart_part_data("u1", 1).expect("data").is_none());
        assert!(store.get_multipart_upload("u2").expect("get").is_some());
        assert_eq!(store.list_multipart_parts("u2").expect("list").len(), 1);
        assert!(store.get_multipart_part_data("u2", 1).expect("data").is_some());
    }

    // listing returns every in-flight upload
    #[test]
    fn list_uploads() {
        let store = store();
        store.put_multipart_upload("u1", &upload()).expect("put upload");
        store.put_multipart_upload("u2", &upload()).expect("put upload");

        let ids: Vec<String> = store
            .list_multipart_uploads()
            .expect("list uploads")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"u1".to_string()));
        assert!(ids.contains(&"u2".to_string()));
    }
}
