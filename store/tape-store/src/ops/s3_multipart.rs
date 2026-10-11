//! Durable S3 multipart upload operations.

use tape_store_api::{Column, Store, WriteBatch};
use tape_crypto::hash::{hash, Hash};

use crate::columns::{S3MultipartPartCol, S3MultipartPartDataCol, S3MultipartUploadCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{
    MultipartPart, MultipartPartChunk, MultipartPartChunkKey, MultipartPartKey, MultipartUpload,
    MULTIPART_CHUNK_BYTES,
};
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

/// Chunks a part of `size` bytes occupies. An empty part still takes one chunk,
/// so a stored zero-byte part reads back as present rather than missing.
fn chunk_count(size: usize) -> u32 {
    let count = size.div_ceil(MULTIPART_CHUNK_BYTES).max(1);
    count as u32
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

    /// Insert or overwrite one whole part of `upload_id`, through the same
    /// retire, chunk and publish steps a streamed part takes
    fn put_multipart_part(
        &self,
        upload_id: &str,
        part: &MultipartPart,
        data: Vec<u8>,
    ) -> Result<()>;

    /// Drop the metadata row of one part of `upload_id` ahead of a rewrite, so
    /// the part reads as absent until the new one is published
    fn retire_multipart_part(&self, upload_id: &str, part_number: u32) -> Result<()>;

    /// Write chunk `chunk_index` of a part being uploaded. Chunks hold
    /// `MULTIPART_CHUNK_BYTES` each, and only the last one is short.
    fn put_multipart_part_chunk(
        &self,
        upload_id: &str,
        part_number: u32,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<()>;

    /// Make a part whose chunks are written readable: drop any chunk past
    /// `part.size` left by an earlier, longer upload, and write the metadata row
    fn publish_multipart_part(&self, upload_id: &str, part: &MultipartPart) -> Result<()>;

    /// Delete one part of `upload_id`: its metadata row and every chunk
    fn delete_multipart_part(&self, upload_id: &str, part_number: u32) -> Result<()>;

    /// Every buffered part's metadata for `upload_id`, in ascending part-number
    /// order, without reading any payload bytes
    fn list_multipart_parts(&self, upload_id: &str) -> Result<Vec<MultipartPart>>;

    /// The buffered payload of one part of `upload_id`, rejoined from its chunks,
    /// if present
    fn get_multipart_part_data(&self, upload_id: &str, part_number: u32) -> Result<Option<Vec<u8>>>;

    /// One stored chunk of one part of `upload_id`, if present. A part of `size`
    /// bytes fills chunks `0..size.div_ceil(MULTIPART_CHUNK_BYTES)`, and only the
    /// last one is short.
    fn get_multipart_part_chunk(
        &self,
        upload_id: &str,
        part_number: u32,
        chunk_index: u32,
    ) -> Result<Option<Vec<u8>>>;

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
        self.retire_multipart_part(upload_id, part.part_number)?;
        for index in 0..chunk_count(data.len()) {
            let offset = index as usize * MULTIPART_CHUNK_BYTES;
            let end = data.len().min(offset + MULTIPART_CHUNK_BYTES);
            self.put_multipart_part_chunk(upload_id, part.part_number, index, data[offset..end].to_vec())?;
        }
        self.publish_multipart_part(upload_id, part)
    }

    fn retire_multipart_part(&self, upload_id: &str, part_number: u32) -> Result<()> {
        let key = encode(&part_key(upload_id, part_number), "multipart part key")?;
        let mut retire = WriteBatch::new();
        retire.delete(S3MultipartPartCol::CF_NAME, &key);
        self.inner().inner().write_batch(retire)?;
        Ok(())
    }

    fn put_multipart_part_chunk(
        &self,
        upload_id: &str,
        part_number: u32,
        chunk_index: u32,
        data: Vec<u8>,
    ) -> Result<()> {
        let chunk_key = encode(
            &MultipartPartChunkKey::new(upload_digest(upload_id), part_number, chunk_index),
            "multipart chunk key",
        )?;
        let payload = encode(&MultipartPartChunk { data }, "multipart chunk payload")?;

        // A reel batch has to fit one segment the same way a value does, so each
        // chunk is written on its own.
        let mut batch = WriteBatch::new();
        batch.put(S3MultipartPartDataCol::CF_NAME, &chunk_key, &payload);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn publish_multipart_part(&self, upload_id: &str, part: &MultipartPart) -> Result<()> {
        let digest = upload_digest(upload_id);
        let key = encode(&part_key(upload_id, part.part_number), "multipart part key")?;
        let metadata = encode(part, "multipart part metadata")?;
        let count = chunk_count(part.size as usize);
        let raw = self.inner().inner();

        // The stale chunks and the metadata row go in one batch, so a published
        // part never reads chunks an earlier, longer upload left behind.
        let prefix = MultipartPartChunkKey::part_prefix(digest, part.part_number);
        let mut publish = WriteBatch::new();
        for stale in raw.iter_keys_prefix(S3MultipartPartDataCol::CF_NAME, &prefix)? {
            let chunk_key: MultipartPartChunkKey =
                wincode::deserialize(&stale).map_err(|error| {
                    TapeStoreError::Serialization(format!("multipart chunk key: {error}"))
                })?;
            if chunk_key.chunk_index >= count {
                publish.delete(S3MultipartPartDataCol::CF_NAME, &stale);
            }
        }
        publish.put(S3MultipartPartCol::CF_NAME, &key, &metadata);
        raw.write_batch(publish)?;
        Ok(())
    }

    fn delete_multipart_part(&self, upload_id: &str, part_number: u32) -> Result<()> {
        let digest = upload_digest(upload_id);
        let key = encode(&part_key(upload_id, part_number), "multipart part key")?;
        let raw = self.inner().inner();

        let prefix = MultipartPartChunkKey::part_prefix(digest, part_number);
        let mut batch = WriteBatch::new();
        batch.delete(S3MultipartPartCol::CF_NAME, &key);
        for chunk_key in raw.iter_keys_prefix(S3MultipartPartDataCol::CF_NAME, &prefix)? {
            batch.delete(S3MultipartPartDataCol::CF_NAME, &chunk_key);
        }
        raw.write_batch(batch)?;
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
        let prefix = MultipartPartChunkKey::part_prefix(upload_digest(upload_id), part_number);

        // The 36-byte prefix scopes the scan to this part, and the chunk-index
        // suffix orders it, so concatenating the values rebuilds the part.
        let mut data = Vec::new();
        let mut is_present = false;
        for (_key, value) in self
            .inner()
            .inner()
            .iter_prefix(S3MultipartPartDataCol::CF_NAME, &prefix)?
        {
            let chunk: MultipartPartChunk = wincode::deserialize(&value).map_err(|error| {
                TapeStoreError::Serialization(format!("multipart chunk payload: {error}"))
            })?;
            data.extend_from_slice(&chunk.data);
            is_present = true;
        }
        match is_present {
            true => Ok(Some(data)),
            false => Ok(None),
        }
    }

    fn get_multipart_part_chunk(
        &self,
        upload_id: &str,
        part_number: u32,
        chunk_index: u32,
    ) -> Result<Option<Vec<u8>>> {
        let key = MultipartPartChunkKey::new(upload_digest(upload_id), part_number, chunk_index);
        let chunk = self.get::<S3MultipartPartDataCol>(&key)?;
        Ok(chunk.map(|chunk| chunk.data))
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
    use tape_store_memory::MemoryStore;
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

    // Bytes that vary with position, so a dropped or reordered chunk shows up.
    fn pattern(size: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(size);
        for index in 0..size {
            data.push((index % 251) as u8);
        }
        data
    }

    // Number of chunk records held for an upload, across all of its parts.
    fn stored_chunks(store: &TapeStore<MemoryStore>, upload_id: &str) -> usize {
        let prefix = MultipartPartKey::upload_prefix(upload_digest(upload_id));
        store
            .inner()
            .inner()
            .iter_keys_prefix(S3MultipartPartDataCol::CF_NAME, &prefix)
            .expect("scan chunks")
            .len()
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

    // re-uploading a part number overwrites it, dropping the chunks it no longer fills
    #[test]
    fn part_overwrite() {
        let store = store();
        put_part(&store, "u1", 1, &pattern(MULTIPART_CHUNK_BYTES + 7));
        put_part(&store, "u1", 1, b"new");

        let parts = store.list_multipart_parts("u1").expect("list parts");
        assert_eq!(parts.len(), 1);
        assert_eq!(stored_chunks(&store, "u1"), 1);
        assert_eq!(
            store.get_multipart_part_data("u1", 1).expect("data"),
            Some(b"new".to_vec())
        );
    }

    // a part longer than one chunk reads back whole
    #[test]
    fn multi_chunk_part() {
        let store = store();
        let data = pattern(MULTIPART_CHUNK_BYTES * 2 + 7);

        put_part(&store, "u1", 1, &data);

        assert_eq!(stored_chunks(&store, "u1"), 3);
        assert_eq!(store.list_multipart_parts("u1").expect("list")[0].size, data.len() as u64);
        assert_eq!(store.get_multipart_part_data("u1", 1).expect("data"), Some(data));
    }

    // a part reads back one chunk at a time, the last one short
    #[test]
    fn chunk_reads() {
        let store = store();
        let data = pattern(MULTIPART_CHUNK_BYTES * 2 + 7);

        put_part(&store, "u1", 1, &data);

        for index in 0..3 {
            let offset = index * MULTIPART_CHUNK_BYTES;
            let end = data.len().min(offset + MULTIPART_CHUNK_BYTES);
            let chunk = store.get_multipart_part_chunk("u1", 1, index as u32).expect("chunk");
            assert_eq!(chunk.as_deref(), Some(&data[offset..end]));
        }
        assert!(store.get_multipart_part_chunk("u1", 1, 3).expect("chunk").is_none());
        assert!(store.get_multipart_part_chunk("u1", 2, 0).expect("chunk").is_none());
        assert!(store.get_multipart_part_chunk("u2", 1, 0).expect("chunk").is_none());
    }

    // a part written chunk by chunk reads as absent until published, and a
    // shorter rewrite drops the chunks it no longer fills
    #[test]
    fn streamed_part() {
        let store = store();
        let first = pattern(MULTIPART_CHUNK_BYTES * 2 + 7);
        put_part(&store, "u1", 1, &first);
        assert_eq!(stored_chunks(&store, "u1"), 3);

        store.retire_multipart_part("u1", 1).expect("retire");
        assert!(store.list_multipart_parts("u1").expect("list").is_empty());
        let second = pattern(MULTIPART_CHUNK_BYTES + 3);
        store
            .put_multipart_part_chunk("u1", 1, 0, second[..MULTIPART_CHUNK_BYTES].to_vec())
            .expect("chunk 0");
        store
            .put_multipart_part_chunk("u1", 1, 1, second[MULTIPART_CHUNK_BYTES..].to_vec())
            .expect("chunk 1");
        assert!(store.list_multipart_parts("u1").expect("list").is_empty());

        let part = MultipartPart {
            part_number: 1,
            etag: hash(&second),
            last_modified: 2_000,
            size: second.len() as u64,
        };
        store.publish_multipart_part("u1", &part).expect("publish");
        assert_eq!(stored_chunks(&store, "u1"), 2);
        assert_eq!(store.list_multipart_parts("u1").expect("list"), vec![part]);
        assert_eq!(store.get_multipart_part_data("u1", 1).expect("data"), Some(second));

        store.delete_multipart_part("u1", 1).expect("delete");
        assert_eq!(stored_chunks(&store, "u1"), 0);
        assert!(store.list_multipart_parts("u1").expect("list").is_empty());
    }

    // deleting an upload removes every chunk of every part
    #[test]
    fn delete_clears_chunks() {
        let store = store();
        store.put_multipart_upload("u1", &upload()).expect("put upload");
        put_part(&store, "u1", 1, &pattern(MULTIPART_CHUNK_BYTES + 7));
        put_part(&store, "u1", 2, &pattern(MULTIPART_CHUNK_BYTES * 2));
        assert_eq!(stored_chunks(&store, "u1"), 4);

        store.delete_multipart_upload("u1").expect("delete");

        assert_eq!(stored_chunks(&store, "u1"), 0);
        assert!(store.get_multipart_part_data("u1", 1).expect("data").is_none());
        assert!(store.get_multipart_part_data("u1", 2).expect("data").is_none());
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
