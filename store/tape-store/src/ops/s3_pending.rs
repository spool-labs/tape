//! Durable queue of S3 writes waiting to reach the chain.

use std::future::Future;

use store::{Column, Direction, Store, WriteBatch};
use tape_crypto::address::Address;

use crate::columns::{S3PendingWriteCol, S3PendingWriteDataCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{PendingOp, PendingWrite, PendingWriteData, PendingWriteKey};
use crate::TapeStore;

/// Serialize a value to raw bytes for a write batch.
fn encode<Value>(value: &Value, what: &str) -> Result<Vec<u8>>
where
    Value: wincode::SchemaWrite<Src = Value>,
{
    wincode::serialize(value)
        .map_err(|error| TapeStoreError::Serialization(format!("{what}: {error}")))
}

/// Decode one queue entry read back from the column.
fn decode_entry(value: &[u8]) -> Result<PendingWrite> {
    wincode::deserialize(value)
        .map_err(|error| TapeStoreError::Serialization(format!("pending write: {error}")))
}

/// Operations for the durable queue of S3 writes
pub trait PendingWriteOps {
    /// Queue `write` for `(tape, key)`, replacing any entry and its bytes; awaited so the ack is durable.
    fn put_pending_write(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        data: Option<Vec<u8>>,
    ) -> impl Future<Output = Result<()>> + Send;

    /// Overwrite the entry for `(tape, key)`, leaving its bytes alone.
    fn put_pending_entry(&self, tape: Address, key: &[u8], write: &PendingWrite) -> Result<()>;

    /// Record a landed track on the entry for `(tape, key)` when it names none; returns whether it changed.
    fn attach_landed_track(&self, tape: Address, key: &[u8], track: Address) -> Result<bool>;

    /// The queue entry for `(tape, key)`, if present
    fn get_pending_write(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>>;

    /// The queued bytes for `(tape, key)`, if present
    fn get_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Whether `(tape, key)` has a stored payload, without reading it
    fn has_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<bool>;

    /// One tape's entries under `prefix` from the inclusive `start` key, without payloads
    fn scan_pending_writes_from(
        &self,
        tape: Address,
        prefix: &[u8],
        start: &[u8],
    ) -> Result<Vec<(Vec<u8>, PendingWrite)>>;

    /// Every queue entry for one tape, in key order and without reading any payload
    fn scan_pending_writes(&self, tape: Address) -> Result<Vec<(Vec<u8>, PendingWrite)>>;

    /// Every tape with at least one queue entry
    fn pending_write_tapes(&self) -> Result<Vec<Address>>;

    /// Drop the queue entry for `(tape, key)` and its bytes
    fn delete_pending_write(&self, tape: Address, key: &[u8]) -> Result<()>;

    /// Every queue entry as `(tape, key, entry)`, for the open-time scan
    fn pending_write_entries(&self) -> Result<Vec<(Address, Vec<u8>, PendingWrite)>>;
}

impl<Backend: Store> PendingWriteOps for TapeStore<Backend> {
    async fn put_pending_write(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        data: Option<Vec<u8>>,
    ) -> Result<()> {
        let row_key = encode(&PendingWriteKey::new(tape, key.to_vec()), "pending write key")?;
        let entry = encode(write, "pending write")?;

        let mut batch = WriteBatch::new();
        batch.put(S3PendingWriteCol::CF_NAME, &row_key, &entry);
        // A Delete replacing a Put clears the Put's bytes.
        match data {
            Some(data) => {
                let payload = encode(&PendingWriteData { data }, "pending write payload")?;
                batch.put(S3PendingWriteDataCol::CF_NAME, &row_key, &payload);
            }
            None => batch.delete(S3PendingWriteDataCol::CF_NAME, &row_key),
        }
        self.inner().inner().write_batch_wait(batch).await?;
        Ok(())
    }

    fn put_pending_entry(&self, tape: Address, key: &[u8], write: &PendingWrite) -> Result<()> {
        self.put::<S3PendingWriteCol>(&PendingWriteKey::new(tape, key.to_vec()), write)?;
        Ok(())
    }

    fn attach_landed_track(&self, tape: Address, key: &[u8], track: Address) -> Result<bool> {
        let Some(mut entry) = self.get_pending_write(tape, key)? else {
            return Ok(false);
        };
        match &mut entry.op {
            PendingOp::Put { prior: prior @ None, .. } => *prior = Some(track),
            PendingOp::Delete { track: named @ None } => *named = Some(track),
            PendingOp::Put { .. } => return Ok(false),
            PendingOp::Delete { .. } => return Ok(false),
        }
        self.put_pending_entry(tape, key, &entry)?;
        Ok(true)
    }

    fn get_pending_write(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>> {
        Ok(self.get::<S3PendingWriteCol>(&PendingWriteKey::new(tape, key.to_vec()))?)
    }

    fn get_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get::<S3PendingWriteDataCol>(&PendingWriteKey::new(tape, key.to_vec()))?
            .map(|payload| payload.data))
    }

    fn has_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<bool> {
        let row_key = encode(&PendingWriteKey::new(tape, key.to_vec()), "pending write key")?;
        let keys = self
            .inner()
            .inner()
            .iter_keys_prefix(S3PendingWriteDataCol::CF_NAME, &row_key)?;
        Ok(!keys.is_empty())
    }

    fn scan_pending_writes_from(
        &self,
        tape: Address,
        prefix: &[u8],
        start: &[u8],
    ) -> Result<Vec<(Vec<u8>, PendingWrite)>> {
        let tape_prefix = PendingWriteKey::tape_prefix(tape);
        let mut seek = Vec::with_capacity(tape_prefix.len() + start.len());
        seek.extend_from_slice(&tape_prefix);
        seek.extend_from_slice(start);

        let mut entries = Vec::new();
        for (row_key, value) in
            self.inner()
                .inner()
                .iter_from(S3PendingWriteCol::CF_NAME, &seek, Direction::Asc)?
        {
            if row_key.len() < 32 || row_key[..32] != tape_prefix {
                break;
            }
            let key = &row_key[32..];
            // Keys are sorted, so the first key past the prefix ends the range.
            if !key.starts_with(prefix) {
                break;
            }
            entries.push((key.to_vec(), decode_entry(&value)?));
        }
        Ok(entries)
    }

    fn scan_pending_writes(&self, tape: Address) -> Result<Vec<(Vec<u8>, PendingWrite)>> {
        let tape_prefix = PendingWriteKey::tape_prefix(tape);
        let mut entries = Vec::new();
        for (row_key, value) in self
            .inner()
            .inner()
            .iter_prefix(S3PendingWriteCol::CF_NAME, &tape_prefix)?
        {
            entries.push((row_key[32..].to_vec(), decode_entry(&value)?));
        }
        Ok(entries)
    }

    fn pending_write_tapes(&self) -> Result<Vec<Address>> {
        let mut tapes: Vec<Address> = Vec::new();
        for row_key in self
            .inner()
            .inner()
            .iter_keys_prefix(S3PendingWriteCol::CF_NAME, &[])?
        {
            if row_key.len() < 32 {
                continue;
            }
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&row_key[..32]);
            let tape = Address::from(bytes);
            if !tapes.contains(&tape) {
                tapes.push(tape);
            }
        }
        Ok(tapes)
    }

    fn delete_pending_write(&self, tape: Address, key: &[u8]) -> Result<()> {
        let row_key = encode(&PendingWriteKey::new(tape, key.to_vec()), "pending write key")?;

        let mut batch = WriteBatch::new();
        batch.delete(S3PendingWriteCol::CF_NAME, &row_key);
        batch.delete(S3PendingWriteDataCol::CF_NAME, &row_key);
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn pending_write_entries(&self) -> Result<Vec<(Address, Vec<u8>, PendingWrite)>> {
        let mut entries = Vec::new();
        for (row_key, value) in self.inner().inner().iter(S3PendingWriteCol::CF_NAME)? {
            if row_key.len() < 32 {
                continue;
            }
            let mut bytes = [0u8; 32];
            bytes.copy_from_slice(&row_key[..32]);
            entries.push((Address::from(bytes), row_key[32..].to_vec(), decode_entry(&value)?));
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;
    use tape_core::types::ContentType;
    use tape_crypto::hash::hash;

    use crate::types::{PendingOp, PendingState};

    use super::*;

    fn store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn put_entry(seq: u64, data: &[u8]) -> PendingWrite {
        PendingWrite {
            seq,
            op: PendingOp::Put {
                content_type: ContentType::TextPlain,
                etag: hash(data),
                size: data.len() as u64,
                block_time: 1_700_000_000,
                prior: None,
            },
            state: PendingState::Queued,
        }
    }

    fn delete_entry(seq: u64) -> PendingWrite {
        PendingWrite {
            seq,
            op: PendingOp::Delete { track: None },
            state: PendingState::Queued,
        }
    }

    async fn queue_put(
        store: &TapeStore<MemoryStore>,
        tape: Address,
        key: &str,
        seq: u64,
        data: &[u8],
    ) {
        store
            .put_pending_write(tape, key.as_bytes(), &put_entry(seq, data), Some(data.to_vec()))
            .await
            .expect("queue put");
    }

    // a queued put reads back with its bytes, and removal clears both rows
    #[tokio::test]
    async fn put_round_trip() {
        let store = store();
        let tape = Address::new([1u8; 32]);
        assert!(store.get_pending_write(tape, b"a.txt").expect("get").is_none());

        queue_put(&store, tape, "a.txt", 1, b"hello").await;

        let entry = store.get_pending_write(tape, b"a.txt").expect("get").expect("queued");
        assert_eq!(entry.seq, 1);
        assert_eq!(
            store.get_pending_write_data(tape, b"a.txt").expect("data"),
            Some(b"hello".to_vec())
        );

        store.delete_pending_write(tape, b"a.txt").expect("delete");
        assert!(store.get_pending_write(tape, b"a.txt").expect("get").is_none());
        assert!(store.get_pending_write_data(tape, b"a.txt").expect("data").is_none());
    }

    // re-queueing a key replaces the entry and drops the bytes it held
    #[tokio::test]
    async fn replace_drops_bytes() {
        let store = store();
        let tape = Address::new([2u8; 32]);
        queue_put(&store, tape, "a.txt", 1, b"old").await;

        store
            .put_pending_write(tape, b"a.txt", &delete_entry(2), None)
            .await
            .expect("queue delete");

        let entry = store.get_pending_write(tape, b"a.txt").expect("get").expect("queued");
        assert_eq!(entry.seq, 2);
        assert_eq!(entry.op, PendingOp::Delete { track: None });
        assert!(store.get_pending_write_data(tape, b"a.txt").expect("data").is_none());
    }

    // a prefix scan starts at the given key and stops leaving the prefix
    #[tokio::test]
    async fn scan_from_start() {
        let store = store();
        let tape = Address::new([3u8; 32]);
        queue_put(&store, tape, "logs/a", 1, b"a").await;
        queue_put(&store, tape, "logs/b", 2, b"b").await;
        queue_put(&store, tape, "logs/c", 3, b"c").await;
        queue_put(&store, tape, "other", 4, b"d").await;

        let scanned = store
            .scan_pending_writes_from(tape, b"logs/", b"logs/b")
            .expect("scan");

        let keys: Vec<Vec<u8>> = scanned.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, vec![b"logs/b".to_vec(), b"logs/c".to_vec()]);
    }

    // a tape's scan sees only its own entries
    #[tokio::test]
    async fn scan_scoped() {
        let store = store();
        let one = Address::new([4u8; 32]);
        let two = Address::new([5u8; 32]);
        queue_put(&store, one, "k", 1, b"a").await;
        queue_put(&store, two, "k", 2, b"b").await;

        assert_eq!(store.scan_pending_writes(one).expect("scan").len(), 1);
        assert_eq!(store.scan_pending_writes(two).expect("scan").len(), 1);
        assert_eq!(store.scan_pending_writes_from(one, b"", b"").expect("scan").len(), 1);
    }

    // the tape list names each queued bucket once
    #[tokio::test]
    async fn tape_list() {
        let store = store();
        let one = Address::new([6u8; 32]);
        let two = Address::new([7u8; 32]);
        queue_put(&store, one, "a", 1, b"a").await;
        queue_put(&store, one, "b", 2, b"b").await;
        queue_put(&store, two, "a", 3, b"c").await;

        let mut tapes = store.pending_write_tapes().expect("tapes");
        tapes.sort();
        assert_eq!(tapes, vec![one, two]);
    }

    // the open-time scan reports every entry with its tape
    #[tokio::test]
    async fn entry_scan() {
        let store = store();
        let tape = Address::new([8u8; 32]);
        assert!(store.pending_write_entries().expect("entries").is_empty());

        queue_put(&store, tape, "a", 4, b"a").await;
        queue_put(&store, tape, "b", 9, b"b").await;
        queue_put(&store, tape, "c", 2, b"c").await;

        let entries = store.pending_write_entries().expect("entries");
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|(owner, _, _)| *owner == tape));
        let highest = entries.iter().map(|(_, _, write)| write.seq).max();
        assert_eq!(highest, Some(9));
    }

    // a payload is reported present without reading it
    #[tokio::test]
    async fn payload_presence() {
        let store = store();
        let tape = Address::new([0x14; 32]);
        queue_put(&store, tape, "a.txt", 1, b"payload").await;
        store
            .put_pending_write(tape, b"b.txt", &delete_entry(2), None)
            .await
            .expect("queue delete");

        assert!(store.has_pending_write_data(tape, b"a.txt").expect("presence"));
        assert!(!store.has_pending_write_data(tape, b"b.txt").expect("presence"));
        assert!(!store.has_pending_write_data(tape, b"missing").expect("presence"));
    }

    // a state change leaves the payload row untouched
    #[tokio::test]
    async fn entry_only_write() {
        let store = store();
        let tape = Address::new([0x11; 32]);
        queue_put(&store, tape, "a.txt", 1, b"payload").await;

        let mut entry = put_entry(1, b"payload");
        entry.state = PendingState::Landed { track: Address::new([0xCD; 32]) };
        store.put_pending_entry(tape, b"a.txt", &entry).expect("entry write");

        assert_eq!(
            store.get_pending_write_data(tape, b"a.txt").expect("data"),
            Some(b"payload".to_vec()),
            "the payload row was not rewritten or dropped"
        );
        assert!(matches!(
            store.get_pending_write(tape, b"a.txt").expect("get").expect("queued").state,
            PendingState::Landed { .. }
        ));
    }

    // an entry-only write never creates a payload row
    #[tokio::test]
    async fn entry_only_leaves_no_payload() {
        let store = store();
        let tape = Address::new([0x12; 32]);

        store
            .put_pending_entry(tape, b"a.txt", &delete_entry(1))
            .expect("entry write");

        assert!(store.get_pending_write_data(tape, b"a.txt").expect("data").is_none());
    }

    // attaching a landed track fills in a delete that named none
    #[tokio::test]
    async fn attach_to_delete() {
        let store = store();
        let tape = Address::new([0x13; 32]);
        let track = Address::new([0xEF; 32]);
        store
            .put_pending_entry(tape, b"a.txt", &delete_entry(2))
            .expect("entry write");

        assert!(store.attach_landed_track(tape, b"a.txt", track).expect("attach"));

        let entry = store.get_pending_write(tape, b"a.txt").expect("get").expect("queued");
        assert_eq!(entry.op, PendingOp::Delete { track: Some(track) });
        assert!(
            !store.attach_landed_track(tape, b"a.txt", Address::default()).expect("attach"),
            "a track already named is never replaced"
        );
    }

    // a landed or failed state survives the round trip the drain needs
    #[tokio::test]
    async fn states_round_trip() {
        let store = store();
        let tape = Address::new([9u8; 32]);
        let track = Address::new([0xAB; 32]);
        let mut entry = put_entry(1, b"x");
        entry.state = PendingState::Landed { track };
        store
            .put_pending_write(tape, b"a", &entry, Some(b"x".to_vec()))
            .await
            .expect("queue landed");
        assert_eq!(
            store.get_pending_write(tape, b"a").expect("get").expect("queued").state,
            PendingState::Landed { track }
        );

        entry.state = PendingState::Failed {
            error: "rpc timeout".to_string(),
            attempts: 3,
        };
        store
            .put_pending_write(tape, b"a", &entry, Some(b"x".to_vec()))
            .await
            .expect("queue failed");
        assert_eq!(
            store.get_pending_write(tape, b"a").expect("get").expect("queued").state,
            PendingState::Failed {
                error: "rpc timeout".to_string(),
                attempts: 3,
            }
        );
    }
}
