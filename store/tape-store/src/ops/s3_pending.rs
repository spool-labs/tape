//! Durable queue of S3 writes waiting to reach the chain.
//!
//! A bucket's tape account admits one write per block, so a PUT or DELETE is
//! acknowledged once it is durable here and applied on chain afterwards. Reads
//! serve from this queue until the object index has the key.

use store::{Column, Direction, Store, WriteBatch};
use tape_crypto::address::Address;

use crate::columns::{S3PendingWriteCol, S3PendingWriteDataCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{PendingWrite, PendingWriteData, PendingWriteKey};
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
    /// Queue `write` for `(tape, key)`, replacing any entry already there and
    /// dropping its bytes. The entry and its payload land in one atomic batch.
    fn put_pending_write(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        data: Option<Vec<u8>>,
    ) -> Result<()>;

    /// The queue entry for `(tape, key)`, if present
    fn get_pending_write(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>>;

    /// The queued bytes for `(tape, key)`, if present
    fn get_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// One tape's queue entries whose key starts with `prefix`, from the
    /// inclusive `start` key, in key order and without reading any payload
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

    /// The highest `seq` in the queue, or 0 when it is empty
    fn max_pending_write_seq(&self) -> Result<u64>;
}

impl<Backend: Store> PendingWriteOps for TapeStore<Backend> {
    fn put_pending_write(
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
        // A Delete replacing a Put has to clear the Put's bytes, or the payload
        // outlives the entry that owns it.
        match data {
            Some(data) => {
                let payload = encode(&PendingWriteData { data }, "pending write payload")?;
                batch.put(S3PendingWriteDataCol::CF_NAME, &row_key, &payload);
            }
            None => batch.delete(S3PendingWriteDataCol::CF_NAME, &row_key),
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn get_pending_write(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>> {
        Ok(self.get::<S3PendingWriteCol>(&PendingWriteKey::new(tape, key.to_vec()))?)
    }

    fn get_pending_write_data(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self
            .get::<S3PendingWriteDataCol>(&PendingWriteKey::new(tape, key.to_vec()))?
            .map(|payload| payload.data))
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

    fn max_pending_write_seq(&self) -> Result<u64> {
        let mut highest = 0;
        for (_key, value) in self.inner().inner().iter(S3PendingWriteCol::CF_NAME)? {
            highest = highest.max(decode_entry(&value)?.seq);
        }
        Ok(highest)
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
            },
            state: PendingState::Queued,
        }
    }

    fn delete_entry(seq: u64) -> PendingWrite {
        PendingWrite {
            seq,
            op: PendingOp::Delete,
            state: PendingState::Queued,
        }
    }

    fn queue_put(store: &TapeStore<MemoryStore>, tape: Address, key: &str, seq: u64, data: &[u8]) {
        store
            .put_pending_write(tape, key.as_bytes(), &put_entry(seq, data), Some(data.to_vec()))
            .expect("queue put");
    }

    // a queued put reads back with its bytes, and removal clears both rows
    #[test]
    fn put_round_trip() {
        let store = store();
        let tape = Address::new([1u8; 32]);
        assert!(store.get_pending_write(tape, b"a.txt").expect("get").is_none());

        queue_put(&store, tape, "a.txt", 1, b"hello");

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
    #[test]
    fn replace_drops_bytes() {
        let store = store();
        let tape = Address::new([2u8; 32]);
        queue_put(&store, tape, "a.txt", 1, b"old");

        store
            .put_pending_write(tape, b"a.txt", &delete_entry(2), None)
            .expect("queue delete");

        let entry = store.get_pending_write(tape, b"a.txt").expect("get").expect("queued");
        assert_eq!(entry.seq, 2);
        assert_eq!(entry.op, PendingOp::Delete);
        assert!(store.get_pending_write_data(tape, b"a.txt").expect("data").is_none());
    }

    // a prefix scan starts at the given key and stops leaving the prefix
    #[test]
    fn scan_from_start() {
        let store = store();
        let tape = Address::new([3u8; 32]);
        queue_put(&store, tape, "logs/a", 1, b"a");
        queue_put(&store, tape, "logs/b", 2, b"b");
        queue_put(&store, tape, "logs/c", 3, b"c");
        queue_put(&store, tape, "other", 4, b"d");

        let scanned = store
            .scan_pending_writes_from(tape, b"logs/", b"logs/b")
            .expect("scan");

        let keys: Vec<Vec<u8>> = scanned.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, vec![b"logs/b".to_vec(), b"logs/c".to_vec()]);
    }

    // a tape's scan sees only its own entries
    #[test]
    fn scan_scoped() {
        let store = store();
        let one = Address::new([4u8; 32]);
        let two = Address::new([5u8; 32]);
        queue_put(&store, one, "k", 1, b"a");
        queue_put(&store, two, "k", 2, b"b");

        assert_eq!(store.scan_pending_writes(one).expect("scan").len(), 1);
        assert_eq!(store.scan_pending_writes(two).expect("scan").len(), 1);
        assert_eq!(store.scan_pending_writes_from(one, b"", b"").expect("scan").len(), 1);
    }

    // the tape list names each queued bucket once
    #[test]
    fn tape_list() {
        let store = store();
        let one = Address::new([6u8; 32]);
        let two = Address::new([7u8; 32]);
        queue_put(&store, one, "a", 1, b"a");
        queue_put(&store, one, "b", 2, b"b");
        queue_put(&store, two, "a", 3, b"c");

        let mut tapes = store.pending_write_tapes().expect("tapes");
        tapes.sort();
        assert_eq!(tapes, vec![one, two]);
    }

    // the seed reads the highest stored seq back after a restart
    #[test]
    fn highest_seq() {
        let store = store();
        let tape = Address::new([8u8; 32]);
        assert_eq!(store.max_pending_write_seq().expect("seq"), 0);

        queue_put(&store, tape, "a", 4, b"a");
        queue_put(&store, tape, "b", 9, b"b");
        queue_put(&store, tape, "c", 2, b"c");

        assert_eq!(store.max_pending_write_seq().expect("seq"), 9);
    }

    // a landed or failed state survives the round trip the drain needs
    #[test]
    fn states_round_trip() {
        let store = store();
        let tape = Address::new([9u8; 32]);
        let track = Address::new([0xAB; 32]);
        let mut entry = put_entry(1, b"x");
        entry.state = PendingState::Landed { track };
        store
            .put_pending_write(tape, b"a", &entry, Some(b"x".to_vec()))
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
