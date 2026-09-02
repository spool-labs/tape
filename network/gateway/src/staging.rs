//! Durable queue of S3 writes the gateway has already acknowledged.
//!
//! A bucket's tape account admits one write per block, so a PUT or DELETE is
//! answered once it is durable in this queue and applied on chain afterwards by
//! the drain. Reads and listings serve from here until the object index has the
//! key, which is what gives an S3 client read-after-write.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use store::Store;
use tape_core::types::ContentType;
use tape_crypto::Hash;
use tape_crypto::address::Address;
use tape_store::TapeStore;
use tape_store::error::TapeStoreError;
use tape_store::ops::PendingWriteOps;
use tape_store::types::{PendingOp, PendingState, PendingWrite};
use tokio::sync::Notify;

use crate::metrics;

/// The durable write queue, plus the counter that orders entries and the signal
/// that wakes the drain.
pub struct StagingStore<Db: Store> {
    store: Arc<TapeStore<Db>>,
    /// Enqueue order, seeded from the highest stored seq so it keeps rising
    /// across a restart.
    next_seq: AtomicU64,
    wake: Notify,
}

impl<Db: Store> StagingStore<Db> {
    /// Open the queue over `store`, continuing its sequence where it left off.
    pub fn try_new(store: Arc<TapeStore<Db>>) -> Result<Self, TapeStoreError> {
        let highest = store.max_pending_write_seq()?;
        Ok(Self {
            store,
            next_seq: AtomicU64::new(highest.saturating_add(1)),
            wake: Notify::new(),
        })
    }

    /// Queue a written object with its bytes, superseding whatever the key held.
    pub fn enqueue_put(
        &self,
        tape: Address,
        key: &[u8],
        data: Vec<u8>,
        content_type: ContentType,
        etag: Hash,
        block_time: i64,
    ) -> Result<(), TapeStoreError> {
        let size = data.len() as u64;
        let write = PendingWrite {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            op: PendingOp::Put { content_type, etag, size, block_time },
            state: PendingState::Queued,
        };
        self.store.put_pending_write(tape, key, &write, Some(data))?;
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// Queue an object whose bytes already reached the chain, so reads resolve it
    /// through `track` until the index catches up.
    pub fn enqueue_landed_put(
        &self,
        tape: Address,
        key: &[u8],
        size: u64,
        content_type: ContentType,
        etag: Hash,
        block_time: i64,
        track: Address,
    ) -> Result<(), TapeStoreError> {
        let write = PendingWrite {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            op: PendingOp::Put { content_type, etag, size, block_time },
            state: PendingState::Landed { track },
        };
        self.store.put_pending_write(tape, key, &write, None)?;
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// Queue a delete, dropping any queued Put for the same key and its bytes.
    pub fn enqueue_delete(&self, tape: Address, key: &[u8]) -> Result<(), TapeStoreError> {
        let write = PendingWrite {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            op: PendingOp::Delete,
            state: PendingState::Queued,
        };
        self.store.put_pending_write(tape, key, &write, None)?;
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// The queue entry for one key, if it has one.
    pub fn entry(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>, TapeStoreError> {
        self.store.get_pending_write(tape, key)
    }

    /// The queued bytes for one key, if the entry carries any.
    pub fn bytes(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>, TapeStoreError> {
        self.store.get_pending_write_data(tape, key)
    }

    /// Whether a queued delete hides this key, whatever the index still holds.
    pub fn is_deleted(&self, tape: Address, key: &[u8]) -> Result<bool, TapeStoreError> {
        let Some(entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(false);
        };
        Ok(matches!(entry.op, PendingOp::Delete))
    }

    /// One tape's queue entries under `prefix`, from the inclusive `start` key,
    /// in key order. Feeds the listing merge.
    pub fn queued_from(
        &self,
        tape: Address,
        prefix: &[u8],
        start: &[u8],
    ) -> Result<Vec<(Vec<u8>, PendingWrite)>, TapeStoreError> {
        self.store.scan_pending_writes_from(tape, prefix, start)
    }

    /// Every tape with queued work, for the drain.
    pub fn tapes(&self) -> Result<Vec<Address>, TapeStoreError> {
        self.store.pending_write_tapes()
    }

    /// One tape's queue entries in seq order, for the drain.
    pub fn entries(&self, tape: Address) -> Result<Vec<(Vec<u8>, PendingWrite)>, TapeStoreError> {
        let mut entries = self.store.scan_pending_writes(tape)?;
        entries.sort_by_key(|(_, write)| write.seq);
        Ok(entries)
    }

    /// Record how far the drain got with one entry, leaving its bytes in place.
    ///
    /// A newer entry for the same key means the client has since replaced this
    /// write, so the outcome of the old one is discarded rather than written back.
    pub fn set_state(
        &self,
        tape: Address,
        key: &[u8],
        seq: u64,
        state: PendingState,
    ) -> Result<(), TapeStoreError> {
        let Some(mut entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(());
        };
        if entry.seq != seq {
            return Ok(());
        }
        let data = self.store.get_pending_write_data(tape, key)?;
        entry.state = state;
        self.store.put_pending_write(tape, key, &entry, data)
    }

    /// Drop an entry the index has caught up with.
    pub fn remove(&self, tape: Address, key: &[u8], seq: u64) -> Result<(), TapeStoreError> {
        let Some(entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(());
        };
        if entry.seq != seq {
            return Ok(());
        }
        self.store.delete_pending_write(tape, key)
    }

    /// Wait until a write is queued; the drain also ticks on its own.
    pub async fn queued(&self) {
        self.wake.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use store_memory::MemoryStore;

    use super::*;

    fn staging() -> StagingStore<MemoryStore> {
        StagingStore::try_new(Arc::new(TapeStore::new(MemoryStore::new()))).expect("open queue")
    }

    fn put(staging: &StagingStore<MemoryStore>, tape: Address, key: &[u8], data: &[u8]) {
        staging
            .enqueue_put(
                tape,
                key,
                data.to_vec(),
                ContentType::Unknown,
                Hash([1u8; 32]),
                1_700_000_000,
            )
            .expect("enqueue put");
    }

    // a queued put serves its bytes back and is not hidden
    #[test]
    fn put_visible() {
        let staging = staging();
        let tape = Address::new([1u8; 32]);
        put(&staging, tape, b"a.txt", b"hello");

        assert!(staging.entry(tape, b"a.txt").expect("entry").is_some());
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"hello".to_vec()));
        assert!(!staging.is_deleted(tape, b"a.txt").expect("deleted"));
    }

    // a delete over a queued put hides the key and drops the bytes
    #[test]
    fn delete_supersedes() {
        let staging = staging();
        let tape = Address::new([2u8; 32]);
        put(&staging, tape, b"a.txt", b"hello");

        staging.enqueue_delete(tape, b"a.txt").expect("enqueue delete");

        assert!(staging.is_deleted(tape, b"a.txt").expect("deleted"));
        assert!(staging.bytes(tape, b"a.txt").expect("bytes").is_none());
    }

    // a put after a delete brings the key back
    #[test]
    fn put_supersedes() {
        let staging = staging();
        let tape = Address::new([3u8; 32]);
        staging.enqueue_delete(tape, b"a.txt").expect("enqueue delete");

        put(&staging, tape, b"a.txt", b"back");

        assert!(!staging.is_deleted(tape, b"a.txt").expect("deleted"));
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"back".to_vec()));
    }

    // entries come back in enqueue order, which is the order the drain applies
    #[test]
    fn drain_order() {
        let staging = staging();
        let tape = Address::new([4u8; 32]);
        put(&staging, tape, b"z.txt", b"z");
        put(&staging, tape, b"a.txt", b"a");

        let keys: Vec<Vec<u8>> = staging
            .entries(tape)
            .expect("entries")
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, vec![b"z.txt".to_vec(), b"a.txt".to_vec()]);
    }

    // a state update for a superseded entry is discarded
    #[test]
    fn stale_state() {
        let staging = staging();
        let tape = Address::new([5u8; 32]);
        put(&staging, tape, b"a.txt", b"one");
        let first = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        put(&staging, tape, b"a.txt", b"two");

        staging
            .set_state(tape, b"a.txt", first, PendingState::Landed { track: Address::default() })
            .expect("set state");

        let entry = staging.entry(tape, b"a.txt").expect("entry").expect("queued");
        assert_eq!(entry.state, PendingState::Queued);
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"two".to_vec()));
    }

    // a removal only drops the entry the drain actually finished
    #[test]
    fn stale_remove() {
        let staging = staging();
        let tape = Address::new([6u8; 32]);
        put(&staging, tape, b"a.txt", b"one");
        let first = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        put(&staging, tape, b"a.txt", b"two");

        staging.remove(tape, b"a.txt", first).expect("remove");

        assert!(staging.entry(tape, b"a.txt").expect("entry").is_some());
    }

    // the sequence resumes past the highest stored entry after a restart
    #[test]
    fn seq_resumes() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let tape = Address::new([7u8; 32]);
        let first = StagingStore::try_new(store.clone()).expect("open queue");
        put(&first, tape, b"a.txt", b"a");
        let seq = first.entry(tape, b"a.txt").expect("entry").expect("queued").seq;

        let second = StagingStore::try_new(store).expect("reopen queue");
        second.enqueue_delete(tape, b"b.txt").expect("enqueue delete");

        assert!(second.entry(tape, b"b.txt").expect("entry").expect("queued").seq > seq);
    }
}
