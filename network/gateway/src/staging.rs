//! Durable queue of S3 writes the gateway has already acknowledged.
//!
//! A bucket's tape account admits one write per block, so a PUT or DELETE is
//! answered once it is durable here and applied on chain by the drain. Reads and
//! listings serve from here until the object index has the key.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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

/// What one entry counts towards each byte total.
#[derive(Clone, Copy, Default)]
struct Counts {
    /// Payload bytes on the gateway's disk, whatever the entry's state.
    held: u64,
    /// Payload bytes that have not reached the chain.
    unlanded: u64,
}

/// The entry's contribution to each total.
///
/// A landed entry keeps its payload until the index catches up, so it still
/// costs disk but no longer costs the tape capacity.
fn counts_for(write: &PendingWrite, has_payload: bool) -> Counts {
    let (size, is_landed) = match (&write.op, &write.state) {
        (PendingOp::Put { size, .. }, PendingState::Landed { .. }) => (*size, true),
        (PendingOp::Put { size, .. }, _) => (*size, false),
        (PendingOp::Delete { .. }, _) => (0, true),
    };
    Counts {
        held: if has_payload { size } else { 0 },
        unlanded: if is_landed { 0 } else { size },
    }
}

/// Bytes the queue accounts for: held on disk in total, unlanded per bucket.
///
/// The held total bounds the disk a chain or ingestor outage can consume; the
/// unlanded figure is what a tape's free capacity has to be reduced by.
#[derive(Default)]
struct QueuedBytes {
    held: u64,
    per_tape: HashMap<Address, u64>,
}

impl QueuedBytes {
    /// Start counting `counts` against `tape`.
    fn add(&mut self, tape: Address, counts: Counts) {
        self.held = self.held.saturating_add(counts.held);
        if counts.unlanded == 0 {
            return;
        }
        let unlanded = self.per_tape.entry(tape).or_insert(0);
        *unlanded = unlanded.saturating_add(counts.unlanded);
    }

    /// Stop counting `counts` against `tape`, dropping an emptied bucket.
    fn remove(&mut self, tape: Address, counts: Counts) {
        self.held = self.held.saturating_sub(counts.held);
        let Some(unlanded) = self.per_tape.get_mut(&tape) else {
            return;
        };
        *unlanded = unlanded.saturating_sub(counts.unlanded);
        if *unlanded == 0 {
            self.per_tape.remove(&tape);
        }
    }
}

/// The durable write queue, the counter that orders entries, and the signal that
/// wakes the drain.
pub struct StagingStore<Db: Store> {
    store: Arc<TapeStore<Db>>,
    /// Enqueue order, seeded from the highest stored seq so it keeps rising
    /// across a restart.
    next_seq: AtomicU64,
    queued_bytes: Mutex<QueuedBytes>,
    wake: Notify,
}

impl<Db: Store> StagingStore<Db> {
    /// Open the queue over `store`, continuing its sequence and recounting its
    /// bytes.
    pub fn try_new(store: Arc<TapeStore<Db>>) -> Result<Self, TapeStoreError> {
        let mut highest = 0;
        let mut queued = QueuedBytes::default();
        for (tape, key, write) in store.pending_write_entries()? {
            highest = highest.max(write.seq);
            let has_payload = store.has_pending_write_data(tape, &key)?;
            queued.add(tape, counts_for(&write, has_payload));
        }
        Ok(Self {
            store,
            next_seq: AtomicU64::new(highest.saturating_add(1)),
            queued_bytes: Mutex::new(queued),
            wake: Notify::new(),
        })
    }

    /// Payload bytes the queue holds on disk, across every bucket.
    pub fn queued_bytes(&self) -> u64 {
        match self.queued_bytes.lock() {
            Ok(queued) => queued.held,
            Err(poisoned) => poisoned.into_inner().held,
        }
    }

    /// Bytes queued for one bucket, which its free on-chain capacity has to cover.
    pub fn tape_queued_bytes(&self, tape: Address) -> u64 {
        let queued = match self.queued_bytes.lock() {
            Ok(queued) => queued,
            Err(poisoned) => poisoned.into_inner(),
        };
        queued.per_tape.get(&tape).copied().unwrap_or(0)
    }

    /// Whether `size` more bytes would take the queue past `max_queued_bytes`.
    pub fn is_over_budget(&self, size: u64, max_queued_bytes: u64) -> bool {
        self.queued_bytes().saturating_add(size) > max_queued_bytes
    }

    /// What `(tape, key)` counts towards each total right now.
    fn counted(&self, tape: Address, key: &[u8]) -> Counts {
        let Some(entry) = self.store.get_pending_write(tape, key).ok().flatten() else {
            return Counts::default();
        };
        let has_payload = self.store.has_pending_write_data(tape, key).unwrap_or(false);
        counts_for(&entry, has_payload)
    }

    /// Apply a completed write's effect on the totals, read before it and after.
    fn settle_count(&self, tape: Address, removed: Counts, added: Counts) {
        let mut queued = match self.queued_bytes.lock() {
            Ok(queued) => queued,
            Err(poisoned) => poisoned.into_inner(),
        };
        queued.remove(tape, removed);
        queued.add(tape, added);
    }

    /// Queue a written object with its bytes, superseding whatever the key held.
    ///
    /// A superseded write hands its landed track over, so this one overwrites and
    /// reclaims it rather than orphaning it.
    pub async fn enqueue_put(
        &self,
        tape: Address,
        key: &[u8],
        data: Vec<u8>,
        content_type: ContentType,
        etag: Hash,
        block_time: i64,
    ) -> Result<(), TapeStoreError> {
        let size = data.len() as u64;
        let prior = self.landed_put_track(tape, key)?;
        let write = PendingWrite {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            op: PendingOp::Put { content_type, etag, size, block_time, prior },
            state: PendingState::Queued,
        };
        let replaced = self.counted(tape, key);
        self.store.put_pending_write(tape, key, &write, Some(data)).await?;
        self.settle_count(tape, replaced, counts_for(&write, true));
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// Queue an object whose bytes already reached the chain, so reads resolve it
    /// through `track` until the index catches up.
    pub async fn enqueue_landed_put(
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
            op: PendingOp::Put { content_type, etag, size, block_time, prior: None },
            state: PendingState::Landed { track },
        };
        let replaced = self.counted(tape, key);
        self.store.put_pending_write(tape, key, &write, None).await?;
        self.settle_count(tape, replaced, counts_for(&write, false));
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// Queue a delete, dropping any queued Put for the same key and its bytes.
    ///
    /// A landed Put hands its track over, or the drain finds no index row, calls
    /// the delete a no-op, and the object reappears once the ingestor catches up.
    pub async fn enqueue_delete(&self, tape: Address, key: &[u8]) -> Result<(), TapeStoreError> {
        let landed = self.landed_put_track(tape, key)?;
        let write = PendingWrite {
            seq: self.next_seq.fetch_add(1, Ordering::Relaxed),
            op: PendingOp::Delete { track: landed },
            state: PendingState::Queued,
        };
        let replaced = self.counted(tape, key);
        self.store.put_pending_write(tape, key, &write, None).await?;
        self.settle_count(tape, replaced, counts_for(&write, false));
        metrics::inc_pending_write("enqueued");
        self.wake.notify_one();
        Ok(())
    }

    /// The track a queued Put has already written, when it has one.
    fn landed_put_track(
        &self,
        tape: Address,
        key: &[u8],
    ) -> Result<Option<Address>, TapeStoreError> {
        let Some(entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(None);
        };
        match (entry.op, entry.state) {
            (PendingOp::Put { .. }, PendingState::Landed { track }) => Ok(Some(track)),
            // Still in flight, but it may carry the track of what it superseded.
            (PendingOp::Put { prior, .. }, PendingState::Queued) => Ok(prior),
            (PendingOp::Put { prior, .. }, PendingState::Failed { .. }) => Ok(prior),
            (PendingOp::Delete { track }, _) => Ok(track),
        }
    }

    /// The queue entry for one key, if it has one.
    pub fn entry(&self, tape: Address, key: &[u8]) -> Result<Option<PendingWrite>, TapeStoreError> {
        self.store.get_pending_write(tape, key)
    }

    /// The queued bytes for one key, if the entry carries any.
    pub fn bytes(&self, tape: Address, key: &[u8]) -> Result<Option<Vec<u8>>, TapeStoreError> {
        self.store.get_pending_write_data(tape, key)
    }

    /// Whether the entry for one key carries bytes, without reading them.
    pub fn has_bytes(&self, tape: Address, key: &[u8]) -> Result<bool, TapeStoreError> {
        self.store.has_pending_write_data(tape, key)
    }

    /// Whether a queued delete hides this key, whatever the index still holds.
    pub fn is_deleted(&self, tape: Address, key: &[u8]) -> Result<bool, TapeStoreError> {
        let Some(entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(false);
        };
        Ok(matches!(entry.op, PendingOp::Delete { .. }))
    }

    /// One tape's queue entries under `prefix`, from the inclusive `start` key,
    /// in key order, for the listing merge.
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

    /// Record how far the drain got with one entry, leaving its bytes in place,
    /// and report whether it applied.
    ///
    /// A newer entry means the write was superseded in flight. Its outcome is
    /// discarded, but a track it landed is handed to that newer entry, or the
    /// object is orphaned on chain and reappears once the ingestor indexes it.
    pub fn set_state(
        &self,
        tape: Address,
        key: &[u8],
        seq: u64,
        state: PendingState,
    ) -> Result<bool, TapeStoreError> {
        let Some(mut entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(false);
        };
        if entry.seq != seq {
            if let PendingState::Landed { track } = state {
                self.store.attach_landed_track(tape, key, track)?;
            }
            return Ok(false);
        }
        let before = self.counted(tape, key);
        entry.state = state;
        self.store.put_pending_entry(tape, key, &entry)?;
        let has_payload = self.store.has_pending_write_data(tape, key).unwrap_or(false);
        self.settle_count(tape, before, counts_for(&entry, has_payload));
        Ok(true)
    }

    /// Drop an entry the index has caught up with.
    pub fn remove(&self, tape: Address, key: &[u8], seq: u64) -> Result<(), TapeStoreError> {
        let Some(entry) = self.store.get_pending_write(tape, key)? else {
            return Ok(());
        };
        if entry.seq != seq {
            return Ok(());
        }
        let removed = self.counted(tape, key);
        self.store.delete_pending_write(tape, key)?;
        self.settle_count(tape, removed, Counts::default());
        Ok(())
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

    async fn put(staging: &StagingStore<MemoryStore>, tape: Address, key: &[u8], data: &[u8]) {
        staging
            .enqueue_put(
                tape,
                key,
                data.to_vec(),
                ContentType::Unknown,
                Hash([1u8; 32]),
                1_700_000_000,
            )
            .await
            .expect("enqueue put");
    }

    // a queued put serves its bytes back and is not hidden
    #[tokio::test]
    async fn put_visible() {
        let staging = staging();
        let tape = Address::new([1u8; 32]);
        put(&staging, tape, b"a.txt", b"hello").await;

        assert!(staging.entry(tape, b"a.txt").expect("entry").is_some());
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"hello".to_vec()));
        assert!(!staging.is_deleted(tape, b"a.txt").expect("deleted"));
    }

    // a delete over a queued put hides the key and drops the bytes
    #[tokio::test]
    async fn delete_supersedes() {
        let staging = staging();
        let tape = Address::new([2u8; 32]);
        put(&staging, tape, b"a.txt", b"hello").await;

        staging.enqueue_delete(tape, b"a.txt").await.expect("enqueue delete");

        assert!(staging.is_deleted(tape, b"a.txt").expect("deleted"));
        assert!(staging.bytes(tape, b"a.txt").expect("bytes").is_none());
    }

    // a put after a delete brings the key back
    #[tokio::test]
    async fn put_supersedes() {
        let staging = staging();
        let tape = Address::new([3u8; 32]);
        staging.enqueue_delete(tape, b"a.txt").await.expect("enqueue delete");

        put(&staging, tape, b"a.txt", b"back").await;

        assert!(!staging.is_deleted(tape, b"a.txt").expect("deleted"));
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"back".to_vec()));
    }

    // entries come back in enqueue order, which is the order the drain applies
    #[tokio::test]
    async fn drain_order() {
        let staging = staging();
        let tape = Address::new([4u8; 32]);
        put(&staging, tape, b"z.txt", b"z").await;
        put(&staging, tape, b"a.txt", b"a").await;

        let keys: Vec<Vec<u8>> = staging
            .entries(tape)
            .expect("entries")
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, vec![b"z.txt".to_vec(), b"a.txt".to_vec()]);
    }

    // a delete over a landed put carries the track the put already wrote
    #[tokio::test]
    async fn delete_carries_track() {
        let staging = staging();
        let tape = Address::new([9u8; 32]);
        let track = Address::new([0xAB; 32]);
        put(&staging, tape, b"a.txt", b"one").await;
        let seq = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        staging
            .set_state(tape, b"a.txt", seq, PendingState::Landed { track })
            .expect("set state");

        staging.enqueue_delete(tape, b"a.txt").await.expect("enqueue delete");

        let entry = staging.entry(tape, b"a.txt").expect("entry").expect("queued");
        assert_eq!(entry.op, PendingOp::Delete { track: Some(track) });
    }

    // a delete queued while a put is in flight still gets the track that lands
    #[tokio::test]
    async fn supersede_by_delete() {
        let staging = staging();
        let tape = Address::new([0x21; 32]);
        let track = Address::new([0xAB; 32]);
        put(&staging, tape, b"a.txt", b"one").await;
        let inflight = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;

        staging.enqueue_delete(tape, b"a.txt").await.expect("enqueue delete");
        let is_applied = staging
            .set_state(tape, b"a.txt", inflight, PendingState::Landed { track })
            .expect("set state");

        assert!(!is_applied, "the in-flight write was already superseded");
        let entry = staging.entry(tape, b"a.txt").expect("entry").expect("queued");
        assert_eq!(entry.op, PendingOp::Delete { track: Some(track) });
    }

    // a put queued while a put is in flight carries the landed track as its prior
    #[tokio::test]
    async fn supersede_by_put() {
        let staging = staging();
        let tape = Address::new([0x22; 32]);
        let track = Address::new([0xCD; 32]);
        put(&staging, tape, b"a.txt", b"one").await;
        let inflight = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;

        put(&staging, tape, b"a.txt", b"two").await;
        let is_applied = staging
            .set_state(tape, b"a.txt", inflight, PendingState::Landed { track })
            .expect("set state");

        assert!(!is_applied, "the in-flight write was already superseded");
        let entry = staging.entry(tape, b"a.txt").expect("entry").expect("queued");
        assert!(matches!(entry.op, PendingOp::Put { prior: Some(carried), .. } if carried == track));
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"two".to_vec()));
    }

    // a state change keeps the queued bytes rather than rewriting them
    #[tokio::test]
    async fn state_keeps_bytes() {
        let staging = staging();
        let tape = Address::new([0x23; 32]);
        put(&staging, tape, b"a.txt", b"payload").await;
        let seq = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;

        let is_applied = staging
            .set_state(tape, b"a.txt", seq, PendingState::Landed { track: Address::default() })
            .expect("set state");

        assert!(is_applied);
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"payload".to_vec()));
    }

    // a state update for a superseded entry is discarded
    #[tokio::test]
    async fn stale_state() {
        let staging = staging();
        let tape = Address::new([5u8; 32]);
        put(&staging, tape, b"a.txt", b"one").await;
        let first = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        put(&staging, tape, b"a.txt", b"two").await;

        staging
            .set_state(tape, b"a.txt", first, PendingState::Landed { track: Address::default() })
            .expect("set state");

        let entry = staging.entry(tape, b"a.txt").expect("entry").expect("queued");
        assert_eq!(entry.state, PendingState::Queued);
        assert_eq!(staging.bytes(tape, b"a.txt").expect("bytes"), Some(b"two".to_vec()));
    }

    // a removal only drops the entry the drain actually finished
    #[tokio::test]
    async fn stale_remove() {
        let staging = staging();
        let tape = Address::new([6u8; 32]);
        put(&staging, tape, b"a.txt", b"one").await;
        let first = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        put(&staging, tape, b"a.txt", b"two").await;

        staging.remove(tape, b"a.txt", first).expect("remove");

        assert!(staging.entry(tape, b"a.txt").expect("entry").is_some());
    }

    // a reopened queue keeps every entry, its bytes and a rising sequence
    #[tokio::test]
    async fn restart_resume() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let tape = Address::new([0x31; 32]);
        let mut highest = 0;
        {
            let staging = StagingStore::try_new(store.clone()).expect("open queue");
            put(&staging, tape, b"a.txt", b"alpha").await;
            put(&staging, tape, b"b.txt", b"beta").await;
            staging.enqueue_delete(tape, b"c.txt").await.expect("enqueue delete");
            for (_, write) in staging.entries(tape).expect("entries") {
                highest = highest.max(write.seq);
            }
        }

        let reopened = StagingStore::try_new(store).expect("reopen queue");

        let entries = reopened.entries(tape).expect("entries");
        assert_eq!(entries.len(), 3);
        assert_eq!(reopened.bytes(tape, b"a.txt").expect("bytes"), Some(b"alpha".to_vec()));
        assert_eq!(reopened.bytes(tape, b"b.txt").expect("bytes"), Some(b"beta".to_vec()));
        assert!(reopened.is_deleted(tape, b"c.txt").expect("deleted"));
        assert_eq!(reopened.queued_bytes(), 9, "the byte totals are recounted at open");

        reopened.enqueue_delete(tape, b"d.txt").await.expect("enqueue delete");
        let next = reopened.entry(tape, b"d.txt").expect("entry").expect("queued").seq;
        assert!(next > highest, "the sequence continues past every stored entry");
    }

    // the byte budget refuses a write once the queue is full
    #[tokio::test]
    async fn byte_budget() {
        let staging = staging();
        let tape = Address::new([0x32; 32]);
        put(&staging, tape, b"a.txt", b"0123456789").await;

        assert_eq!(staging.queued_bytes(), 10);
        assert_eq!(staging.tape_queued_bytes(tape), 10);
        assert!(staging.is_over_budget(1, 10));
        assert!(!staging.is_over_budget(1, 16));

        // A landed write no longer holds the bucket's capacity down, but its
        // payload is still on disk until the index catches up and it is removed.
        let seq = staging.entry(tape, b"a.txt").expect("entry").expect("queued").seq;
        staging
            .set_state(tape, b"a.txt", seq, PendingState::Landed { track: Address::default() })
            .expect("set state");
        assert_eq!(staging.tape_queued_bytes(tape), 0);
        assert_eq!(staging.queued_bytes(), 10);
        assert!(staging.is_over_budget(1, 10), "a landed payload still fills the budget");

        staging.remove(tape, b"a.txt", seq).expect("remove");
        assert_eq!(staging.queued_bytes(), 0);
    }

    // a landed write that never held bytes here costs the budget nothing
    #[tokio::test]
    async fn landed_put_holds_nothing() {
        let staging = staging();
        let tape = Address::new([0x34; 32]);

        staging
            .enqueue_landed_put(
                tape,
                b"streamed.bin",
                1 << 30,
                ContentType::Unknown,
                Hash([2u8; 32]),
                1_700_000_000,
                Address::new([0xAB; 32]),
            )
            .await
            .expect("enqueue landed put");

        assert_eq!(staging.queued_bytes(), 0);
        assert_eq!(staging.tape_queued_bytes(tape), 0);
    }

    // a replaced entry stops counting the bytes it held
    #[tokio::test]
    async fn replace_recounts() {
        let staging = staging();
        let tape = Address::new([0x33; 32]);
        put(&staging, tape, b"a.txt", b"0123456789").await;

        put(&staging, tape, b"a.txt", b"01").await;
        assert_eq!(staging.queued_bytes(), 2);

        staging.enqueue_delete(tape, b"a.txt").await.expect("enqueue delete");
        assert_eq!(staging.queued_bytes(), 0, "a delete holds no bytes");
    }

    // the sequence resumes past the highest stored entry after a restart
    #[tokio::test]
    async fn seq_resumes() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let tape = Address::new([7u8; 32]);
        let first = StagingStore::try_new(store.clone()).expect("open queue");
        put(&first, tape, b"a.txt", b"a").await;
        let seq = first.entry(tape, b"a.txt").expect("entry").expect("queued").seq;

        let second = StagingStore::try_new(store).expect("reopen queue");
        second.enqueue_delete(tape, b"b.txt").await.expect("enqueue delete");

        assert!(second.entry(tape, b"b.txt").expect("entry").expect("queued").seq > seq);
    }
}
