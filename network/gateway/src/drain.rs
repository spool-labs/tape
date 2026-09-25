//! Applies queued S3 writes to the chain, one in-flight write per bucket.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::join_all;
use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_protocol::Api;
use tape_sdk::error::TapedriveError;
use tape_sdk::object::{MAX_OBJECT_BATCH_BYTES, MAX_OBJECT_BATCH_ITEMS, ObjectBatchItem};
use tape_sdk::stream::manifest::MAX_TRACK_SIZE;
use tape_store::types::{PendingOp, PendingState, PendingWrite};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::http::handlers::resolve::resolve_object;
use crate::http::handlers::s3::write::S3WriteContext;
use crate::metrics;
use crate::staging::StagingStore;

/// How often the drain looks for work regardless of an enqueue signal.
const DRAIN_TICK: Duration = Duration::from_millis(500);
/// First retry delay after a failed chain write.
const RETRY_MIN: Duration = Duration::from_secs(1);
/// Ceiling on the exponential retry delay.
const RETRY_MAX: Duration = Duration::from_secs(60);
/// Retry interval once an entry has been marked failed.
const FAILED_RETRY: Duration = Duration::from_secs(300);
/// Consecutive permanent failures before an entry is parked.
const MAX_ATTEMPTS: u32 = 3;
/// How often one bucket's transient outage is logged, however many entries hit it.
const OUTAGE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// How often the delegate's SOL balance is read.
const BALANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Balance below which chain attempts pause, since an underfunded payer only shows up as timeouts.
const DELEGATE_LAMPORTS_FLOOR: u64 = 50_000_000;

/// Program errors that fail the same way until an operator acts.
const PERMANENT_TRANSACTION_ERRORS: &[&str] = &[
    "insufficient funds",
    "insufficient lamports",
    "insufficient capacity",
    "account already in use",
    "invalid account data for instruction",
];

/// Transport and cluster failures the typed variants do not separate.
const TRANSIENT_TRANSACTION_ERRORS: &[&str] = &[
    "blockhash",
    "unreachable",
    "connection",
    "timed out",
    "timeout",
    "quorum",
];

/// Whether an error will clear on its own once the cluster or a peer is back.
fn is_transient(error: &TapedriveError) -> bool {
    match error {
        TapedriveError::Rpc(rpc_error) => is_transient_rpc(rpc_error),
        TapedriveError::Network(_) => true,
        TapedriveError::Peer(_) => true,
        TapedriveError::RateLimited { .. } => true,
        TapedriveError::Io(_) => true,
        TapedriveError::Certification(_) => true,
        TapedriveError::Upload(_) => true,
        TapedriveError::Download(_) => true,
        TapedriveError::NotFound => true,
        TapedriveError::InsufficientCapacity { .. } => false,
        TapedriveError::WriteConflict { .. } => false,
        TapedriveError::TrackPositionConflict { .. } => false,
        TapedriveError::CommitmentMismatch => false,
        TapedriveError::MissingPayer => false,
        TapedriveError::Encoding(_) => false,
        TapedriveError::InvalidArgument(_) => false,
        TapedriveError::Stream(_) => false,
    }
}

/// Whether one RPC failure is transient.
fn is_transient_rpc(error: &RpcError) -> bool {
    match error {
        RpcError::Transaction { message, .. } => is_transient_message(message),
        RpcError::Request(_) => true,
        RpcError::Timeout(_) => true,
        RpcError::BlockNotAvailable => true,
        RpcError::BlockPruned => true,
        RpcError::AllEndpointsFailed { .. } => true,
        RpcError::AccountNotFound(_) => true,
        RpcError::TransactionNotFound(_) => true,
        RpcError::BlockhashExpired => true,
        RpcError::SlotSkipped => false,
        RpcError::Internal(_) => true,
        RpcError::Deserialization(_) => false,
    }
}

/// Classify a transaction failure by its message; unrecognised failures stay transient.
fn is_transient_message(message: &str) -> bool {
    let message = message.to_lowercase();
    for permanent in PERMANENT_TRANSACTION_ERRORS {
        if message.contains(permanent) {
            return false;
        }
    }
    for transient in TRANSIENT_TRANSACTION_ERRORS {
        if message.contains(transient) {
            return true;
        }
    }
    true
}

/// Retry state for one entry.
struct Retry {
    /// Failures of any kind, for the backoff curve.
    steps: u32,
    /// Permanent failures only; a transient one never parks the entry.
    attempts: u32,
    next_attempt: Instant,
}

/// What the drain reports about its own health.
#[derive(Clone, Debug, Default)]
pub struct DrainHealth {
    /// Delegate signer balance in lamports, once it has been read.
    pub delegate_lamports: Option<u64>,
    /// Why chain attempts are held, when they are.
    pub paused_reason: Option<String>,
}

/// The drain's health, published for the admin control plane.
#[derive(Default)]
pub struct DrainStatus {
    health: Mutex<DrainHealth>,
}

impl DrainStatus {
    /// An empty status.
    pub fn new() -> Self {
        Self::default()
    }

    /// The last published health.
    pub fn health(&self) -> DrainHealth {
        match self.health.lock() {
            Ok(health) => health.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }

    /// Replace the published health.
    fn publish(&self, health: DrainHealth) {
        match self.health.lock() {
            Ok(mut current) => *current = health,
            Err(poisoned) => *poisoned.into_inner() = health,
        }
    }
}

/// Drains the durable S3 write queue onto the chain.
pub struct WriteDrain<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    write_ctx: Arc<S3WriteContext>,
    staging: Arc<StagingStore<Db>>,
    retries: Mutex<HashMap<(Address, Vec<u8>), Retry>>,
    /// When each bucket's outage was last logged.
    outage_logged: Mutex<HashMap<Address, Instant>>,
    status: Arc<DrainStatus>,
    cancel: CancellationToken,
}

impl<Db, Cluster, Blockchain> WriteDrain<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    /// Build the drain over the queue the S3 listener writes into.
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        write_ctx: Arc<S3WriteContext>,
        staging: Arc<StagingStore<Db>>,
        status: Arc<DrainStatus>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            write_ctx,
            staging,
            retries: Mutex::new(HashMap::new()),
            outage_logged: Mutex::new(HashMap::new()),
            status,
            cancel,
        }
    }

    /// Drain until cancelled, waking on each enqueue and ticking regardless.
    pub async fn run(self) -> Result<(), NodeError> {
        let mut balance_read_at = Instant::now() - BALANCE_INTERVAL;
        loop {
            tokio::select! {
                // Safe: cancellation carries no partial state.
                _ = self.cancel.cancelled() => return Ok(()),
                // Safe: a missed notification is picked up by the next tick.
                _ = self.staging.queued() => {}
                // Safe: sleep is cancellation-safe.
                _ = tokio::time::sleep(DRAIN_TICK) => {}
            }
            if balance_read_at.elapsed() >= BALANCE_INTERVAL {
                self.read_delegate_balance().await;
                balance_read_at = Instant::now();
            }
            self.pass().await;
        }
    }

    /// Read the delegate's balance and hold chain attempts while it is under the floor.
    async fn read_delegate_balance(&self) {
        let delegate = self.write_ctx.delegate_address();
        let lamports = match self.context.rpc.rpc().get_account(&delegate).await {
            Ok(account) => account.lamports,
            // A missing account holds nothing, which is exactly what the floor catches.
            Err(RpcError::AccountNotFound(_)) => 0,
            Err(error) => {
                warn!(%error, %delegate, "s3 drain: delegate balance unavailable");
                return;
            }
        };
        metrics::set_delegate_lamports(lamports);

        let was_paused = self.status.health().paused_reason.is_some();
        let paused_reason = match lamports < DELEGATE_LAMPORTS_FLOOR {
            true => Some(format!(
                "delegate {delegate} holds {lamports} lamports, under the {DELEGATE_LAMPORTS_FLOOR} floor"
            )),
            false => None,
        };
        if let Some(reason) = &paused_reason {
            if !was_paused {
                warn!(%reason, "s3 drain: chain writes paused, queue holding");
            }
        }
        self.status.publish(DrainHealth {
            delegate_lamports: Some(lamports),
            paused_reason,
        });
    }

    /// Whether chain attempts are held right now.
    fn is_paused(&self) -> bool {
        self.status.health().paused_reason.is_some()
    }

    /// One sweep of every bucket with queued work.
    async fn pass(&self) {
        let tapes = match self.staging.tapes() {
            Ok(tapes) => tapes,
            Err(error) => {
                warn!(%error, "s3 drain: listing queued buckets failed");
                return;
            }
        };

        let mut passes = Vec::with_capacity(tapes.len());
        for tape in tapes {
            passes.push(self.drain_tape(tape));
        }
        let remaining: u64 = join_all(passes).await.into_iter().sum();
        metrics::set_pending_writes_queued(remaining);
        metrics::set_pending_writes_queued_bytes(self.staging.queued_bytes());
    }

    /// Apply one bucket's entries in queue order, returning how many are left.
    async fn drain_tape(&self, tape: Address) -> u64 {
        let entries = match self.staging.entries(tape) {
            Ok(entries) => entries,
            Err(error) => {
                warn!(%error, %tape, "s3 drain: reading a bucket's queue failed");
                return 0;
            }
        };

        let mut remaining = 0;
        let mut run = Vec::new();
        for (key, write) in entries {
            if self.cancel.is_cancelled() {
                remaining += 1;
                continue;
            }
            if matches!(write.state, PendingState::Landed { .. }) {
                if !self.reap(tape, &key, &write) {
                    remaining += 1;
                }
                continue;
            }
            // An attempted entry stays until the index catches up and it is reaped.
            remaining += 1;
            if self.is_paused() || !self.is_due(tape, &key) {
                continue;
            }
            match self.batch_candidate(tape, &key, &write) {
                Some(candidate) => run.push(candidate),
                None => {
                    // Queue order holds, so everything before this entry goes first.
                    self.write_run(tape, std::mem::take(&mut run)).await;
                    self.attempt(tape, &key, &write).await;
                }
            }
        }
        self.write_run(tape, run).await;
        remaining
    }

    /// A queued put that can share a batch: a fresh object of one track, or one replacing different content
    fn batch_candidate(&self, tape: Address, key: &[u8], write: &PendingWrite) -> Option<Candidate> {
        let PendingOp::Put { etag, size, prior, .. } = write.op else {
            return None;
        };
        let name = String::from_utf8(key.to_vec()).ok()?;
        if name.is_empty() || name.contains(['\0', '\n', '\r']) || size > MAX_TRACK_SIZE as u64 {
            return None;
        }
        // The single path handles matching content, whether landed or left to finish.
        let indexed = resolve_object(self.context.store.as_ref(), tape, key).ok()?;
        if indexed.as_ref().is_some_and(|object| object.etag == etag) {
            return None;
        }
        Some(Candidate {
            name,
            write: write.clone(),
            reclaim: prior.or(indexed.map(|object| object.track_address)),
        })
    }

    /// Write consecutive batchable puts in runs within the batch limits
    async fn write_run(&self, tape: Address, candidates: Vec<Candidate>) {
        let mut sizes = Vec::with_capacity(candidates.len());
        for candidate in &candidates {
            sizes.push(candidate.size());
        }
        let mut candidates = candidates.into_iter();
        for run in split_runs(&sizes) {
            let batch: Vec<Candidate> = candidates.by_ref().take(run.len()).collect();
            self.write_batch(tape, batch).await;
        }
    }

    /// One batch through the SDK pipeline; a rejected batch retries its entries one by one
    async fn write_batch(&self, tape: Address, candidates: Vec<Candidate>) {
        let mut items = Vec::with_capacity(candidates.len());
        let mut entries = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let key = candidate.name.as_bytes().to_vec();
            let Ok(Some(data)) = self.staging.bytes(tape, &key) else {
                // A row without bytes gets the single path's error.
                self.attempt(tape, &key, &candidate.write).await;
                continue;
            };
            let PendingOp::Put { content_type, etag, .. } = candidate.write.op else {
                continue;
            };
            items.push(ObjectBatchItem {
                name: candidate.name,
                data,
                content_type,
                plan: self.staging.take_plan(&etag),
            });
            entries.push((key, candidate.write, candidate.reclaim));
        }
        if items.is_empty() {
            return;
        }

        match self.write_ctx.write_objects(self.context.as_ref(), tape, items).await {
            Ok(receipts) => {
                for ((key, write, reclaim), receipt) in entries.iter().zip(receipts) {
                    self.landed(tape, key, write, receipt.address);
                    self.reclaim(tape, *reclaim, receipt.address).await;
                }
            }
            Err(error) if entries.len() == 1 || is_transient(&error) => {
                for (key, write, _) in &entries {
                    self.record_failure(tape, key, write, &error);
                }
            }
            Err(error) => {
                warn!(%error, %tape, entries = entries.len(), "s3 drain: batch rejected, writing its entries one by one");
                for (key, write, _) in &entries {
                    self.attempt(tape, key, write).await;
                }
            }
        }
    }

    /// Delete the old track after a batched overwrite lands; a failure leaves it for a later sweep
    async fn reclaim(&self, tape: Address, prior: Option<Address>, written: Address) {
        let Some(prior) = prior.filter(|prior| *prior != written) else {
            return;
        };
        if let Err(error) = self.write_ctx.delete_object(self.context.as_ref(), tape, prior).await {
            debug!(%error, %tape, %prior, "s3 drain: overwrite reclaim failed, stale track left for later reclaim");
        }
    }

    /// Drop a landed entry once the object index agrees with it.
    fn reap(&self, tape: Address, key: &[u8], write: &PendingWrite) -> bool {
        let indexed = match resolve_object(self.context.store.as_ref(), tape, key) {
            Ok(indexed) => indexed,
            Err(error) => {
                warn!(%error, %tape, "s3 drain: object index lookup failed");
                return false;
            }
        };
        let is_visible = match write.op {
            PendingOp::Put { etag, .. } => {
                indexed.is_some_and(|resolved| resolved.etag == etag)
            }
            PendingOp::Delete { .. } => indexed.is_none(),
        };
        if !is_visible {
            return false;
        }

        if let Err(error) = self.staging.remove(tape, key, write.seq) {
            warn!(%error, %tape, "s3 drain: dropping a landed entry failed");
            return false;
        }
        self.clear_retry(tape, key);
        true
    }

    /// Try the chain write for one entry, honouring its backoff.
    async fn attempt(&self, tape: Address, key: &[u8], write: &PendingWrite) -> bool {
        if self.is_paused() || !self.is_due(tape, key) {
            return false;
        }

        match self.write_chain(tape, key, write).await {
            Ok(track) => self.landed(tape, key, write, track),
            Err(error) => self.record_failure(tape, key, write, &error),
        }
        // The index has not seen the write yet, so the entry stays.
        false
    }

    /// Record that an entry's write reached the chain as `track`
    fn landed(&self, tape: Address, key: &[u8], write: &PendingWrite, track: Address) {
        self.clear_retry(tape, key);
        metrics::inc_pending_write("landed");
        // Superseded in flight: the track went to the entry that replaced it.
        if let Err(error) =
            self.staging.set_state(tape, key, write.seq, PendingState::Landed { track })
        {
            warn!(%error, %tape, "s3 drain: marking an entry landed failed");
        }
    }

    /// Perform one entry's chain operation, returning the track it settled on.
    async fn write_chain(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
    ) -> Result<Address, TapedriveError> {
        match write.op {
            PendingOp::Put { content_type, prior, etag, .. } => {
                self.put(tape, key, content_type, prior, etag).await
            }
            PendingOp::Delete { track } => self.delete(tape, key, track).await,
        }
    }

    /// Write a queued object, overwriting the track it supersedes.
    async fn put(
        &self,
        tape: Address,
        key: &[u8],
        content_type: ContentType,
        prior: Option<Address>,
        etag: Hash,
    ) -> Result<Address, TapedriveError> {
        // A queued Put with no payload is a corrupt row, not an outage.
        let bytes = self
            .staging
            .bytes(tape, key)
            .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?
            .ok_or_else(|| {
                TapedriveError::InvalidArgument(format!(
                    "queued object {} on {tape} has no stored payload",
                    String::from_utf8_lossy(key)
                ))
            })?;

        let existing = match prior {
            Some(track) => Some(track),
            None => self.indexed_track(tape, key)?,
        };
        let plan = self.staging.take_plan(&etag);
        let (_etag, track) = self
            .write_ctx
            .write_object(self.context.as_ref(), tape, key, content_type, &bytes, existing, plan)
            .await?;
        Ok(track)
    }

    /// Delete the track a queued delete names; an already-gone key is a success.
    async fn delete(
        &self,
        tape: Address,
        key: &[u8],
        landed: Option<Address>,
    ) -> Result<Address, TapedriveError> {
        let track = match landed {
            Some(track) => Some(track),
            None => self.indexed_track(tape, key)?,
        };
        let Some(track) = track else {
            return Ok(Address::default());
        };
        match self.write_ctx.delete_object(self.context.as_ref(), tape, track).await {
            Ok(()) => Ok(track),
            Err(TapedriveError::NotFound) => Ok(track),
            Err(error) => Err(error),
        }
    }

    /// The track the object index binds this key to right now.
    fn indexed_track(&self, tape: Address, key: &[u8]) -> Result<Option<Address>, TapedriveError> {
        let resolved = resolve_object(self.context.store.as_ref(), tape, key)
            .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?;
        Ok(resolved.map(|object| object.track_address))
    }

    /// Whether an entry's backoff has elapsed.
    fn is_due(&self, tape: Address, key: &[u8]) -> bool {
        let Ok(retries) = self.retries.lock() else {
            return false;
        };
        match retries.get(&(tape, key.to_vec())) {
            Some(retry) => Instant::now() >= retry.next_attempt,
            None => true,
        }
    }

    /// Record a failed attempt and back off; only permanent failures count towards parking.
    fn record_failure(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        error: &TapedriveError,
    ) {
        let is_transient = is_transient(error);
        let attempts = self.back_off(tape, key, is_transient);
        if is_transient {
            self.log_outage(tape, error);
            return;
        }

        warn!(
            %error,
            %tape,
            key = %String::from_utf8_lossy(key),
            attempts,
            "s3 drain: chain write rejected, will retry"
        );
        if attempts < MAX_ATTEMPTS {
            return;
        }
        if matches!(write.state, PendingState::Failed { .. }) {
            return;
        }
        metrics::inc_pending_write("failed");
        let state = PendingState::Failed {
            error: error.to_string(),
            attempts,
        };
        if let Err(store_error) = self.staging.set_state(tape, key, write.seq, state) {
            warn!(%store_error, %tape, "s3 drain: marking an entry failed did not stick");
        }
    }

    /// Log one bucket's outage at most once a minute.
    fn log_outage(&self, tape: Address, error: &TapedriveError) {
        let Ok(mut logged) = self.outage_logged.lock() else {
            return;
        };
        let now = Instant::now();
        match logged.get(&tape) {
            Some(last) if now.duration_since(*last) < OUTAGE_LOG_INTERVAL => return,
            Some(_) => {}
            None => {}
        }
        logged.insert(tape, now);
        warn!(%error, %tape, "s3 drain: chain unreachable, queue holding");
    }

    /// Push the next attempt out, returning the consecutive permanent-failure count.
    fn back_off(&self, tape: Address, key: &[u8], is_transient: bool) -> u32 {
        let Ok(mut retries) = self.retries.lock() else {
            return 0;
        };
        let retry = retries.entry((tape, key.to_vec())).or_insert(Retry {
            steps: 0,
            attempts: 0,
            next_attempt: Instant::now(),
        });
        retry.steps = retry.steps.saturating_add(1);
        if !is_transient {
            retry.attempts = retry.attempts.saturating_add(1);
        }
        let is_parked = !is_transient && retry.attempts >= MAX_ATTEMPTS;
        retry.next_attempt = Instant::now() + retry_delay(retry.steps, is_parked);
        retry.attempts
    }

    /// Forget an entry's retry state once it stops needing one.
    fn clear_retry(&self, tape: Address, key: &[u8]) {
        if let Ok(mut retries) = self.retries.lock() {
            retries.remove(&(tape, key.to_vec()));
        }
    }
}

/// A queued put headed for a batch
struct Candidate {
    name: String,
    write: PendingWrite,
    reclaim: Option<Address>,
}

impl Candidate {
    fn size(&self) -> u64 {
        match self.write.op {
            PendingOp::Put { size, .. } => size,
            PendingOp::Delete { .. } => 0,
        }
    }
}

/// Cut consecutive puts into runs within the batch's item and byte limits, in queue order
fn split_runs(sizes: &[u64]) -> Vec<Range<usize>> {
    let mut runs = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (index, size) in sizes.iter().enumerate() {
        let is_full = index - start == MAX_OBJECT_BATCH_ITEMS
            || bytes + size > MAX_OBJECT_BATCH_BYTES;
        if is_full && index > start {
            runs.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes += size;
    }
    if start < sizes.len() {
        runs.push(start..sizes.len());
    }
    runs
}

/// The wait before the next attempt: exponential to a minute, or the slow beat once parked.
fn retry_delay(steps: u32, is_parked: bool) -> Duration {
    if is_parked {
        return FAILED_RETRY;
    }
    let doubled = RETRY_MIN.saturating_mul(1u32 << steps.saturating_sub(1).min(6));
    doubled.min(RETRY_MAX)
}

#[cfg(test)]
mod tests {
    use tape_core::types::StorageUnits;
    use tape_sdk::error::UploadError;

    use super::*;

    // backoff doubles from a second to a minute, and a parked entry gets the slow beat
    // a burst of small puts goes out in runs of the batch's item limit
    #[test]
    fn item_runs() {
        let sizes = vec![4096; 40];

        assert_eq!(split_runs(&sizes), vec![0..32, 32..40]);
    }

    // large puts cut a run at the batch's byte limit
    #[test]
    fn byte_runs() {
        let chunk = MAX_TRACK_SIZE as u64;
        let sizes = vec![chunk, chunk, chunk, chunk, chunk, 1];

        assert_eq!(split_runs(&sizes), vec![0..4, 4..6]);
        assert!(split_runs(&[]).is_empty());
    }

    #[test]
    fn backoff_curve() {
        assert_eq!(retry_delay(1, false), Duration::from_secs(1));
        assert_eq!(retry_delay(2, false), Duration::from_secs(2));
        assert_eq!(retry_delay(5, false), Duration::from_secs(16));
        assert_eq!(retry_delay(7, false), Duration::from_secs(60));
        assert_eq!(retry_delay(99, false), RETRY_MAX, "an outage never exceeds the ceiling");
        assert_eq!(retry_delay(4, true), FAILED_RETRY);
    }

    // transport, cluster and peer failures clear on their own
    #[test]
    fn transient_errors() {
        assert!(is_transient(&TapedriveError::Rpc(RpcError::Timeout(RETRY_MIN))));
        assert!(is_transient(&TapedriveError::Rpc(RpcError::AllEndpointsFailed {
            attempts: 3
        })));
        assert!(is_transient(&TapedriveError::Rpc(RpcError::Request("502".into()))));
        assert!(is_transient(&TapedriveError::Upload(UploadError::InsufficientQuorum {
            got: 1,
            need: 14,
        })));
        assert!(is_transient(&TapedriveError::Rpc(RpcError::BlockhashExpired)));
        assert!(is_transient(&TapedriveError::NotFound));
        assert!(is_transient(&transaction_error("Blockhash not found")));
        assert!(is_transient(&transaction_error("peer was unreachable")));
    }

    // funds, capacity and authorization failures need an operator
    #[test]
    fn permanent_errors() {
        assert!(!is_transient(&TapedriveError::InsufficientCapacity {
            need: StorageUnits::from_bytes(2),
            available: StorageUnits::from_bytes(1),
        }));
        assert!(!is_transient(&TapedriveError::InvalidArgument("name too long".into())));
        assert!(!is_transient(&TapedriveError::InvalidArgument(
            "queued object a.txt has no stored payload".into()
        )));
        assert!(!is_transient(&TapedriveError::MissingPayer));
        assert!(!is_transient(&TapedriveError::Rpc(RpcError::SlotSkipped)));
        assert!(!is_transient(&transaction_error(
            "Transfer: insufficient lamports 100, need 5000"
        )));
        assert!(!is_transient(&transaction_error(
            "Error processing Instruction 0: invalid account data for instruction"
        )));
        assert!(!is_transient(&transaction_error("Allocate: account already in use")));
    }

    fn transaction_error(message: &str) -> TapedriveError {
        TapedriveError::Rpc(RpcError::Transaction {
            err: None,
            message: message.to_string(),
            simulated: false,
        })
    }
}
