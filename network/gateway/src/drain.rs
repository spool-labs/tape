//! Applies queued S3 writes to the chain, one in-flight write per bucket.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::join_all;
use tape_rpc::{Rpc, RpcError};
use tape_store_api::Store;
use tape_core::types::ContentType;
use tape_crypto::address::Address;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_protocol::Api;
use tape_sdk::error::TapedriveError;
use tape_store::types::{PendingOp, PendingState, PendingWrite};
use tokio_util::sync::CancellationToken;
use tracing::warn;

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
/// Retry delay while the chain is between writers
const CHAIN_BUSY_RETRY: Duration = Duration::from_millis(500);
/// Consecutive permanent failures before an entry is parked.
const MAX_ATTEMPTS: u32 = 3;
/// An entry waits out this many busy refusals at the floor before it joins the transient curve
const MAX_BUSY_HOLDS: u32 = 20;
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
    "invalid instruction data",
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

/// Chain messages that mean a write lease is changing hands
const CHAIN_BUSY_ERRORS: &[&str] = &["not the writer", "write token", "short of a quorum"];

/// What a failure does to an entry's next attempt
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
    /// The chain is between writers and takes the write again within milliseconds.
    Busy,
    /// Clears on its own once the cluster or a peer is back.
    Transient,
    /// Fails the same way until an operator acts.
    Permanent,
}

/// Sort one failure into the class that decides its retry
fn classify(error: &TapedriveError) -> Failure {
    match error {
        TapedriveError::Rpc(rpc_error) => classify_rpc(rpc_error),
        TapedriveError::Network(_) => Failure::Transient,
        TapedriveError::Peer(_) => Failure::Transient,
        TapedriveError::RateLimited { .. } => Failure::Transient,
        TapedriveError::Io(_) => Failure::Transient,
        TapedriveError::Certification(_) => Failure::Transient,
        TapedriveError::Upload(_) => Failure::Transient,
        TapedriveError::Download(_) => Failure::Transient,
        TapedriveError::NotFound => Failure::Transient,
        TapedriveError::InsufficientCapacity { .. } => Failure::Permanent,
        TapedriveError::WriteConflict { .. } => Failure::Permanent,
        TapedriveError::TrackPositionConflict { .. } => Failure::Permanent,
        TapedriveError::CommitmentMismatch => Failure::Permanent,
        TapedriveError::MissingPayer => Failure::Permanent,
        TapedriveError::Encoding(_) => Failure::Permanent,
        TapedriveError::InvalidArgument(_) => Failure::Permanent,
        TapedriveError::Stream(_) => Failure::Permanent,
    }
}

/// Sort one RPC failure
fn classify_rpc(error: &RpcError) -> Failure {
    match error {
        RpcError::Transaction { message, .. }
        | RpcError::Request(message)
        | RpcError::Internal(message) => classify_message(message),
        RpcError::Timeout(_) => Failure::Transient,
        RpcError::BlockNotAvailable => Failure::Transient,
        RpcError::BlockPruned => Failure::Transient,
        RpcError::AllEndpointsFailed { .. } => Failure::Transient,
        RpcError::AccountNotFound(_) => Failure::Transient,
        RpcError::TransactionNotFound(_) => Failure::Transient,
        RpcError::BlockhashExpired => Failure::Transient,
        RpcError::SlotSkipped => Failure::Permanent,
        RpcError::Deserialization(_) => Failure::Permanent,
    }
}

/// Sort a chain failure by its message; an unrecognised one stays transient
fn classify_message(message: &str) -> Failure {
    if is_chain_busy(message) {
        return Failure::Busy;
    }
    let message = message.to_lowercase();
    for permanent in PERMANENT_TRANSACTION_ERRORS {
        if message.contains(permanent) {
            return Failure::Permanent;
        }
    }
    for transient in TRANSIENT_TRANSACTION_ERRORS {
        if message.contains(transient) {
            return Failure::Transient;
        }
    }
    Failure::Transient
}

/// Whether a chain message reports a write lease changing hands
fn is_chain_busy(message: &str) -> bool {
    let message = message.to_lowercase();
    for busy in CHAIN_BUSY_ERRORS {
        if message.contains(busy) {
            return true;
        }
    }
    false
}

/// Retry state for one entry.
struct Retry {
    /// Failures of any kind, for the backoff curve.
    steps: u32,
    /// Permanent failures only; a transient one never parks the entry.
    attempts: u32,
    /// Busy refusals in a row, so a chain with no writer stops getting 2 Hz retries.
    busy_holds: u32,
    next_attempt: Instant,
}

impl Retry {
    /// Fresh state for an entry with nothing held against it.
    fn new() -> Self {
        Self {
            steps: 0,
            attempts: 0,
            busy_holds: 0,
            next_attempt: Instant::now(),
        }
    }

    /// Push the next attempt out for one failure, returning the permanent-failure count.
    fn back_off(&mut self, failure: Failure) -> u32 {
        match failure {
            Failure::Busy => {
                self.busy_holds = self.busy_holds.saturating_add(1);
                if self.busy_holds <= MAX_BUSY_HOLDS {
                    self.next_attempt = Instant::now() + CHAIN_BUSY_RETRY;
                    return self.attempts;
                }
                self.steps = self.steps.saturating_add(1);
            }
            Failure::Transient => {
                self.busy_holds = 0;
                self.steps = self.steps.saturating_add(1);
            }
            Failure::Permanent => {
                self.busy_holds = 0;
                self.steps = self.steps.saturating_add(1);
                self.attempts = self.attempts.saturating_add(1);
            }
        }
        let is_parked = matches!(failure, Failure::Permanent) && self.attempts >= MAX_ATTEMPTS;
        self.next_attempt = Instant::now() + retry_delay(self.steps, is_parked);
        self.attempts
    }
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

/// One retry state per queued entry, keyed by bucket and key
#[derive(Default)]
struct RetryTable {
    entries: Mutex<HashMap<(Address, Vec<u8>), Retry>>,
}

impl RetryTable {
    /// Push the next attempt out, returning the consecutive permanent-failure count
    fn back_off(&self, tape: Address, key: &[u8], failure: Failure) -> u32 {
        let Ok(mut entries) = self.entries.lock() else {
            return 0;
        };
        entries
            .entry((tape, key.to_vec()))
            .or_insert_with(Retry::new)
            .back_off(failure)
    }

    /// Forget an entry's retry state once it stops needing one
    fn clear(&self, tape: Address, key: &[u8]) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.remove(&(tape, key.to_vec()));
        }
    }

    /// Whether an entry's backoff has elapsed
    fn is_due(&self, tape: Address, key: &[u8]) -> bool {
        let Ok(entries) = self.entries.lock() else {
            return false;
        };
        match entries.get(&(tape, key.to_vec())) {
            Some(retry) => Instant::now() >= retry.next_attempt,
            None => true,
        }
    }
}

/// Drains the durable S3 write queue onto the chain.
pub struct WriteDrain<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    write_ctx: Arc<S3WriteContext>,
    staging: Arc<StagingStore<Db>>,
    retries: RetryTable,
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
            retries: RetryTable::default(),
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
        for (key, write) in entries {
            if self.cancel.is_cancelled() {
                remaining += 1;
                continue;
            }
            if !self.apply(tape, &key, &write).await {
                remaining += 1;
            }
        }
        remaining
    }

    /// Move one entry along, returning whether it left the queue.
    async fn apply(&self, tape: Address, key: &[u8], write: &PendingWrite) -> bool {
        match write.state {
            PendingState::Landed { .. } => self.reap(tape, key, write),
            PendingState::Queued => self.attempt(tape, key, write).await,
            PendingState::Failed { .. } => self.attempt(tape, key, write).await,
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
        self.retries.clear(tape, key);
        true
    }

    /// Try the chain write for one entry, honouring its backoff.
    async fn attempt(&self, tape: Address, key: &[u8], write: &PendingWrite) -> bool {
        if self.is_paused() || !self.retries.is_due(tape, key) {
            return false;
        }

        match self.write_chain(tape, key, write).await {
            Ok(track) => {
                self.retries.clear(tape, key);
                metrics::inc_pending_write("landed");
                // Superseded in flight: the track went to the entry that replaced it.
                if let Err(error) =
                    self.staging.set_state(tape, key, write.seq, PendingState::Landed { track })
                {
                    warn!(%error, %tape, "s3 drain: marking an entry landed failed");
                }
                // The index has not seen the write yet, so the entry stays.
                false
            }
            Err(error) => {
                self.record_failure(tape, key, write, &error);
                false
            }
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
            PendingOp::Put { content_type, prior, .. } => {
                self.put(tape, key, content_type, prior).await
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
        let (_etag, track) = self
            .write_ctx
            .write_object(self.context.as_ref(), tape, key, content_type, &bytes, existing)
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

    /// Record a failed attempt and back off; only permanent failures count towards parking.
    fn record_failure(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        error: &TapedriveError,
    ) {
        let failure = classify(error);
        let attempts = self.retries.back_off(tape, key, failure);
        match failure {
            Failure::Busy | Failure::Transient => {
                self.log_outage(tape, error);
                return;
            }
            Failure::Permanent => {}
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
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Timeout(RETRY_MIN))),
            Failure::Transient
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::AllEndpointsFailed { attempts: 3 })),
            Failure::Transient
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Request("502".into()))),
            Failure::Transient
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Request("connection reset".into()))),
            Failure::Transient
        );
        assert_eq!(
            classify(&TapedriveError::Upload(UploadError::InsufficientQuorum {
                got: 1,
                need: 14,
            })),
            Failure::Transient
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::BlockhashExpired)),
            Failure::Transient
        );
        assert_eq!(classify(&TapedriveError::NotFound), Failure::Transient);
        assert_eq!(classify(&transaction_error("Blockhash not found")), Failure::Transient);
        assert_eq!(classify(&transaction_error("peer was unreachable")), Failure::Transient);
    }

    // funds, capacity and authorization failures need an operator
    #[test]
    fn permanent_errors() {
        assert_eq!(
            classify(&TapedriveError::InsufficientCapacity {
                need: StorageUnits::from_bytes(2),
                available: StorageUnits::from_bytes(1),
            }),
            Failure::Permanent
        );
        assert_eq!(
            classify(&TapedriveError::InvalidArgument("name too long".into())),
            Failure::Permanent
        );
        assert_eq!(
            classify(&TapedriveError::InvalidArgument(
                "queued object a.txt has no stored payload".into()
            )),
            Failure::Permanent
        );
        assert_eq!(classify(&TapedriveError::MissingPayer), Failure::Permanent);
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::SlotSkipped)),
            Failure::Permanent
        );
        assert_eq!(
            classify(&transaction_error("Transfer: insufficient lamports 100, need 5000")),
            Failure::Permanent
        );
        assert_eq!(
            classify(&transaction_error(
                "Error processing Instruction 0: invalid account data for instruction"
            )),
            Failure::Permanent
        );
        assert_eq!(
            classify(&transaction_error("Allocate: account already in use")),
            Failure::Permanent
        );
    }

    // the chain refuses its own writes while its lease flaps, then takes them milliseconds later
    #[test]
    fn busy_errors() {
        assert_eq!(
            classify(&transaction_error("the log is short of a quorum")),
            Failure::Busy
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Internal(
                "nobody holds the write token".into()
            ))),
            Failure::Busy
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Request(
                "RPC request failed: not the writer".into()
            ))),
            Failure::Busy
        );
        assert_eq!(
            classify(&transaction_error("Transfer: insufficient funds 1, need 5000")),
            Failure::Permanent
        );
        assert_eq!(classify(&transaction_error("request timed out")), Failure::Transient);
    }

    // a program refusal reaching the drain untyped still parks the entry
    #[test]
    fn program_refusal_parks() {
        let refusal = "RPC error: RPC request failed: RPC response error -32002: Error processing \
                       Instruction 0: invalid instruction data; Program 4iG5 failed: invalid \
                       instruction data;  tape=7vQf";

        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Request(refusal.into()))),
            Failure::Permanent
        );
        assert_eq!(
            classify(&TapedriveError::Rpc(RpcError::Internal(
                "Transfer: insufficient funds 1, need 5000".into()
            ))),
            Failure::Permanent
        );
    }

    // a busy chain holds an entry at the floor, and the next transient failure starts the curve
    #[test]
    fn busy_holds_the_curve() {
        let mut retry = Retry::new();
        for _ in 0..3 {
            assert_eq!(retry.back_off(Failure::Busy), 0);
        }
        assert_eq!(retry.steps, 0, "a busy chain leaves the curve alone");
        assert_eq!(retry.attempts, 0, "a busy chain never parks an entry");
        let busy_wait = retry.next_attempt.saturating_duration_since(Instant::now());
        assert!(busy_wait <= CHAIN_BUSY_RETRY);

        retry.back_off(Failure::Transient);
        assert_eq!(retry.steps, 1);
        assert_eq!(retry.attempts, 0);
        let transient_wait = retry.next_attempt.saturating_duration_since(Instant::now());
        assert!(transient_wait > CHAIN_BUSY_RETRY && transient_wait <= RETRY_MIN);
    }

    // a chain with no writer stops holding an entry at the floor, and a landed write starts it over
    #[test]
    fn busy_escalates() {
        let table = RetryTable::default();
        let tape = Address::new_unique();
        let key = b"objects/a.txt".as_slice();

        for _ in 0..MAX_BUSY_HOLDS {
            assert_eq!(table.back_off(tape, key, Failure::Busy), 0);
        }
        assert_eq!(steps(&table, tape, key), 0, "the last hold stays at the floor");
        assert!(wait(&table, tape, key) <= CHAIN_BUSY_RETRY);
        assert!(!table.is_due(tape, key), "a held entry waits out the floor");

        table.back_off(tape, key, Failure::Busy);
        assert_eq!(steps(&table, tape, key), 1, "one past the ceiling joins the curve");
        assert_eq!(attempts(&table, tape, key), 0, "a busy chain never parks an entry");
        let escalated = wait(&table, tape, key);
        assert!(escalated > CHAIN_BUSY_RETRY && escalated <= RETRY_MIN);

        // A landed write drops the entry's retry state, so the next refusal is
        // held at the floor again.
        table.clear(tape, key);
        assert!(table.is_due(tape, key), "a cleared entry is due at once");

        assert_eq!(table.back_off(tape, key, Failure::Busy), 0);
        assert_eq!(steps(&table, tape, key), 0);
        assert!(wait(&table, tape, key) <= CHAIN_BUSY_RETRY);
    }

    /// The backoff curve position the table holds for one entry
    fn steps(table: &RetryTable, tape: Address, key: &[u8]) -> u32 {
        with_entry(table, tape, key, |retry| retry.steps)
    }

    /// The parking count the table holds for one entry
    fn attempts(table: &RetryTable, tape: Address, key: &[u8]) -> u32 {
        with_entry(table, tape, key, |retry| retry.attempts)
    }

    /// How long the table still holds one entry back
    fn wait(table: &RetryTable, tape: Address, key: &[u8]) -> Duration {
        with_entry(table, tape, key, |retry| {
            retry.next_attempt.saturating_duration_since(Instant::now())
        })
    }

    fn with_entry<Value>(
        table: &RetryTable,
        tape: Address,
        key: &[u8],
        read: impl FnOnce(&Retry) -> Value,
    ) -> Value {
        let entries = table.entries.lock().expect("retry table");
        let retry = entries.get(&(tape, key.to_vec())).expect("entry held");
        read(retry)
    }

    fn transaction_error(message: &str) -> TapedriveError {
        TapedriveError::Rpc(RpcError::Transaction {
            err: None,
            message: message.to_string(),
            simulated: false,
        })
    }
}
