//! Applies queued S3 writes to the chain, one in-flight write per bucket.
//!
//! A bucket's tape account admits one write per block, so the S3 surface
//! acknowledges a write once it is durable in the queue and this service applies
//! it afterwards. Buckets drain in parallel; one bucket's entries go in the order
//! they were queued.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::join_all;
use rpc::{Rpc, RpcError};
use store::Store;
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
/// Consecutive permanent failures before an entry is marked failed; it is still
/// retried after that, because funds and capacity can be topped up.
const MAX_ATTEMPTS: u32 = 3;
/// How often one bucket's transient outage is logged, however many entries hit it.
const OUTAGE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// How often the delegate's SOL balance is read.
const BALANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Balance below which chain attempts pause: a payer under the rent floor gets
/// its transactions dropped, which reaches the drain as a timeout rather than as
/// the funding problem it is.
const DELEGATE_LAMPORTS_FLOOR: u64 = 50_000_000;

/// Program errors that mean the write will fail the same way until an operator
/// acts, matched on the transaction message the runtime returned.
const PERMANENT_TRANSACTION_ERRORS: &[&str] = &[
    "insufficient funds",
    "insufficient lamports",
    "insufficient capacity",
    "account already in use",
    "invalid account data for instruction",
];

/// Transport and cluster failures that clear on their own, matched on text the
/// typed variants do not separate.
const TRANSIENT_TRANSACTION_ERRORS: &[&str] = &[
    "blockhash",
    "unreachable",
    "connection",
    "timed out",
    "timeout",
    "quorum",
];

/// Whether an error will clear on its own once the cluster or a peer is back.
///
/// A transient error never marks an entry failed: the moment the chain is back
/// the queue has to move at the backoff ceiling, not once every five minutes.
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
        RpcError::AllEndpointsFailed { .. } => true,
        RpcError::AccountNotFound(_) => true,
        RpcError::TransactionNotFound(_) => true,
        RpcError::BlockhashExpired => true,
        RpcError::Internal(_) => true,
        RpcError::Deserialization(_) => false,
    }
}

/// Classify a transaction failure by the message the runtime returned.
///
/// A message naming a permanent program error wins; anything else is treated as
/// transient, so an unrecognised failure keeps retrying rather than parking.
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

/// What the drain remembers between passes about a struggling entry.
struct Retry {
    /// Failures of any kind, driving the exponential backoff.
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

/// The drain's health, published for the admin control plane to read.
#[derive(Default)]
pub struct DrainStatus {
    health: Mutex<DrainHealth>,
}

impl DrainStatus {
    /// An empty status, which is what the admin plane sees with no drain running.
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
    /// When each bucket's transient outage was last logged, so an outage costs
    /// one line a minute rather than one per attempt.
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

    /// Read the delegate's balance, publish it, and hold chain attempts while it
    /// is under the floor.
    ///
    /// A payer below the rent floor has its transactions silently dropped, so
    /// attempting anyway burns the backoff and reports timeouts that say nothing
    /// about the real cause.
    async fn read_delegate_balance(&self) {
        let delegate = self.write_ctx.delegate_address();
        let lamports = match self.context.rpc.rpc().get_account(&delegate).await {
            Ok(account) => account.lamports,
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
        self.clear_retry(tape, key);
        true
    }

    /// Try the chain write for one entry, honouring its backoff.
    async fn attempt(&self, tape: Address, key: &[u8], write: &PendingWrite) -> bool {
        if self.is_paused() || !self.is_due(tape, key) {
            return false;
        }

        match self.write_chain(tape, key, write).await {
            Ok(track) => {
                self.clear_retry(tape, key);
                metrics::inc_pending_write("landed");
                // A false return means the client replaced this write while it
                // was in flight; the track went to the replacement instead.
                if let Err(error) =
                    self.staging.set_state(tape, key, write.seq, PendingState::Landed { track })
                {
                    warn!(%error, %tape, "s3 drain: marking an entry landed failed");
                }
                // The index has not seen the write yet, so the entry stays until
                // the next pass reaps it.
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

    /// Write a queued object, resuming or overwriting whatever it supersedes.
    ///
    /// `prior` is the track a superseded write landed, which the index may not
    /// show yet; without it the track comes from the index, resolved now rather
    /// than at enqueue so an overwrite reclaims what the name binds at the moment
    /// the write actually goes out.
    async fn put(
        &self,
        tape: Address,
        key: &[u8],
        content_type: ContentType,
        prior: Option<Address>,
    ) -> Result<Address, TapedriveError> {
        let bytes = self
            .staging
            .bytes(tape, key)
            .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?
            .ok_or(TapedriveError::NotFound)?;

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
    ///
    /// `landed` is the track a superseded Put wrote, which the index may not show
    /// yet; without it the track comes from the index.
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

    /// Record a failed attempt and back off.
    ///
    /// A transient failure keeps the entry queued and the backoff capped, so the
    /// bucket resumes at full speed the moment the cluster is back. A permanent
    /// one parks the entry after a few tries; it is still retried on the slow
    /// beat, and still served to readers.
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
    ///
    /// A transient failure backs off on the same curve but does not count towards
    /// parking the entry.
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

/// The wait before the next attempt: exponential from a second to a minute, or
/// the slow steady beat once the entry has been parked.
///
/// A transient failure never reaches the slow beat, however long the outage runs,
/// so a bucket resumes at the ceiling the moment the cluster is back.
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
        assert!(!is_transient(&TapedriveError::MissingPayer));
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
