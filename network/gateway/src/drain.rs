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
use rpc::Rpc;
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
/// Attempts before an entry is marked failed; it is still retried after that.
const MAX_ATTEMPTS: u32 = 10;

/// What the drain remembers between passes about a struggling entry.
struct Retry {
    attempts: u32,
    next_attempt: Instant,
}

/// Drains the durable S3 write queue onto the chain.
pub struct WriteDrain<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    write_ctx: Arc<S3WriteContext>,
    staging: Arc<StagingStore<Db>>,
    retries: Mutex<HashMap<(Address, Vec<u8>), Retry>>,
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
        cancel: CancellationToken,
    ) -> Self {
        Self {
            context,
            write_ctx,
            staging,
            retries: Mutex::new(HashMap::new()),
            cancel,
        }
    }

    /// Drain until cancelled, waking on each enqueue and ticking regardless.
    pub async fn run(self) -> Result<(), NodeError> {
        loop {
            tokio::select! {
                // Safe: cancellation carries no partial state.
                _ = self.cancel.cancelled() => return Ok(()),
                // Safe: a missed notification is picked up by the next tick.
                _ = self.staging.queued() => {}
                // Safe: sleep is cancellation-safe.
                _ = tokio::time::sleep(DRAIN_TICK) => {}
            }
            self.pass().await;
        }
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
        if !self.is_due(tape, key) {
            return false;
        }

        match self.write_chain(tape, key, write).await {
            Ok(track) => {
                self.clear_retry(tape, key);
                metrics::inc_pending_write("landed");
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
            PendingOp::Put { content_type, .. } => self.put(tape, key, content_type).await,
            PendingOp::Delete { track } => self.delete(tape, key, track).await,
        }
    }

    /// Write a queued object, resuming or overwriting whatever the index binds.
    async fn put(
        &self,
        tape: Address,
        key: &[u8],
        content_type: ContentType,
    ) -> Result<Address, TapedriveError> {
        let bytes = self
            .staging
            .bytes(tape, key)
            .map_err(|error| TapedriveError::InvalidArgument(error.to_string()))?
            .ok_or(TapedriveError::NotFound)?;

        // Resolved now rather than at enqueue: an overwrite has to reclaim the
        // track the index binds at the moment the write actually goes out.
        let existing = self.indexed_track(tape, key)?;
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

    /// Record a failed attempt, backing off and marking the entry failed once it
    /// has run out of attempts. It is still retried and still served to readers.
    fn record_failure(
        &self,
        tape: Address,
        key: &[u8],
        write: &PendingWrite,
        error: &TapedriveError,
    ) {
        let attempts = self.back_off(tape, key);
        warn!(
            %error,
            %tape,
            key = %String::from_utf8_lossy(key),
            attempts,
            "s3 drain: chain write failed, will retry"
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

    /// Bump the attempt count and push the next attempt out, returning the count.
    fn back_off(&self, tape: Address, key: &[u8]) -> u32 {
        let Ok(mut retries) = self.retries.lock() else {
            return 0;
        };
        let retry = retries.entry((tape, key.to_vec())).or_insert(Retry {
            attempts: 0,
            next_attempt: Instant::now(),
        });
        retry.attempts = retry.attempts.saturating_add(1);
        retry.next_attempt = Instant::now() + retry_delay(retry.attempts);
        retry.attempts
    }

    /// Forget an entry's retry state once it stops needing one.
    fn clear_retry(&self, tape: Address, key: &[u8]) {
        if let Ok(mut retries) = self.retries.lock() {
            retries.remove(&(tape, key.to_vec()));
        }
    }
}

/// The wait before attempt `attempts + 1`: exponential to a ceiling while an
/// entry is still retryable, then a slow steady beat once it has failed.
fn retry_delay(attempts: u32) -> Duration {
    if attempts >= MAX_ATTEMPTS {
        return FAILED_RETRY;
    }
    let doubled = RETRY_MIN.saturating_mul(1u32 << attempts.saturating_sub(1).min(6));
    doubled.min(RETRY_MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    // backoff doubles from a second to a minute, then settles at the failed beat
    #[test]
    fn backoff_curve() {
        assert_eq!(retry_delay(1), Duration::from_secs(1));
        assert_eq!(retry_delay(2), Duration::from_secs(2));
        assert_eq!(retry_delay(5), Duration::from_secs(16));
        assert_eq!(retry_delay(7), Duration::from_secs(60));
        assert_eq!(retry_delay(MAX_ATTEMPTS), FAILED_RETRY);
    }
}
