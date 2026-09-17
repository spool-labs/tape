//! Catching blacklists up from the store, then applying each persisted block

use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt, TryStreamExt};
use tape_rpc::Rpc;
use store::Store;
use tape_blacklist::blacklists::Changes;
use tape_blacklist::expiry::is_near_expiry;
use tape_blacklist::extract::{Change, change};
use tape_core::track::data::BlobData;
use tape_core::types::EpochNumber;
use tape_node::context::NodeContext;
use tape_node::core::error::NodeError;
use tape_node::features::replay::types::ReplayBatch;
use tape_protocol::Api;
use tape_protocol::inline::fetch_inline_from_peers;
use tape_rack::reach::TrackRef;
use tape_retry::{Backoff, RetryConfig, backoff_or_cancel};
use tape_store::ops::{TapeOps, TrackDataOps, TrackOps};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::denials::GatewayBlacklists;
use super::purge::purge_denied;
use super::source::StoreSource;
use crate::cache::GatewaySliceCache;
use crate::metrics::{set_blacklist_denied, set_blacklist_health};

/// Tracks fetched from peers at once
const FETCHES: usize = 8;

/// How often expiring tapes and skipped tracks are reported
const REPORT_INTERVAL: Duration = Duration::from_secs(600);

/// Applies followed blacklists to a gateway and purges what they deny
pub struct BlacklistFollow<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    slice_cache: Arc<GatewaySliceCache<Db>>,
    blacklists: Arc<GatewayBlacklists>,
    /// The epoch the store has been replayed to, which decides which tapes have expired
    epoch: EpochNumber,
    reported_at: Option<Instant>,
}

impl<Db, Cluster, Blockchain> BlacklistFollow<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        slice_cache: Arc<GatewaySliceCache<Db>>,
        blacklists: Arc<GatewayBlacklists>,
    ) -> Self {
        let epoch = context.state().epoch();
        Self {
            context,
            slice_cache,
            blacklists,
            epoch,
            reported_at: None,
        }
    }

    /// Read every list from the store and fetch the bytes it lacks, then open reads
    ///
    /// Returns without opening reads when cancelled.
    pub async fn catch_up(&mut self, cancel: &CancellationToken) -> Result<(), NodeError> {
        self.epoch = self.context.state().epoch();
        let changes = self.blacklists.rebuild(&self.source())?;
        self.settle(&changes)?;
        if !self.fetch_missing(cancel).await? {
            return Ok(());
        }

        self.blacklists.mark_caught_up();
        let denied = self.blacklists.denied_count();
        set_blacklist_denied(denied);
        info!(denied, "blacklists caught up");
        Ok(())
    }

    /// Apply what a persisted block changed on the tapes the lists read
    pub async fn apply_block(&mut self, batch: &ReplayBatch, cancel: &CancellationToken) -> Result<(), NodeError> {
        let mut changes = Vec::new();
        for record in &batch.records {
            let Some(change) = change(record) else {
                continue;
            };
            if let Change::Epoch { epoch } = change {
                self.epoch = epoch;
            }
            if self.matters(&change) {
                changes.push(change);
            }
        }

        if !changes.is_empty() {
            let applied = self.blacklists.apply(&changes, &self.source())?;
            self.settle(&applied)?;
            self.fetch_missing(cancel).await?;
        }
        self.drop_denied_writes(batch)
    }

    /// Warn about reached tapes near expiry and skipped tracks, at most once per interval
    pub fn report(&mut self) {
        if self.reported_at.is_some_and(|at| at.elapsed() < REPORT_INTERVAL) {
            return;
        }
        self.reported_at = Some(Instant::now());

        let mut near_expiry = 0;
        for tape in self.blacklists.reached_tapes() {
            match self.context.store.get_tape(tape) {
                Ok(Some(info)) if is_near_expiry(info.end_epoch, self.epoch) => {
                    near_expiry += 1;
                    warn!(%tape, end_epoch = %info.end_epoch, "followed blacklist tape is close to expiry");
                }
                Ok(Some(_)) => {}
                Ok(None) => warn!(%tape, "followed blacklist tape is not in the store"),
                Err(error) => warn!(%error, %tape, "reading a followed blacklist tape failed"),
            }
        }
        let skipped = self.blacklists.skipped();
        if let Some((name, latest)) = skipped.last() {
            warn!(
                count = skipped.len(),
                %name,
                tape = %latest.track.tape,
                track_number = %latest.track.track_number,
                reason = %latest.reason,
                "blacklists skipped tracks they could not use"
            );
        }
        set_blacklist_health(near_expiry, skipped.len());
    }

    fn source(&self) -> StoreSource<'_, Db> {
        StoreSource::new(self.context.store.as_ref(), self.epoch)
    }

    /// Whether a change can matter to a followed list
    fn matters(&self, change: &Change) -> bool {
        match change {
            Change::Written { tape, .. } | Change::Tape { tape } => self.blacklists.watches(*tape),
            Change::Removed { .. } | Change::Epoch { .. } => true,
        }
    }

    /// Fetch every track some list still needs, until none is missing
    ///
    /// Answers false when cancelled first.
    async fn fetch_missing(&self, cancel: &CancellationToken) -> Result<bool, NodeError> {
        loop {
            let missing = self.blacklists.missing();
            if missing.is_empty() {
                return Ok(true);
            }
            let fetched: Vec<Option<Change>> = stream::iter(missing)
                .map(|track| self.fetch_track(track, cancel))
                .buffer_unordered(FETCHES)
                .try_collect()
                .await?;
            if fetched.iter().any(Option::is_none) {
                return Ok(false);
            }
            let changes: Vec<Change> = fetched.into_iter().flatten().collect();
            let applied = self.blacklists.apply(&changes, &self.source())?;
            self.settle(&applied)?;
        }
    }

    /// Fetch one track's bytes from its group's peers and store them
    ///
    /// Answers the change to report, or `None` when cancelled first.
    async fn fetch_track(&self, reference: TrackRef, cancel: &CancellationToken) -> Result<Option<Change>, NodeError> {
        let Some(track) = self.context.store.get_track(reference.address).map_err(store_error)? else {
            return Ok(Some(Change::Removed { track: reference.address }));
        };
        let mut backoff = Backoff::new(RetryConfig::infinite());
        loop {
            let peers = self.context.state().group_peers(track.group);
            let fetched =
                fetch_inline_from_peers(self.context.api.as_ref(), &peers, reference.address, track.value_hash).await;
            match fetched {
                Ok(payload) => {
                    self.context
                        .store
                        .put_track_data(reference.address, BlobData::Inline(payload.bytes))
                        .map_err(store_error)?;
                    return Ok(Some(Change::Written {
                        tape: reference.tape,
                        track_number: reference.track_number,
                    }));
                }
                Err(error) => {
                    if backoff.attempt() == 0 {
                        warn!(%error, track = %reference.address, "blacklist track unavailable; retrying");
                    } else {
                        debug!(%error, track = %reference.address, "blacklist track still unavailable");
                    }
                    if backoff_or_cancel(&mut backoff, cancel).await {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Delete content a block wrote to tracks the lists already deny
    fn drop_denied_writes(&self, batch: &ReplayBatch) -> Result<(), NodeError> {
        if self.blacklists.denied_count() == 0 {
            return Ok(());
        }
        for raw in &batch.raw_tracks {
            let Some(track) = self.context.store.get_track(raw.track).map_err(store_error)? else {
                continue;
            };
            if self.blacklists.denies(raw.track, track.tape) {
                self.context.store.delete_track_data(raw.track).map_err(store_error)?;
            }
        }
        Ok(())
    }

    fn settle(&self, changes: &Changes) -> Result<(), NodeError> {
        if changes.denied.is_empty() && changes.allowed.is_empty() {
            return Ok(());
        }
        info!(
            denied = changes.denied.len(),
            allowed = changes.allowed.len(),
            "blacklist changes applied"
        );
        purge_denied(&self.context.store, &self.slice_cache, changes).map_err(store_error)?;
        set_blacklist_denied(self.blacklists.denied_count());
        Ok(())
    }
}

fn store_error(error: impl Display) -> NodeError {
    NodeError::Store(error.to_string())
}
