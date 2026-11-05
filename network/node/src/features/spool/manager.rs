//! The spool manager is the part of the node that makes sure the node has all
//! of the data for every spool it owns. A node can be missing some of that
//! data after it takes over a spool from another node, or when part of an
//! upload never reached it. For each spool, the manager keeps a list on disk
//! of the tracks whose data we don't have yet, the missing_list, and works
//! through it a short stretch at a time.
//!
//! ```text
//! every second, and whenever a fetch finishes:
//!   record what each finished fetch got and what failed
//!   if a new epoch has started, note which spools we gained and lost
//!   once we are caught up with the chain:
//!     take a turn on each of the next few spools we own
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tape_core::spooler::GroupFold;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_protocol::{Api, ProtocolState};
use tape_recover::attempt::{AttemptFailure, Method};
use tape_recover::fetch::{FetchPool, FetchRes, FetchTrack};
use tape_recover::limits::SpoolLimits;
use tape_recover::peer::PeerScores;
use tape_recover::time::WallTime;
use tape_recover::track::MissingTrack;
use tape_rpc::Rpc;
use tape_store::ops::SpoolOps;
use tape_store_api::Store;
use tokio::task::{Id, JoinError, JoinSet};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::ingest::SLOT_MS;
use crate::features::spool::advance::{
    get_owned_live_spools, start_new_epoch,
};
use crate::features::spool::error::SpoolError;
use crate::features::spool::history::record_epoch_owners;
use crate::features::store::apply::REPAIR_GRACE_SLOTS;

/// The spool manager's one task
pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    pub context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    pub limits: SpoolLimits,
    pub upload_wait: Duration,
    pub cancel: CancellationToken,
    pub peer_scores: PeerScores,
    pub fetch_pool: FetchPool<Id>,
    pub tasks: JoinSet<FetchRes>,
    pub cursors: HashMap<SpoolIndex, MissingTrack>,
    pub epoch: Option<EpochNumber>,
    pub next_spool: usize,
}

impl<Db, Cluster, Blockchain> SpoolManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    /// A spool manager with no fetches running and no epoch seen yet
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: RecoveryConfig,
        cancel: CancellationToken,
    ) -> Self {

        let limits = SpoolLimits::from(&config);
        let upload_wait = get_upload_wait(&context);
        let peer_scores = PeerScores::default();
        let fetch_pool = FetchPool::default();
        let tasks = JoinSet::new();
        let cursors = HashMap::new();

        Self {
            context,
            limits,
            upload_wait,
            cancel,
            peer_scores,
            fetch_pool,
            tasks,
            cursors,
            epoch: None,
            next_spool: 0,
        }
    }

    /// Wake when a fetch finishes or the wake time passes, until cancelled
    pub async fn run(mut self) -> Result<(), NodeError> {
        let wake = self.limits.wake;
        loop {
            tokio::select! {
                // Safe: cancelled holds no state
                _ = self.cancel.cancelled() => break,

                // Safe: join_next only takes a fetch that has finished
                Some(joined) = self.tasks.join_next_with_id() => {
                    self.record_finished_fetch(joined);
                }

                // Safe: a sleep dropped early loses nothing
                _ = sleep(wake) => {}
            }
            self.wake();
        }
        self.tasks.shutdown().await;
        Ok(())
    }

    /// Record the finished fetches, start the new epoch when the epoch
    /// changed, and take turns while our node is at the tip
    fn wake(&mut self) {
        self.record_finished_fetches();
        let state = self.context.state();
        let Some(fold) = state.fold() else {
            return;
        };
        if self.epoch != Some(state.epoch()) {
            if let Err(error) = self.start_epoch(&state) {
                warn!(%error, "spool: epoch turn failed");
                return;
            }
        }
        if self.context.is_at_tip() {
            self.take_turns(&state, fold);
        }
    }

    /// Record every fetch that has finished since the last wake
    pub fn record_finished_fetches(&mut self) {
        while let Some(joined) = self.tasks.try_join_next_with_id() {
            self.record_finished_fetch(joined);
        }
    }

    /// A fetch ended: let go of what it held, and record its result
    fn record_finished_fetch(
        &mut self,
        joined: Result<(Id, FetchRes), JoinError>,
    ) {
        let id = match &joined {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        if let Some(req) = self.fetch_pool.remove(id) {
            for peer in &req.peers {
                self.peer_scores.finish_fetch(peer.peer);
            }
        }
        match joined {
            Ok((_, res)) => self.record_fetch_result(res),
            Err(error) => warn!(%error, "spool: fetch ended without a result"),
        }
    }

    /// Update each peer's score, then each track's missing track
    pub fn record_fetch_result(&mut self, res: FetchRes) {
        let now = WallTime::now();
        for (peer, outcome) in &res.peers {
            let scores = &mut self.peer_scores;
            scores.record_peer_outcome(*peer, *outcome, now, &self.limits);
        }
        let method = res.req.method;
        for (address, outcome) in &res.tracks {
            let Some(track) = res.req.get_track(*address) else {
                continue;
            };
            let recorded =
                self.record_track_result(method, track, *outcome, now);
            if let Err(error) = recorded {
                warn!(%address, %error, "spool: missing track write failed");
            }
        }
        let tracks = res.tracks.len();
        let peers = &res.peers;
        debug!(?method, ?peers, tracks, "spool: fetch finished");
    }

    /// A track that got what it lacked leaves the missing_list; any other
    /// records its failure
    fn record_track_result(
        &self,
        method: Method,
        track: &FetchTrack,
        outcome: Result<(), AttemptFailure>,
        now: WallTime,
    ) -> Result<(), SpoolError> {
        let store = &self.context.store;
        let mut missing = track.missing_track;
        let failure = match outcome {
            Ok(()) => return Ok(store.delete_missing_track(&missing)?),
            Err(failure) => failure,
        };
        missing.record_failure(method, failure, track.holder_epoch, now);
        Ok(store.put_missing_track(&missing)?)
    }

    /// At start, record last epoch's owners; then start the new epoch, and
    /// forget the cursors of spools no longer owned
    fn start_epoch(&mut self, state: &ProtocolState) -> Result<(), SpoolError> {
        let context = &self.context;
        let profile = context.params().profile;
        if self.epoch.is_none() {
            if let Some(previous) = &state.previous {
                record_epoch_owners(&context.store, previous, profile)?;
            }
        }
        let scores = &mut self.peer_scores;
        start_new_epoch(context, state, scores, &self.limits)?;

        let me = context.node_address();
        let owned = get_owned_live_spools(state, me);
        let owned: HashSet<SpoolIndex> = owned.into_iter().collect();
        let is_owned = |spool: &SpoolIndex, _: &mut MissingTrack| {
            owned.contains(spool)
        };
        self.cursors.retain(is_owned);
        self.epoch = Some(state.epoch());
        Ok(())
    }

    /// Up to the most turns, round robin over the spools we own; none once
    /// the fetch_pool is full
    pub fn take_turns(&mut self, state: &ProtocolState, fold: GroupFold) {
        self.upload_wait = get_upload_wait(&self.context);
        let me = self.context.node_address();
        let spools = get_owned_live_spools(state, me);
        if spools.is_empty() {
            return;
        }
        for _ in 0..self.limits.turns_per_wake {
            if self.fetch_pool.is_full(&self.limits) {
                return;
            }
            let spool = spools[self.next_spool % spools.len()];
            self.next_spool = self.next_spool.wrapping_add(1);
            if let Err(error) = self.take_turn(state.epoch(), fold, spool) {
                warn!(%spool, %error, "spool: turn failed");
            }
        }
    }
}

/// After the upload, the uploader's time to deliver, at the network's
/// measured slot length
pub fn get_upload_wait<Db: Store, Cluster: Api, Blockchain: Rpc>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Duration {
    let progress = context.ingest.progress();
    get_grace_wait(progress.slot_ms().unwrap_or(SLOT_MS))
}

/// The upload's grace slots at a slot length
fn get_grace_wait(slot_ms: u64) -> Duration {
    Duration::from_millis(REPAIR_GRACE_SLOTS.0.saturating_mul(slot_ms))
}

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use tape_core::spooler::GroupIndex;
    use tape_crypto::address::Address;
    use tape_recover::attempt::Attempt;
    use tape_recover::state::is_ready_to_attest;
    use tape_store::ops::TrackDataOps;
    use tape_store_memory::MemoryStore;

    use super::*;
    use crate::features::spool::fixture::{
        at_tip, hold_before, lacking, Lacking, Peers,
    };

    type TestManager = SpoolManager<MemoryStore, MemoryApi, LiteSvmRpc>;

    const HOLDER: Address = Address::new([0xA1; 32]);

    fn manager(lacking: &Lacking) -> TestManager {
        let ctx = Arc::clone(&lacking.ctx);
        let config = RecoveryConfig::default();
        SpoolManager::new(ctx, config, CancellationToken::new())
    }

    /// Wait for every running fetch and record what each sends back
    async fn settle(manager: &mut TestManager) {
        loop {
            let joined = manager.tasks.join_next_with_id().await;
            let Some(joined) = joined else {
                return;
            };
            manager.record_finished_fetch(joined);
        }
    }

    /// The track's missing track in our stored spool's missing_list
    fn missing_of(lacking: &Lacking) -> Option<MissingTrack> {
        let store = &lacking.ctx.store;
        let read = store.get_missing_track(lacking.stored(), lacking.address);
        read.expect("read")
    }

    fn last_attempt(lacking: &Lacking) -> Attempt {
        let missing = missing_of(lacking).expect("a missing track");
        missing.last_attempt.expect("an attempt")
    }

    fn is_ready(lacking: &Lacking) -> bool {
        let store = &lacking.ctx.store;
        let (spool, fold) = (lacking.spool, lacking.fold);
        let states = store.get_live_spool_states(spool, fold).expect("states");
        let has_handover = store.has_handover_tracks(spool, fold);
        let carried = fold.carried_count(GroupIndex::containing(spool));
        let has_handover = has_handover.expect("handover");
        is_ready_to_attest(&states, carried, has_handover)
    }

    fn epoch_before(lacking: &Lacking) -> EpochNumber {
        lacking.ctx.state().epoch().saturating_sub(EpochNumber(1))
    }

    // a gained spool scans its missing_list, a holder's transfer brings the
    // slice, and the node is then ready to attest
    #[tokio::test]
    async fn transfer_gets_slice() {
        let lacking = lacking(Peers::Serve).await;
        let mut manager = manager(&lacking);
        manager.wake();
        hold_before(&lacking, HOLDER);
        at_tip(&lacking.ctx);

        assert!(!is_ready(&lacking));
        manager.wake();
        settle(&mut manager).await;

        assert!(lacking.has_slice());
        assert!(missing_of(&lacking).is_none());
        assert!(is_ready(&lacking));
    }

    // a holder that lacks the slice passes the track to older holders, and
    // with none left the next turn tries a repair
    #[tokio::test]
    async fn holder_lacks() {
        let lacking = lacking(Peers::Lack).await;
        let mut manager = manager(&lacking);
        manager.wake();
        hold_before(&lacking, HOLDER);
        at_tip(&lacking.ctx);

        manager.wake();
        settle(&mut manager).await;
        let missing = missing_of(&lacking).expect("a missing track");
        assert_eq!(missing.owner_epoch_tried, epoch_before(&lacking));
        assert_eq!(last_attempt(&lacking).method, Method::Transfer);

        manager.wake();
        settle(&mut manager).await;
        let missing = missing_of(&lacking).expect("a missing track");
        let attempt = last_attempt(&lacking);
        assert_eq!(attempt.method, Method::Repair);
        assert_eq!(attempt.failure, AttemptFailure::NotHeld);
        assert!(missing.repair_failing_since.is_some());
        assert!(!is_ready(&lacking));
    }

    // a holder that's down rests, and the track waits on it: nothing else is
    // tried in its place
    #[tokio::test]
    async fn holder_down() {
        let lacking = lacking(Peers::Down).await;
        let mut manager = manager(&lacking);
        manager.wake();
        hold_before(&lacking, HOLDER);
        at_tip(&lacking.ctx);

        manager.wake();
        settle(&mut manager).await;
        let attempt = last_attempt(&lacking);
        assert_eq!(attempt.method, Method::Transfer);
        assert_eq!(attempt.failure, AttemptFailure::Down);

        manager.wake();
        assert!(manager.tasks.is_empty());
        assert_eq!(last_attempt(&lacking).method, Method::Transfer);
    }

    // with no holder the track is repaired from the spool_group's helpers
    #[tokio::test]
    async fn repairs_without_holders() {
        let lacking = lacking(Peers::Serve).await;
        let mut manager = manager(&lacking);
        manager.wake();
        at_tip(&lacking.ctx);

        manager.wake();
        settle(&mut manager).await;

        assert!(lacking.has_slice());
        assert!(missing_of(&lacking).is_none());
    }

    // a coded track with no encoding in the store waits on the list, and
    // nothing is fetched for it
    #[tokio::test]
    async fn no_encoding_waits() {
        let lacking = lacking(Peers::Serve).await;
        let mut manager = manager(&lacking);
        manager.wake();
        let store = &lacking.ctx.store;
        store.delete_track_data(lacking.address).expect("drop encoding");
        at_tip(&lacking.ctx);

        manager.wake();

        assert!(manager.tasks.is_empty());
        let missing = missing_of(&lacking).expect("a missing track");
        assert!(missing.last_attempt.is_none());
        assert!(!lacking.has_slice());
    }
}
