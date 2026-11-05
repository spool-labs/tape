//! The spool manager: one loop that wakes when a fetch finishes or a second
//! passes, records what finished, checks the epoch, and takes turns over the
//! spools this node owns, round robin
//!
//! A turn looks at up to a page of one spool's missing_list from where the
//! last turn stopped, decides a next_attempt for each ready entry and starts
//! fetches. Fetches run on their own and report back to the loop, so nothing
//! in a turn awaits the network.

use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, VirtualGroup};
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::{Api, ProtocolState};
use tape_rpc::Rpc;
use tape_store::ops::{SpoolOps, TrackDataOps, TrackOps};
use tape_store::types::{
    Attempt, AttemptResult, Entry, EntryKey, Method, WallTime,
};
use tape_store::TapeStore;
use tape_store_api::Store;
use tokio::task::{Id, JoinError};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::recovery::RecoveryConfig;
use crate::context::NodeContext;
use crate::core::error::NodeError;
use crate::core::ingest::SLOT_MS;
use crate::features::spool::attempt::{
    choose_attempt, is_ready, pick_peer, Choice, Helper, HolderView,
    NextAttempt, PositionView, TrackWaits,
};
use crate::features::spool::epoch::{
    change_rows, kept_peers, owned_live_spools, record_history,
};
use crate::features::spool::error::SpoolError;
use crate::features::spool::fetch::{
    time_limit, track_bytes, Fetch, FetchReport, FetchTrack, Held, Running,
    TrackOutcome,
};
use crate::features::spool::fill::{fill_missing_list, is_live, is_missing};
use crate::features::spool::peers::{
    holders, load_history, position_peers, sync_peers, GroupHistory,
    PeerRules, PeerScores, PeerView,
};
use crate::features::spool::repair::{clay_slicer, ClaySlicer, RepairPlanner};
use crate::features::spool::{recover, repair, sync, transfer};
use crate::features::store::apply::REPAIR_GRACE_SLOTS;

/// Whether a turn goes on to the next entry or stops where it is
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Step {
    Next,
    Stop,
}

/// The spool manager's one task
pub struct SpoolManager<Db: Store, Cluster: Api, Blockchain: Rpc> {
    context: Arc<NodeContext<Db, Cluster, Blockchain>>,
    config: RecoveryConfig,
    cancel: CancellationToken,

    peers: PeerScores,
    rules: PeerRules,
    clay: Option<ClaySlicer>,

    cursors: HashMap<SpoolIndex, EntryKey>,
    running: Running,
    epoch: Option<EpochNumber>,
    next_spool: usize,
}

/// One turn's spool, its clock and waits, the histories it read, and the
/// fetches it fills
struct Turn {
    spool: SpoolIndex,
    fold: GroupFold,
    epoch: EpochNumber,
    now: WallTime,
    instant: Instant,
    waits: TrackWaits,
    histories: HashMap<VirtualGroup, GroupHistory>,
    batches: Vec<Fetch>,
    /// Tracks put in a fetch this turn, so a list read twice adds each once
    batched: HashSet<Address>,
}

/// A track the turn read, with its encoding when it is coded
struct Looked {
    track: CompressedTrack,
    blob: Option<BlobEncoding>,
}

impl<Db, Cluster, Blockchain> SpoolManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    pub fn new(
        context: Arc<NodeContext<Db, Cluster, Blockchain>>,
        config: RecoveryConfig,
        cancel: CancellationToken,
    ) -> Self {
        let rules = PeerRules::from_config(&config);
        let clay = clay_slicer(context.params().profile);
        Self {
            context,
            config,
            cancel,
            peers: PeerScores::default(),
            rules,
            clay,
            cursors: HashMap::new(),
            running: Running::default(),
            epoch: None,
            next_spool: 0,
        }
    }

    pub async fn run(mut self) -> Result<(), NodeError> {
        let wake = Duration::from_secs(self.config.wake_secs);
        loop {
            tokio::select! {
                // Safe: cancelled holds no state
                _ = self.cancel.cancelled() => break,
                // Safe: join_next only reaps a fetch that has finished
                Some(joined) = self.running.tasks.join_next_with_id() => {
                    self.finished(joined);
                }
                // Safe: a sleep dropped early loses nothing
                _ = sleep(wake) => {}
            }
            self.wake();
        }
        self.running.tasks.shutdown().await;
        Ok(())
    }

    /// Record the finished fetches, check the epoch, and take the turns
    fn wake(&mut self) {
        while let Some(joined) = self.running.tasks.try_join_next_with_id() {
            self.finished(joined);
        }
        let state = self.context.state();
        let Some(fold) = state.fold() else {
            return;
        };
        if let Err(error) = self.check_epoch(&state) {
            warn!(%error, "spool: epoch turn failed");
            return;
        }
        if self.context.is_at_tip() {
            self.take_turns(&state, fold);
        }
    }

    fn check_epoch(
        &mut self,
        state: &ProtocolState,
    ) -> Result<(), SpoolError> {
        let epoch = state.epoch();
        if self.epoch == Some(epoch) {
            return Ok(());
        }
        let is_first = self.epoch.is_none();
        self.new_epoch(state, is_first)?;
        self.epoch = Some(epoch);
        Ok(())
    }

    /// Record the epoch's owners, write the rows of the spools gained and
    /// lost, purge those lost long enough ago, and forget the peers no
    /// longer around. The missing_lists and running fetches carry on.
    fn new_epoch(
        &mut self,
        state: &ProtocolState,
        is_first: bool,
    ) -> Result<(), SpoolError> {
        let store = &self.context.store;
        let profile = self.context.params().profile;
        let keep = self.config.keep_epochs;
        record_history(store, state, profile, keep, is_first)?;

        let me = self.context.node_address();
        let page = self.config.fill_page;
        let changes = change_rows(store, state, me, keep, page)?;
        let kept = kept_peers(store, state, keep)?;
        self.peers.keep_only(&kept);

        let owned = owned_live_spools(state, me);
        let owned: HashSet<SpoolIndex> = owned.into_iter().collect();
        let is_owned = |spool: &SpoolIndex, _: &mut EntryKey| {
            owned.contains(spool)
        };
        self.cursors.retain(is_owned);

        let epoch = state.epoch();
        let written = changes.written.len();
        let purged = changes.purged.len();
        info!(%epoch, written, purged, "spool: new epoch");
        Ok(())
    }

    /// Up to the most turns, round robin over the spools we own
    fn take_turns(&mut self, state: &ProtocolState, fold: GroupFold) {
        let spools = owned_live_spools(state, self.context.node_address());
        if spools.is_empty() {
            return;
        }
        for _ in 0..self.config.turns_per_wake {
            if !self.running.has_room(&self.config) {
                return;
            }
            let spool = spools[self.next_spool % spools.len()];
            self.next_spool = self.next_spool.wrapping_add(1);
            let mut turn = self.start_turn(state.epoch(), fold, spool);
            if let Err(error) = self.turn(&mut turn) {
                warn!(%spool, %error, "spool: turn failed");
            }
            self.start_batches(&mut turn);
        }
    }

    fn start_turn(
        &self,
        epoch: EpochNumber,
        fold: GroupFold,
        spool: SpoolIndex,
    ) -> Turn {
        let progress = self.context.ingest.progress();
        let slot_ms = progress.slot_ms().unwrap_or(SLOT_MS);
        let upload_ms = REPAIR_GRACE_SLOTS.0.saturating_mul(slot_ms);
        let upload = Duration::from_millis(upload_ms);
        Turn {
            spool,
            fold,
            epoch,
            now: WallTime::now(),
            instant: Instant::now(),
            waits: TrackWaits::new(&self.config, upload),
            histories: HashMap::new(),
            batches: Vec::new(),
            batched: HashSet::new(),
        }
    }

    /// Look at up to a page of entries from the cursor on; at the end of the
    /// missing_list, fill one page and go round from the top
    fn turn(&mut self, turn: &mut Turn) -> Result<(), SpoolError> {
        let spool = turn.spool;
        let mut cursor = self.cursors.get(&spool).copied();
        let mut left = self.config.turn_entries;
        let mut has_wrapped = false;
        loop {
            let store = &self.context.store;
            let read = store.missing_page(spool, turn.fold, cursor, left);
            let page = read.map_err(store_error)?;
            left -= page.len();
            let step = self.look_at_page(turn, &page, &mut cursor)?;
            if step == Step::Stop || left == 0 || has_wrapped {
                break;
            }
            let store = &self.context.store;
            let page_size = self.config.fill_page;
            fill_missing_list(store, spool, turn.fold, page_size)?;
            cursor = None;
            has_wrapped = true;
        }
        match cursor {
            Some(key) => self.cursors.insert(spool, key),
            None => self.cursors.remove(&spool),
        };
        Ok(())
    }

    fn look_at_page(
        &mut self,
        turn: &mut Turn,
        page: &[(EntryKey, Entry)],
        cursor: &mut Option<EntryKey>,
    ) -> Result<Step, SpoolError> {
        for (key, entry) in page {
            if self.look(turn, *key, *entry)? == Step::Stop {
                return Ok(Step::Stop);
            }
            *cursor = Some(*key);
        }
        Ok(Step::Next)
    }

    /// Decide one entry's next_attempt and act on it
    fn look(
        &mut self,
        turn: &mut Turn,
        key: EntryKey,
        entry: Entry,
    ) -> Result<Step, SpoolError> {
        if !self.running.has_room(&self.config) {
            return Ok(Step::Stop);
        }
        let is_running = self.running.is_running(key.track);
        let is_taken = is_running || turn.batched.contains(&key.track);
        if is_taken || !is_ready(&entry, turn.now, &turn.waits) {
            return Ok(Step::Next);
        }
        let Some(looked) = self.look_up(key)? else {
            let store = &self.context.store;
            store.delete_entry(key).map_err(store_error)?;
            return Ok(Step::Next);
        };
        let next = self.next_attempt(turn, key, &entry, &looked)?;
        Ok(self.act(turn, key, entry, looked, next))
    }

    /// The entry's track, or nothing when it was deleted, isn't certified,
    /// left its tape, or has nothing missing
    fn look_up(&self, key: EntryKey) -> Result<Option<Looked>, SpoolError> {
        let store = &self.context.store;
        let read = store.get_track(key.track).map_err(store_error)?;
        let Some(track) = read else {
            return Ok(None);
        };
        if !is_live(store, &track)? {
            return Ok(None);
        }
        if !is_missing(store, key.spool, key.track, &track)? {
            return Ok(None);
        }
        let blob = coded_blob(store, key.track, &track)?;
        Ok(Some(Looked { track, blob }))
    }

    fn next_attempt(
        &self,
        turn: &mut Turn,
        key: EntryKey,
        entry: &Entry,
        looked: &Looked,
    ) -> Result<NextAttempt, SpoolError> {
        let me = self.context.node_address();
        let position = leaf_position(key.spool).as_usize();
        let now = turn.now;
        let instant = turn.instant;
        let waits = turn.waits;
        let profile = self.context.params().profile;
        let width = usize::from(profile.n());
        let keep = self.config.keep_epochs;
        let store = &self.context.store;
        let history = turn.history(store, key.spool, width, keep)?;
        let Some(blob) = looked.blob else {
            let peers = sync_peers(history, position, me);
            let peers = self.peer_views(&peers, instant);
            return Ok(sync_attempt(pick_peer(&peers)));
        };

        let holders = self.holder_views(history, position, me, instant);
        let positions = self.position_views(history, position, me, instant);
        let planner = self.planner(position, &blob);
        let attempt = choose_attempt(
            entry,
            &holders,
            &positions,
            planner.as_ref(),
            profile,
            now,
            &waits,
        );
        Ok(attempt)
    }

    fn planner(
        &self,
        position: usize,
        blob: &BlobEncoding,
    ) -> Option<RepairPlanner> {
        let slicer = self.clay.as_ref()?;
        Some(RepairPlanner::new(Arc::clone(slicer), position, blob))
    }

    fn peer_views(&self, peers: &[Address], now: Instant) -> Vec<PeerView> {
        let mut views = Vec::with_capacity(peers.len());
        for peer in peers {
            let status = self.peers.status(*peer, now, &self.rules);
            views.push(PeerView { peer: *peer, status });
        }
        views
    }

    fn holder_views(
        &self,
        history: &GroupHistory,
        position: usize,
        me: Address,
        now: Instant,
    ) -> Vec<HolderView> {
        let mut views = Vec::new();
        for holder in holders(history, position, me) {
            let status = self.peers.status(holder.node, now, &self.rules);
            views.push(HolderView { holder, status });
        }
        views
    }

    /// Every other position of the spool_group, with its peers' status
    fn position_views(
        &self,
        history: &GroupHistory,
        ours: usize,
        me: Address,
        now: Instant,
    ) -> Vec<PositionView> {
        let width = usize::from(self.context.params().profile.n());
        let mut views = Vec::with_capacity(width);
        for position in 0..width {
            if position == ours {
                continue;
            }
            let peers = position_peers(history, position, me);
            let peers = self.peer_views(&peers, now);
            views.push(PositionView { position, peers });
        }
        views
    }

    /// Add the entry's track to the fetch its next_attempt calls for
    fn act(
        &mut self,
        turn: &mut Turn,
        key: EntryKey,
        entry: Entry,
        looked: Looked,
        next: NextAttempt,
    ) -> Step {
        let position = leaf_position(key.spool).as_usize();
        let track = FetchTrack {
            key,
            entry,
            track: looked.track,
            blob: looked.blob,
            holder_epoch: EpochNumber(0),
        };
        match next {
            NextAttempt::NotNow => Step::Next,
            NextAttempt::Busy => Step::Stop,
            NextAttempt::Sync { peer } => {
                let peers = vec![Helper { position, peer }];
                self.add(turn, Method::Sync, peers, track)
            }
            NextAttempt::Transfer { holder } => {
                let peer = holder.node;
                let peers = vec![Helper { position, peer }];
                let epoch = holder.epoch;
                let track = FetchTrack { holder_epoch: epoch, ..track };
                self.add(turn, Method::Transfer, peers, track)
            }
            NextAttempt::Repair { helpers } => {
                self.add(turn, Method::Repair, helpers, track)
            }
            NextAttempt::Recovery { peers } => {
                self.context.metrics.inc_repair_to_recovery();
                self.add(turn, Method::Recovery, peers, track)
            }
        }
    }

    /// Put a track in the turn's fetch for its method and peers; a fetch
    /// that fills up starts right then
    fn add(
        &mut self,
        turn: &mut Turn,
        method: Method,
        peers: Vec<Helper>,
        track: FetchTrack,
    ) -> Step {
        let profile = self.context.params().profile;
        let blob = track.blob.as_ref();
        let bytes = track_bytes(method, profile, &track.track, blob);
        let Some(index) = self.batch_for(turn, method, peers, bytes) else {
            return Step::Stop;
        };
        turn.batched.insert(track.key.track);
        turn.batches[index].push(track, bytes);
        if !turn.batches[index].is_full(&self.config) {
            return Step::Next;
        }
        let full = turn.batches.swap_remove(index);
        if !self.start(full, turn.instant) {
            return Step::Stop;
        }
        Step::Next
    }

    /// The turn's fetch for a method and its peers; one that `bytes` would
    /// take past its bytes starts first, or nothing when it can't
    fn batch_for(
        &mut self,
        turn: &mut Turn,
        method: Method,
        peers: Vec<Helper>,
        bytes: StorageUnits,
    ) -> Option<usize> {
        let Some(index) = batch_index(turn, method, &peers) else {
            return Some(new_batch(turn, method, peers));
        };
        if !turn.batches[index].would_overflow(bytes, &self.config) {
            return Some(index);
        }
        let full = turn.batches.swap_remove(index);
        if !self.start(full, turn.instant) {
            return None;
        }
        Some(new_batch(turn, method, peers))
    }

    /// Start each fetch of the turn there's room for
    fn start_batches(&mut self, turn: &mut Turn) {
        for fetch in std::mem::take(&mut turn.batches) {
            self.start(fetch, turn.instant);
        }
    }

    /// Start a fetch if there's room for it; a fetch with no room is
    /// dropped, and its entries wait for a later turn
    fn start(&mut self, fetch: Fetch, now: Instant) -> bool {
        let running = &self.running;
        let peers = &self.peers;
        let rules = &self.rules;
        if !running.can_start(&fetch, &self.config, peers, now, rules) {
            return false;
        }
        let held = Held::of(&fetch);
        let Some(id) = self.spawn(fetch) else {
            return false;
        };
        for peer in &held.peers {
            self.peers.start(*peer, now);
        }
        self.running.hold(id, held);
        true
    }

    fn spawn(&mut self, fetch: Fetch) -> Option<Id> {
        let ctx = Arc::clone(&self.context);
        let limit = time_limit(fetch.bytes, &self.config);
        let tasks = &mut self.running.tasks;
        let handle = match fetch.method {
            Method::Upload | Method::Sync => {
                tasks.spawn(sync::run(ctx, fetch, limit))
            }
            Method::Transfer => tasks.spawn(transfer::run(ctx, fetch, limit)),
            Method::Recovery => tasks.spawn(recover::run(ctx, fetch, limit)),
            Method::Repair => {
                let slicer = Arc::clone(self.clay.as_ref()?);
                tasks.spawn(repair::run(ctx, fetch, limit, slicer))
            }
        };
        Some(handle.id())
    }

    /// A fetch ended: let go of what it held and record what it reports
    fn finished(&mut self, joined: Result<(Id, FetchReport), JoinError>) {
        let now = Instant::now();
        let id = match &joined {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        if let Some(held) = self.running.release(id) {
            for peer in &held.peers {
                self.peers.release(*peer, now);
            }
        }
        match joined {
            Ok((_, report)) => self.record_result(report, now),
            Err(error) => warn!(%error, "spool: fetch ended without a report"),
        }
    }

    /// Update the peers' scores, then each track's entry
    fn record_result(&mut self, report: FetchReport, now: Instant) {
        for result in &report.peers {
            let outcome = result.outcome;
            self.peers.update(result.peer, outcome, now, &self.rules);
        }
        let wall = WallTime::now();
        let method = report.fetch.method;
        for result in &report.tracks {
            let address = result.key.track;
            let Some(track) = report.fetch.track(address) else {
                continue;
            };
            let outcome = result.outcome;
            let recorded = self.record_track(method, track, outcome, wall);
            if let Err(error) = recorded {
                warn!(track = %address, %error, "spool: entry write failed");
            }
        }
        let tracks = report.tracks.len();
        let peers = &report.peers;
        debug!(?method, ?peers, tracks, "spool: fetch finished");
    }

    fn record_track(
        &self,
        method: Method,
        track: &FetchTrack,
        outcome: TrackOutcome,
        now: WallTime,
    ) -> Result<(), SpoolError> {
        let store = &self.context.store;
        let result = match outcome {
            TrackOutcome::Got => {
                return store.delete_entry(track.key).map_err(store_error);
            }
            TrackOutcome::Failed(result) => result,
        };
        let epoch = track.holder_epoch;
        let entry = recorded(track.entry, method, result, epoch, now);
        store.put_entry(track.key, entry).map_err(store_error)
    }
}

impl Turn {
    /// The owners of a stored spool's virtual group over the recorded
    /// epochs, read once a turn
    fn history<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        spool: SpoolIndex,
        width: usize,
        keep_epochs: u64,
    ) -> Result<&GroupHistory, SpoolError> {
        let group = VirtualGroup::containing(spool);
        let epoch = self.epoch;
        match self.histories.entry(group) {
            MapEntry::Occupied(found) => Ok(found.into_mut()),
            MapEntry::Vacant(empty) => {
                let keep = keep_epochs;
                let read = load_history(store, epoch, group, width, keep);
                Ok(empty.insert(read?))
            }
        }
    }
}

/// Record what an attempt did to an entry: a holder that said no moves the
/// holders on, a repair that failed starts the repair clock, and a failure
/// of the track's own counts
pub fn recorded(
    entry: Entry,
    method: Method,
    result: AttemptResult,
    holder_epoch: EpochNumber,
    now: WallTime,
) -> Entry {
    let mut entry = entry;
    let is_own = matches!(
        result,
        AttemptResult::NotHeld | AttemptResult::WrongData
    );
    if is_own {
        own_failure(&mut entry, method, holder_epoch, now);
    }
    let attempt = Attempt { at: now, method, result: Some(result) };
    entry.last_attempt = Some(attempt);
    entry
}

/// A failure of the track's own, by the method that met it
fn own_failure(
    entry: &mut Entry,
    method: Method,
    holder_epoch: EpochNumber,
    now: WallTime,
) {
    match method {
        Method::Transfer => entry.owner_epoch_tried = holder_epoch,
        Method::Repair => {
            entry.repair_failing_since.get_or_insert(now);
            entry.failures = entry.failures.saturating_add(1);
        }
        Method::Upload | Method::Sync | Method::Recovery => {
            entry.failures = entry.failures.saturating_add(1);
        }
    }
}

fn sync_attempt(choice: Choice<Address>) -> NextAttempt {
    match choice {
        Choice::Give(peer) => NextAttempt::Sync { peer },
        Choice::Busy => NextAttempt::Busy,
        Choice::NotNow | Choice::No => NextAttempt::NotNow,
    }
}

fn batch_index(turn: &Turn, method: Method, peers: &[Helper]) -> Option<usize> {
    for (index, fetch) in turn.batches.iter().enumerate() {
        if fetch.method == method && fetch.peers == peers {
            return Some(index);
        }
    }
    None
}

fn new_batch(turn: &mut Turn, method: Method, peers: Vec<Helper>) -> usize {
    let live_groups = turn.fold.live();
    let fetch = Fetch::new(method, turn.spool, live_groups, peers);
    turn.batches.push(fetch);
    turn.batches.len() - 1
}

/// A coded track's encoding, when the store holds it
fn coded_blob<Db: Store>(
    store: &TapeStore<Db>,
    address: Address,
    track: &CompressedTrack,
) -> Result<Option<BlobEncoding>, SpoolError> {
    if !track.is_coded() {
        return Ok(None);
    }
    match store.get_track_data(address).map_err(store_error)? {
        Some(BlobData::Coded(blob)) => Ok(Some(blob)),
        Some(BlobData::Inline(_)) | None => Ok(None),
    }
}

fn store_error(error: impl std::fmt::Display) -> SpoolError {
    SpoolError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use peer_memory::MemoryApi;
    use rpc_litesvm::LiteSvmRpc;
    use tape_store_memory::MemoryStore;

    use super::*;
    use crate::features::spool::fill::ready_to_attest;
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

    /// Wait for every running fetch and record what each reports
    async fn settle(manager: &mut TestManager) {
        loop {
            let joined = manager.running.tasks.join_next_with_id().await;
            let Some(joined) = joined else {
                return;
            };
            manager.finished(joined);
        }
    }

    /// The track's entry in the live spool's missing_list
    fn entry_of(lacking: &Lacking) -> Option<Entry> {
        let store = &lacking.ctx.store;
        let page = store.missing_page(lacking.spool, lacking.fold, None, 10);
        for (key, entry) in page.expect("page") {
            if key.track == lacking.address {
                return Some(entry);
            }
        }
        None
    }

    fn last_attempt(lacking: &Lacking) -> Attempt {
        let entry = entry_of(lacking).expect("an entry");
        entry.last_attempt.expect("an attempt")
    }

    fn is_ready_to_attest(lacking: &Lacking) -> bool {
        let store = &lacking.ctx.store;
        let ready = ready_to_attest(store, lacking.spool, lacking.fold);
        ready.expect("ready")
    }

    fn epoch_before(lacking: &Lacking) -> EpochNumber {
        lacking.ctx.state().epoch().saturating_sub(EpochNumber(1))
    }

    // a gained spool fills its missing_list, a holder's transfer brings the
    // slice, and the node is then ready to attest
    #[tokio::test]
    async fn transfer_gets_slice() {
        let lacking = lacking(Peers::Serve).await;
        let mut manager = manager(&lacking);
        manager.wake();
        hold_before(&lacking, HOLDER);
        at_tip(&lacking.ctx);

        assert!(!is_ready_to_attest(&lacking));
        manager.wake();
        settle(&mut manager).await;

        assert!(lacking.has_slice());
        assert!(entry_of(&lacking).is_none());
        assert!(is_ready_to_attest(&lacking));
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
        let entry = entry_of(&lacking).expect("an entry");
        assert_eq!(entry.owner_epoch_tried, epoch_before(&lacking));
        assert_eq!(last_attempt(&lacking).method, Method::Transfer);

        manager.wake();
        settle(&mut manager).await;
        let entry = entry_of(&lacking).expect("an entry");
        let attempt = last_attempt(&lacking);
        assert_eq!(attempt.method, Method::Repair);
        assert_eq!(attempt.result, Some(AttemptResult::NotHeld));
        assert!(entry.repair_failing_since.is_some());
        assert!(!is_ready_to_attest(&lacking));
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
        assert_eq!(attempt.result, Some(AttemptResult::Down));

        manager.wake();
        assert!(manager.running.tasks.is_empty());
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
        assert!(entry_of(&lacking).is_none());
    }

    // a holder that said no moves the holders on, a failed repair starts the
    // repair clock, and only a failure of the track's own counts
    #[test]
    fn records_results() {
        let entry = Entry::never_tried();
        let now = WallTime(5_000);
        let later = WallTime(9_000);
        let epoch = EpochNumber(8);
        let none = EpochNumber(0);
        let not_held = AttemptResult::NotHeld;
        let wrong = AttemptResult::WrongData;
        let down = AttemptResult::Down;

        let transfer = recorded(entry, Method::Transfer, not_held, epoch, now);
        let repair = recorded(entry, Method::Repair, wrong, none, now);
        let again = recorded(repair, Method::Repair, wrong, none, later);
        let sync = recorded(entry, Method::Sync, down, none, now);

        assert_eq!(transfer.owner_epoch_tried, epoch);
        assert_eq!(transfer.failures, 0);
        assert_eq!(again.repair_failing_since, Some(now));
        assert_eq!(again.failures, 2);
        assert_eq!(sync.failures, 0);
        let result = Some(down);
        let attempt = Attempt { at: now, method: Method::Sync, result };
        assert_eq!(sync.last_attempt, Some(attempt));
    }
}
