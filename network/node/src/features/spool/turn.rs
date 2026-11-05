//! A turn is a short stretch of work on one spool's missing_list, the list on
//! disk of the spool's tracks whose data we don't have yet. The spool manager
//! takes turns on its spools one after another, so that a spool with millions
//! of missing tracks can't hold up the others, and so that it soon gets back
//! to the fetches that have finished.
//!
//! ```text
//! for the next 128 missing tracks, starting where the last turn stopped:
//!   skip the track if it is already being fetched
//!   skip it if it is still waiting after a failure
//!   drop it from the list if the track is gone or isn't missing anything
//!   pick a method for it: sync, transfer, repair or recover
//!   add it to the fetch for that method and those peers
//!   if the peers it needs are all busy, stop the turn
//! when we reach the end of the list, scan the next page of the spool's tracks
//! start the fetches
//! ```

use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tape_core::encoding::ErasureCode;
use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, VirtualGroup};
use tape_core::track::blob::BlobEncoding;
use tape_core::track::data::BlobData;
use tape_core::track::types::CompressedTrack;
use tape_core::types::{EpochNumber, SpoolIndex, StorageUnits};
use tape_crypto::address::Address;
use tape_protocol::Api;
use tape_recover::attempt::{
    can_try_now, choose_attempt, pick_sync_peer, Method, NextAttempt,
};
use tape_recover::fetch::{get_track_bytes, FetchReq, FetchTrack};
use tape_recover::history::{
    get_group_peers, get_holders, get_sync_peers, OwnerHistory, PositionPeer,
};
use tape_recover::limits::SpoolLimits;
use tape_recover::time::WallTime;
use tape_recover::track::MissingTrack;
use tape_rpc::Rpc;
use tape_slicer::coded_slice_len;
use tape_store::ops::{SpoolOps, TrackDataOps, TrackOps};
use tape_store::TapeStore;
use tape_store_api::Store;
use tracing::warn;

use crate::features::spool::error::SpoolError;
use crate::features::spool::fetch::start_fetch;
use crate::features::spool::history::load_owner_history;
use crate::features::spool::manager::SpoolManager;
use crate::features::spool::scan::{
    is_track_live, is_track_missing, scan_next_page,
};

impl<Db, Cluster, Blockchain> SpoolManager<Db, Cluster, Blockchain>
where
    Db: Store + 'static,
    Cluster: Api + 'static,
    Blockchain: Rpc + 'static,
{
    /// Take one turn over a spool, then start the fetch_reqs it built
    pub fn take_turn(
        &mut self,
        epoch: EpochNumber,
        fold: GroupFold,
        spool: SpoolIndex,
    ) -> Result<(), SpoolError> {
        let mut turn = Turn::new(spool, fold, epoch);
        let visited = self.visit_missing_list(&mut turn);
        self.start_turn_fetches(&mut turn);
        visited
    }

    /// Visit up to a turn's missing tracks from the cursor on; at the end of
    /// the missing_list, scan one page and go round from the top
    fn visit_missing_list(
        &mut self,
        turn: &mut Turn,
    ) -> Result<(), SpoolError> {
        let mut cursor = self.cursors.get(&turn.spool).copied();
        let mut left = self.limits.turn_tracks;
        let mut has_wrapped = false;
        loop {
            let store = &self.context.store;
            let after = cursor.as_ref();
            let page =
                store.get_missing_tracks(turn.spool, turn.fold, after, left)?;
            left -= page.len();
            let step = self.visit_page(turn, &page, &mut cursor)?;
            if step == TurnStep::StopTurn || left == 0 || has_wrapped {
                break;
            }
            let store = &self.context.store;
            scan_next_page(store, turn.spool, turn.fold, &self.limits)?;
            cursor = None;
            has_wrapped = true;
        }
        match cursor {
            Some(missing) => self.cursors.insert(turn.spool, missing),
            None => self.cursors.remove(&turn.spool),
        };
        Ok(())
    }

    fn visit_page(
        &mut self,
        turn: &mut Turn,
        page: &[MissingTrack],
        cursor: &mut Option<MissingTrack>,
    ) -> Result<TurnStep, SpoolError> {
        for missing in page {
            let step = self.visit_missing_track(turn, *missing)?;
            if step == TurnStep::StopTurn {
                return Ok(TurnStep::StopTurn);
            }
            *cursor = Some(*missing);
        }
        Ok(TurnStep::NextEntry)
    }

    /// Decide one missing track's next_attempt and add it to a fetch_req
    fn visit_missing_track(
        &mut self,
        turn: &mut Turn,
        missing: MissingTrack,
    ) -> Result<TurnStep, SpoolError> {
        if self.fetch_pool.is_full(&self.limits) {
            return Ok(TurnStep::StopTurn);
        }
        let is_running = self.fetch_pool.is_track_running(missing.track);
        let is_queued = turn.queued.contains(&missing.track);
        let wait = self.upload_wait;
        let can_try = can_try_now(&missing, turn.now, wait, &self.limits);
        if is_running || is_queued || !can_try {
            return Ok(TurnStep::NextEntry);
        }
        let Some(live) = self.get_live_track(&missing)? else {
            self.context.store.delete_missing_track(&missing)?;
            return Ok(TurnStep::NextEntry);
        };
        let attempt = self.choose_next_attempt(turn, &missing, &live)?;
        let fetch_track = FetchTrack {
            missing_track: missing,
            track: live.track,
            encoding: live.encoding,
            holder_epoch: EpochNumber(0),
        };
        Ok(self.add_to_fetch(turn, attempt, fetch_track))
    }

    /// The missing track's track and its encoding, or nothing when it was
    /// deleted, isn't certified, left its tape, or has nothing missing
    fn get_live_track(
        &self,
        missing: &MissingTrack,
    ) -> Result<Option<LiveTrack>, SpoolError> {
        let store = &self.context.store;
        let Some(track) = store.get_track(missing.track)? else {
            return Ok(None);
        };
        if !is_track_live(store, &track)? {
            return Ok(None);
        }
        let spool = missing.spool;
        if !is_track_missing(store, spool, missing.track, &track)? {
            return Ok(None);
        }
        let encoding = get_coded_encoding(store, missing.track, &track)?;
        Ok(Some(LiveTrack { track, encoding }))
    }

    /// Sync for an inline track, whose payload a node keeps only for the
    /// groups it owns, and the choice of attempt for a coded track. A coded
    /// track always comes with its encoding, so one without it means the
    /// store is damaged, and it waits on the list.
    fn choose_next_attempt(
        &self,
        turn: &mut Turn,
        missing: &MissingTrack,
        live: &LiveTrack,
    ) -> Result<NextAttempt, SpoolError> {
        if live.track.is_coded() && live.encoding.is_none() {
            let track = missing.track;
            warn!(%track, "spool: coded track has no encoding in the store");
            self.context.metrics.inc_spool_missing_encoding();
            return Ok(NextAttempt::NotNow);
        }
        let me = self.context.node_address();
        let position = leaf_position(missing.spool).as_usize();
        let profile = self.context.params().profile;
        let now = turn.now;
        let limits = &self.limits;
        let store = &self.context.store;
        let history =
            turn.get_owner_history(store, missing.spool, profile, limits)?;
        let scores = &self.peer_scores;
        if live.track.is_inline() {
            let peers = get_sync_peers(history, position, me);
            return Ok(pick_sync_peer(&peers, scores, now, limits));
        }
        let holders = get_holders(history, position, me);
        let peers = get_group_peers(history, position, me);
        let attempt = choose_attempt(
            missing, &holders, &peers, scores, profile, now, limits,
        );
        Ok(attempt)
    }

    /// Add the track to the fetch_req its next_attempt calls for
    fn add_to_fetch(
        &mut self,
        turn: &mut Turn,
        attempt: NextAttempt,
        track: FetchTrack,
    ) -> TurnStep {
        let position = leaf_position(track.missing_track.spool).as_usize();
        match attempt {
            NextAttempt::NotNow => TurnStep::NextEntry,
            NextAttempt::Busy => TurnStep::StopTurn,
            NextAttempt::Sync { peer } => {
                let peers = vec![PositionPeer { position, peer }];
                self.add_to_req(turn, Method::Sync, peers, track)
            }
            NextAttempt::Transfer { holder } => {
                let peer = PositionPeer { position, peer: holder.node };
                let track = FetchTrack { holder_epoch: holder.epoch, ..track };
                self.add_to_req(turn, Method::Transfer, vec![peer], track)
            }
            NextAttempt::Repair { helpers } => {
                self.add_to_req(turn, Method::Repair, helpers, track)
            }
            NextAttempt::Recover { peers } => {
                self.context.metrics.inc_repair_to_recovery();
                self.add_to_req(turn, Method::Recover, peers, track)
            }
        }
    }

    /// Put a track in the turn's fetch_req for its method and peers; a
    /// fetch_req that fills up starts right then
    fn add_to_req(
        &mut self,
        turn: &mut Turn,
        method: Method,
        peers: Vec<PositionPeer>,
        track: FetchTrack,
    ) -> TurnStep {
        let profile = self.context.params().profile;
        let bytes = get_fetch_bytes(method, profile, &track);
        let found = self.get_req_index(turn, method, peers, bytes);
        let Some(index) = found else {
            return TurnStep::StopTurn;
        };
        turn.queued.insert(track.missing_track.track);
        turn.fetches[index].add_track(track, bytes);
        if !turn.fetches[index].is_full(&self.limits) {
            return TurnStep::NextEntry;
        }
        let full = turn.fetches.swap_remove(index);
        if !self.start_fetch_req(full, turn.now) {
            return TurnStep::StopTurn;
        }
        TurnStep::NextEntry
    }

    /// The turn's fetch_req for a method and its peers; one the track would
    /// take past its bytes starts first, and nothing when it can't
    fn get_req_index(
        &mut self,
        turn: &mut Turn,
        method: Method,
        peers: Vec<PositionPeer>,
        bytes: StorageUnits,
    ) -> Option<usize> {
        let Some(index) = turn.find_req(method, &peers) else {
            return Some(turn.add_req(method, peers));
        };
        if turn.fetches[index].can_add_track(bytes, &self.limits) {
            return Some(index);
        }
        let full = turn.fetches.swap_remove(index);
        if !self.start_fetch_req(full, turn.now) {
            return None;
        }
        Some(turn.add_req(method, peers))
    }

    /// Start each fetch_req of the turn the fetch_pool can start
    fn start_turn_fetches(&mut self, turn: &mut Turn) {
        for req in std::mem::take(&mut turn.fetches) {
            self.start_fetch_req(req, turn.now);
        }
    }

    /// Start a fetch_req when the fetch_pool can start it; one it can't is
    /// dropped, and its missing tracks wait for a later turn
    fn start_fetch_req(&mut self, req: FetchReq, now: WallTime) -> bool {
        let pool = &self.fetch_pool;
        if !pool.can_start(&req, &self.peer_scores, now, &self.limits) {
            return false;
        }
        let context = Arc::clone(&self.context);
        let tasks = &mut self.tasks;
        let id = start_fetch(tasks, context, req.clone(), &self.limits);
        for peer in &req.peers {
            self.peer_scores.start_fetch(peer.peer);
        }
        self.fetch_pool.add(id, req);
        true
    }
}

/// What a track counts toward a fetch_req for a method
fn get_fetch_bytes(
    method: Method,
    profile: ErasureCode,
    track: &FetchTrack,
) -> StorageUnits {
    let slice = get_slice_bytes(profile, track.encoding.as_ref());
    get_track_bytes(method, profile, &track.track, slice)
}

/// The size of one slice of a coded track, when the store holds its
/// encoding
fn get_slice_bytes(
    profile: ErasureCode,
    encoding: Option<&BlobEncoding>,
) -> Option<StorageUnits> {
    let encoding = encoding?;
    let len = coded_slice_len(
        profile,
        encoding.size.as_usize(),
        encoding.stripe_size.as_usize(),
        encoding.stripe_count.as_usize(),
    );
    Some(StorageUnits(len as u64))
}

/// A coded track's encoding, when the store holds it
fn get_coded_encoding<Db: Store>(
    store: &TapeStore<Db>,
    address: Address,
    track: &CompressedTrack,
) -> Result<Option<BlobEncoding>, SpoolError> {
    if !track.is_coded() {
        return Ok(None);
    }
    match store.get_track_data(address)? {
        Some(BlobData::Coded(encoding)) => Ok(Some(encoding)),
        Some(BlobData::Inline(_)) | None => Ok(None),
    }
}

/// One turn's spool, its clock, the owner histories it read, and the
/// fetch_reqs it builds
struct Turn {
    spool: SpoolIndex,
    fold: GroupFold,
    epoch: EpochNumber,
    now: WallTime,
    histories: HashMap<VirtualGroup, OwnerHistory>,
    fetches: Vec<FetchReq>,
    queued: HashSet<Address>,
}

impl Turn {
    fn new(spool: SpoolIndex, fold: GroupFold, epoch: EpochNumber) -> Self {
        Self {
            spool,
            fold,
            epoch,
            now: WallTime::now(),
            histories: HashMap::new(),
            fetches: Vec::new(),
            queued: HashSet::new(),
        }
    }

    /// The owner_history of a stored spool's virtual group, read once a turn
    fn get_owner_history<Db: Store>(
        &mut self,
        store: &TapeStore<Db>,
        spool: SpoolIndex,
        profile: ErasureCode,
        limits: &SpoolLimits,
    ) -> Result<&OwnerHistory, SpoolError> {
        let group = VirtualGroup::containing(spool);
        let epoch = self.epoch;
        let load = || load_owner_history(store, epoch, group, profile, limits);
        match self.histories.entry(group) {
            MapEntry::Occupied(found) => Ok(found.into_mut()),
            MapEntry::Vacant(empty) => Ok(empty.insert(load()?)),
        }
    }

    /// The turn's fetch_req for a method and its peers
    fn find_req(
        &self,
        method: Method,
        peers: &[PositionPeer],
    ) -> Option<usize> {
        for (index, req) in self.fetches.iter().enumerate() {
            if req.method == method && req.peers == peers {
                return Some(index);
            }
        }
        None
    }

    /// A new fetch_req for a method and its peers, at our live spool
    fn add_req(&mut self, method: Method, peers: Vec<PositionPeer>) -> usize {
        let live_groups = self.fold.live();
        let req = FetchReq::new(method, self.spool, live_groups, peers);
        self.fetches.push(req);
        self.fetches.len() - 1
    }
}

/// Whether a turn goes on to the next missing track or stops where it is
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TurnStep {
    NextEntry,
    StopTurn,
}

/// A missing track's track, and its encoding when it is coded and held
struct LiveTrack {
    track: CompressedTrack,
    encoding: Option<BlobEncoding>,
}
