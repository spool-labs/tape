//! The choice of attempt: pure functions over what an entry records and how
//! the peers are doing right now
//!
//! Nothing here reads the store or the network. The turn gathers the views
//! and these functions decide.

use std::cmp::Reverse;
use std::time::Duration;

use tape_core::encoding::ErasureCode;
use tape_crypto::address::Address;
use tape_store::types::{AttemptResult, Entry, Method, WallTime};

use crate::config::recovery::RecoveryConfig;
use crate::features::spool::peers::{
    doubled, BytesPerSecond, Holder, PeerStatus, PeerView,
};

/// The waits a track's own record sets
#[derive(Clone, Copy, Debug)]
pub struct TrackWaits {
    /// After the upload, the uploader's time to deliver
    pub upload: Duration,
    /// After a failure of the track's own, doubled per failure
    pub own: Duration,
    pub own_max: Duration,
    /// Failing repairs this long may go to recovery
    pub repair_failing: Duration,
}

/// One position's peer, for a repair or a recovery
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Helper {
    pub position: usize,
    pub peer: Address,
}

/// A holder and its status
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HolderView {
    pub holder: Holder,
    pub status: PeerStatus,
}

/// Another position of the spool_group: its owner now, then its holders
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PositionView {
    pub position: usize,
    pub peers: Vec<PeerView>,
}

/// What a turn decides for one entry
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NextAttempt {
    Sync { peer: Address },
    Transfer { holder: Holder },
    Repair { helpers: Vec<Helper> },
    Recovery { peers: Vec<Helper> },
    /// The entry can't go right now; a later turn looks again
    NotNow,
    /// The peers it needs serve our other fetches; the turn stops here
    Busy,
}

/// What one method can do for an entry right now
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Choice<Pick> {
    Give(Pick),
    NotNow,
    Busy,
    /// The method can't help; the next one may start
    No,
}

/// Repair needs d helpers and a plan over the positions it may ask
pub trait PlanRepair {
    /// How many helpers a repair asks: d
    fn helpers(&self) -> usize;

    /// The positions a repair from `available`, in order of preference,
    /// would ask, or nothing when they give no plan
    fn plan(&self, available: &[usize]) -> Option<Vec<usize>>;
}

impl TrackWaits {
    /// The track waits a recovery config sets, with the upload's wait
    pub fn new(config: &RecoveryConfig, upload: Duration) -> Self {
        Self {
            upload,
            own: Duration::from_secs(config.track_wait_secs),
            own_max: Duration::from_secs(config.track_wait_max_secs),
            repair_failing: Duration::from_secs(config.repair_failing_secs),
        }
    }
}

/// Whether the turn may try the entry now
pub fn is_ready(entry: &Entry, now: WallTime, waits: &TrackWaits) -> bool {
    let Some(attempt) = entry.last_attempt else {
        return true;
    };
    let waited = now.since(attempt.at);
    match attempt.method {
        Method::Upload => waited >= waits.upload,
        Method::Transfer => true,
        Method::Sync | Method::Repair | Method::Recovery => {
            let failures = u32::from(entry.failures);
            let own = doubled(waits.own, failures, waits.own_max);
            !is_own_failure(attempt.result) || waited >= own
        }
    }
}

/// A failure of the track's own: a peer said it doesn't have it, or sent
/// wrong data
fn is_own_failure(result: Option<AttemptResult>) -> bool {
    matches!(result, Some(AttemptResult::NotHeld | AttemptResult::WrongData))
}

/// Transfer, then repair, then recovery, always from the top, so a holder
/// that comes back takes over again
pub fn choose_attempt<Plan: PlanRepair>(
    entry: &Entry,
    holders: &[HolderView],
    positions: &[PositionView],
    repair: Option<&Plan>,
    profile: ErasureCode,
    now: WallTime,
    waits: &TrackWaits,
) -> NextAttempt {
    match can_transfer(entry, holders) {
        Choice::Give(holder) => return NextAttempt::Transfer { holder },
        Choice::NotNow => return NextAttempt::NotNow,
        Choice::Busy => return NextAttempt::Busy,
        Choice::No => {}
    }
    match can_repair(entry, positions, repair, now, waits) {
        Choice::Give(helpers) => return NextAttempt::Repair { helpers },
        Choice::NotNow => return NextAttempt::NotNow,
        Choice::Busy => return NextAttempt::Busy,
        Choice::No => {}
    }
    match can_recover(positions, profile) {
        Choice::Give(peers) => NextAttempt::Recovery { peers },
        Choice::Busy => NextAttempt::Busy,
        Choice::NotNow | Choice::No => NextAttempt::NotNow,
    }
}

/// Ask the holders newest first, only those older than the last that said
/// no; one down or slow waits its turn, and only gone ones are passed over
pub fn can_transfer(entry: &Entry, holders: &[HolderView]) -> Choice<Holder> {
    let candidates = candidates(entry, holders);
    let newest = first_not_gone(&candidates);
    if let Some(view) = newest.filter(is_free_view) {
        return Choice::Give(view.holder);
    }
    if let Some(view) = first_probe(&candidates) {
        return Choice::Give(view.holder);
    }
    match newest {
        Some(view) if view.status.is_resting() => Choice::NotNow,
        Some(_) => Choice::Busy,
        None => Choice::No,
    }
}

/// Rebuild our slice from helpers at other positions, while repairs
/// haven't failed for too long
pub fn can_repair<Plan: PlanRepair>(
    entry: &Entry,
    positions: &[PositionView],
    repair: Option<&Plan>,
    now: WallTime,
    waits: &TrackWaits,
) -> Choice<Vec<Helper>> {
    let Some(plan) = repair else {
        return Choice::No;
    };
    if has_repairs_failed(entry, now, waits) || was_wrong_repair(entry) {
        return Choice::No;
    }
    if !can_plan(plan, &usable(positions)) {
        return Choice::No;
    }
    if let Some(helpers) = free_helpers(plan, positions) {
        return Choice::Give(helpers);
    }
    if can_plan(plan, &awake(positions)) {
        return Choice::Busy;
    }
    Choice::NotNow
}

/// Whether `positions` hold d helpers and give a plan
fn can_plan<Plan: PlanRepair>(plan: &Plan, positions: &[usize]) -> bool {
    positions.len() >= plan.helpers() && plan.plan(positions).is_some()
}

/// The fastest free peer of each of the d fastest positions, when d
/// positions have a free peer and the plan takes them
fn free_helpers<Plan: PlanRepair>(
    plan: &Plan,
    positions: &[PositionView],
) -> Option<Vec<Helper>> {
    let mut free = fastest_free(positions);
    if free.len() < plan.helpers() {
        return None;
    }
    free.truncate(plan.helpers());
    let needed = plan.plan(&positions_of(&free))?;
    Some(chosen(&free, &needed))
}

/// Get k whole slices from k positions
pub fn can_recover(
    positions: &[PositionView],
    profile: ErasureCode,
) -> Choice<Vec<Helper>> {
    let k = usize::from(profile.k());
    if usable(positions).len() < k {
        return Choice::NotNow;
    }
    let mut free = fastest_free(positions);
    if free.len() >= k {
        free.truncate(k);
        return Choice::Give(free);
    }
    if awake(positions).len() >= k {
        return Choice::Busy;
    }
    Choice::NotNow
}

/// The fastest free peer, a probe when none is free, busy while one isn't
/// resting, and not_now otherwise
pub fn pick_peer(peers: &[PeerView]) -> Choice<Address> {
    if let Some(view) = fastest(peers) {
        return Choice::Give(view.peer);
    }
    for view in peers {
        if view.status.can_probe() {
            return Choice::Give(view.peer);
        }
    }
    for view in peers {
        if view.status == PeerStatus::Busy {
            return Choice::Busy;
        }
    }
    Choice::NotNow
}

/// Holders older than the last one that said no, newest first
fn candidates(entry: &Entry, holders: &[HolderView]) -> Vec<HolderView> {
    let tried = entry.owner_epoch_tried;
    let mut candidates = Vec::new();
    for view in holders {
        if tried.is_zero() || view.holder.epoch < tried {
            candidates.push(*view);
        }
    }
    candidates
}

fn first_not_gone(candidates: &[HolderView]) -> Option<HolderView> {
    for view in candidates {
        if !view.status.is_gone() {
            return Some(*view);
        }
    }
    None
}

fn first_probe(candidates: &[HolderView]) -> Option<HolderView> {
    for view in candidates {
        if view.status.can_probe() {
            return Some(*view);
        }
    }
    None
}

fn is_free_view(view: &HolderView) -> bool {
    view.status.is_free()
}

fn has_repairs_failed(
    entry: &Entry,
    now: WallTime,
    waits: &TrackWaits,
) -> bool {
    let Some(since) = entry.repair_failing_since else {
        return false;
    };
    now.since(since) >= waits.repair_failing
}

/// A repair whose slice failed its check had a helper that sent wrong data.
/// Nothing tells which one, and a helper a position's plan always needs
/// would fail every repair, so the track goes on to recovery.
fn was_wrong_repair(entry: &Entry) -> bool {
    let Some(attempt) = entry.last_attempt else {
        return false;
    };
    let is_wrong = attempt.result == Some(AttemptResult::WrongData);
    attempt.method == Method::Repair && is_wrong
}

/// Positions with a peer that isn't gone
fn usable(positions: &[PositionView]) -> Vec<usize> {
    let mut usable = Vec::new();
    for view in positions {
        if has_status(&view.peers, is_not_gone) {
            usable.push(view.position);
        }
    }
    usable
}

/// Positions with a peer that is neither resting nor gone
fn awake(positions: &[PositionView]) -> Vec<usize> {
    let mut awake = Vec::new();
    for view in positions {
        if has_status(&view.peers, is_awake) {
            awake.push(view.position);
        }
    }
    awake
}

fn is_not_gone(status: PeerStatus) -> bool {
    !status.is_gone()
}

fn is_awake(status: PeerStatus) -> bool {
    matches!(status, PeerStatus::Free { .. } | PeerStatus::Busy)
}

fn has_status(peers: &[PeerView], wanted: fn(PeerStatus) -> bool) -> bool {
    for view in peers {
        if wanted(view.status) {
            return true;
        }
    }
    false
}

/// The fastest free peer of each position that has one, fastest first
fn fastest_free(positions: &[PositionView]) -> Vec<Helper> {
    let mut free = Vec::new();
    for view in positions {
        let Some(peer) = fastest(&view.peers) else {
            continue;
        };
        let helper = Helper { position: view.position, peer: peer.peer };
        free.push(Ranked { rank: peer.status.speed_rank(), helper });
    }
    let by_speed = |ranked: &Ranked| Reverse(ranked.rank);
    free.sort_by_key(by_speed);

    let mut helpers = Vec::with_capacity(free.len());
    for ranked in free {
        helpers.push(ranked.helper);
    }
    helpers
}

/// A helper and its peer's speed
struct Ranked {
    rank: BytesPerSecond,
    helper: Helper,
}

fn fastest(peers: &[PeerView]) -> Option<PeerView> {
    let mut best: Option<PeerView> = None;
    for view in peers {
        if !view.status.is_free() {
            continue;
        }
        let is_faster = match best {
            Some(best) => view.status.speed_rank() > best.status.speed_rank(),
            None => true,
        };
        if is_faster {
            best = Some(*view);
        }
    }
    best
}

fn positions_of(helpers: &[Helper]) -> Vec<usize> {
    let mut positions = Vec::with_capacity(helpers.len());
    for helper in helpers {
        positions.push(helper.position);
    }
    positions
}

fn chosen(free: &[Helper], needed: &[usize]) -> Vec<Helper> {
    let mut helpers = Vec::with_capacity(needed.len());
    for helper in free {
        if needed.contains(&helper.position) {
            helpers.push(*helper);
        }
    }
    helpers
}

#[cfg(test)]
mod tests {
    use tape_core::encoding::{RSParams, TAPENET_PROFILE};
    use tape_core::types::EpochNumber;
    use tape_store::types::Attempt;

    use super::*;

    /// Three of four slices rebuild a track on this profile
    const SMALL: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    const FREE: PeerStatus = PeerStatus::Free { speed: None };

    /// A planner that needs the first `0` positions it is offered
    struct Needs(usize);

    impl PlanRepair for Needs {
        fn helpers(&self) -> usize {
            self.0
        }

        fn plan(&self, available: &[usize]) -> Option<Vec<usize>> {
            if available.len() < self.0 {
                return None;
            }
            Some(available[..self.0].to_vec())
        }
    }

    /// The helpers a choice gives, if it gives any
    fn given(choice: Choice<Vec<Helper>>) -> Option<Vec<Helper>> {
        if let Choice::Give(helpers) = choice {
            return Some(helpers);
        }
        None
    }

    fn waits() -> TrackWaits {
        TrackWaits::new(&RecoveryConfig::default(), Duration::from_secs(2))
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn secs(count: u64) -> WallTime {
        WallTime(count * 1_000)
    }

    fn tried(method: Method, result: AttemptResult, at: WallTime) -> Entry {
        let attempt = Attempt { at, method, result: Some(result) };
        Entry { last_attempt: Some(attempt), ..Entry::never_tried() }
    }

    fn holder(byte: u8, epoch: u64, status: PeerStatus) -> HolderView {
        let epoch = EpochNumber(epoch);
        let holder = Holder { node: address(byte), epoch };
        HolderView { holder, status }
    }

    fn view(byte: u8, status: PeerStatus) -> PeerView {
        PeerView { peer: address(byte), status }
    }

    fn positions(statuses: &[PeerStatus]) -> Vec<PositionView> {
        let mut views = Vec::new();
        for (position, status) in statuses.iter().enumerate() {
            let peer = view(position as u8 + 1, *status);
            views.push(PositionView { position, peers: vec![peer] });
        }
        views
    }

    fn gives(view: &HolderView) -> Choice<Holder> {
        Choice::Give(view.holder)
    }

    // a fresh entry is ready, the upload waits its slots, and a peer's
    // failure leaves the entry ready for the peer's rest to decide
    #[test]
    fn ready_rules() {
        let uploaded = Entry::uploaded(secs(10));
        let down = tried(Method::Sync, AttemptResult::Down, secs(10));

        assert!(is_ready(&Entry::never_tried(), secs(10), &waits()));
        assert!(!is_ready(&uploaded, secs(11), &waits()));
        assert!(is_ready(&uploaded, secs(12), &waits()));
        assert!(is_ready(&down, secs(10), &waits()));
    }

    // a failure of the track's own waits 2 s doubled per failure, up to 5 min
    #[test]
    fn own_wait_doubles() {
        let not_held = AttemptResult::NotHeld;
        let mut entry = tried(Method::Repair, not_held, secs(100));
        entry.failures = 3;

        assert!(!is_ready(&entry, secs(107), &waits()));
        assert!(is_ready(&entry, secs(108), &waits()));
        entry.failures = 20;
        assert!(!is_ready(&entry, secs(399), &waits()));
        assert!(is_ready(&entry, secs(400), &waits()));
    }

    // holders go newest first, and a resting newest holder keeps the track
    // waiting
    #[test]
    fn newest_holder() {
        let entry = Entry::never_tried();
        let free = [holder(1, 9, FREE), holder(2, 8, FREE)];
        let resting = [holder(1, 9, PeerStatus::Resting), holder(2, 8, FREE)];

        assert_eq!(can_transfer(&entry, &free), gives(&free[0]));
        assert_eq!(can_transfer(&entry, &resting), Choice::NotNow);
    }

    // a holder that said no passes the track to older ones, and none left
    // means repair may start
    #[test]
    fn holders_said_no() {
        let holders = [holder(1, 9, FREE), holder(2, 8, FREE)];
        let mut entry = Entry::never_tried();

        entry.owner_epoch_tried = EpochNumber(9);
        assert_eq!(can_transfer(&entry, &holders), gives(&holders[1]));
        entry.owner_epoch_tried = EpochNumber(8);
        assert_eq!(can_transfer(&entry, &holders), Choice::No);
    }

    // a gone holder is passed over, or probed once its rest ends
    #[test]
    fn gone_holder() {
        let entry = Entry::never_tried();
        let resting = PeerStatus::Gone { can_probe: false };
        let passed = [holder(1, 9, resting), holder(2, 8, FREE)];
        let probed = [holder(1, 9, PeerStatus::Gone { can_probe: true })];

        let none = [holder(1, 9, resting)];

        assert_eq!(can_transfer(&entry, &passed), gives(&passed[1]));
        assert_eq!(can_transfer(&entry, &probed), gives(&probed[0]));
        assert_eq!(can_transfer(&entry, &none), Choice::No);
    }

    // healthy holders serving our other fetches keep the track waiting for
    // them
    #[test]
    fn busy_holders() {
        let entry = Entry::never_tried();
        let holders = [holder(1, 9, PeerStatus::Busy)];

        let attempt = choose_attempt(
            &entry,
            &holders,
            &positions(&[FREE; 19]),
            Some(&Needs(16)),
            TAPENET_PROFILE,
            secs(0),
            &waits(),
        );

        assert_eq!(attempt, NextAttempt::Busy);
    }

    // a short outage of the only holder keeps the track at transfer, and
    // nothing falls through to repair
    #[test]
    fn short_outage() {
        let entry = tried(Method::Transfer, AttemptResult::Down, secs(0));
        let holders = [holder(1, 9, PeerStatus::Resting)];

        let attempt = choose_attempt(
            &entry,
            &holders,
            &positions(&[FREE; 19]),
            Some(&Needs(16)),
            TAPENET_PROFILE,
            secs(1),
            &waits(),
        );

        assert_eq!(attempt, NextAttempt::NotNow);
    }

    // repairs failing for 5 min, or a repair whose slice was wrong, give way
    // to recovery
    #[test]
    fn repair_gives_way() {
        let views = positions(&[FREE; 19]);
        let plan = Some(&Needs(16));
        let mut failing = Entry::never_tried();
        failing.repair_failing_since = Some(secs(0));
        let wrong = tried(Method::Repair, AttemptResult::WrongData, secs(0));

        let early = can_repair(&failing, &views, plan, secs(299), &waits());
        let late = can_repair(&failing, &views, plan, secs(300), &waits());
        let after_wrong = can_repair(&wrong, &views, plan, secs(1), &waits());

        assert!(matches!(early, Choice::Give(_)));
        assert_eq!(late, Choice::No);
        assert_eq!(after_wrong, Choice::No);
    }

    // repair gives the planned helpers, waits while they serve or rest, and
    // gives way when too few positions are usable
    #[test]
    fn repair_helpers() {
        let entry = Entry::never_tried();
        let plan = Some(&Needs(3));
        let gone = PeerStatus::Gone { can_probe: false };
        let resting = PeerStatus::Resting;

        let repair = |statuses: &[PeerStatus]| {
            can_repair(&entry, &positions(statuses), plan, secs(0), &waits())
        };

        let free = repair(&[FREE; 4]);
        let busy = repair(&[FREE, PeerStatus::Busy, PeerStatus::Busy, FREE]);
        let rest = repair(&[FREE, resting, resting, FREE]);
        let few = repair(&[FREE, gone, gone, FREE]);

        let helpers = given(free).expect("repair gives helpers");
        assert_eq!(helpers.len(), 3);
        assert_eq!(busy, Choice::Busy);
        assert_eq!(rest, Choice::NotNow);
        assert_eq!(few, Choice::No);
        let views = positions(&[FREE; 4]);
        let none = can_repair::<Needs>(&entry, &views, None, secs(0), &waits());
        assert_eq!(none, Choice::No);
    }

    // repair offers the plan only the d fastest free positions, so a slow
    // one sits out
    #[test]
    fn repair_skips_slow() {
        let entry = Entry::never_tried();
        let slow = PeerStatus::Free { speed: Some(1) };
        let fast = PeerStatus::Free { speed: Some(500) };
        let views = positions(&[slow, fast, fast, fast]);
        let plan = Some(&Needs(3));

        let choice = can_repair(&entry, &views, plan, secs(0), &waits());

        let helpers = given(choice).expect("repair gives helpers");
        assert_eq!(positions_of(&helpers), vec![1, 2, 3]);
    }

    // recovery takes the k fastest free positions, waits on busy ones, and
    // can't go with fewer than k usable
    #[test]
    fn recovery_peers() {
        let fast = PeerStatus::Free { speed: Some(900) };
        let slow = PeerStatus::Free { speed: Some(10) };
        let gone = PeerStatus::Gone { can_probe: false };

        let recovery = |statuses: &[PeerStatus]| {
            can_recover(&positions(statuses), SMALL)
        };

        let fastest = recovery(&[slow, fast, fast, fast]);
        let busy = recovery(&[FREE, FREE, PeerStatus::Busy]);
        let short = recovery(&[FREE, FREE, gone, gone]);

        let peers = given(fastest).expect("recovery gives peers");
        assert_eq!(positions_of(&peers), vec![1, 2, 3]);
        assert_eq!(busy, Choice::Busy);
        assert_eq!(short, Choice::NotNow);
    }

    // pick_peer gives the fastest free peer, never measured first, then busy,
    // then not_now
    #[test]
    fn pick_fastest() {
        let speed = Some(5);
        let measured = view(1, PeerStatus::Free { speed });
        let unseen = view(2, FREE);
        let busy = view(3, PeerStatus::Busy);
        let resting = view(4, PeerStatus::Resting);

        assert_eq!(pick_peer(&[measured, unseen]), Choice::Give(address(2)));
        assert_eq!(pick_peer(&[busy, resting]), Choice::Busy);
        assert_eq!(pick_peer(&[resting]), Choice::NotNow);
    }
}
