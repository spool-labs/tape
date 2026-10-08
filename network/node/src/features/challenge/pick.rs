//! Picks the track a spool has to prove in a round

use tape_core::spooler::{GroupFold, GroupIndex};
use tape_core::track::types::CompressedTrack;
use tape_core::types::EpochNumber;
use tape_crypto::Address;
use tape_store::ops::{ObjectInfoOps, TrackOps};
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::assignment::size::is_active_track;
use crate::features::challenge::error::ChallengeError;

/// Each step of the walk reads this many track rows
const PICK_SCAN_BATCH: usize = 32;

/// A round picked this track for a spool
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pick {
    pub address: Address,
    pub track: CompressedTrack,
}

/// Pick the first track of the previous epoch's set, in `group`, after `start` in address order
///
/// The walk wraps once and ends at `start`. `epoch` is the chain's current
/// epoch, and `fold` is the one the answering holders live under. Nothing
/// comes back for a group with no track in the set.
pub fn pick_track<Db: Store>(
    store: &TapeStore<Db>,
    start: Address,
    group: GroupIndex,
    fold: GroupFold,
    epoch: EpochNumber,
) -> Result<Option<Pick>, ChallengeError> {
    let mut cursor = Some(start);
    let mut has_wrapped = false;

    loop {
        let page = store
            .iter_tracks_from(cursor, PICK_SCAN_BATCH)
            .map_err(store_error)?;

        for (address, track) in &page {
            if has_wrapped && *address > start {
                return Ok(None);
            }
            if is_in_set(store, *address, track, group, fold, epoch)? {
                return Ok(Some(Pick { address: *address, track: *track }));
            }
        }

        match page.last() {
            Some((last, _)) if page.len() == PICK_SCAN_BATCH => cursor = Some(*last),
            Some(_) | None if has_wrapped => return Ok(None),
            Some(_) | None => {
                has_wrapped = true;
                cursor = None;
            }
        }
    }
}

/// Whether `group` has any track in the previous epoch's set
///
/// With none, this walks the whole track index before it can say so.
pub fn has_tracks<Db: Store>(
    store: &TapeStore<Db>,
    group: GroupIndex,
    fold: GroupFold,
    epoch: EpochNumber,
) -> Result<bool, ChallengeError> {
    let first = pick_track(store, Address::default(), group, fold, epoch)?;

    Ok(first.is_some())
}

/// Whether a track belongs to `group` and to the set the assignment sizing counts
fn is_in_set<Db: Store>(
    store: &TapeStore<Db>,
    address: Address,
    track: &CompressedTrack,
    group: GroupIndex,
    fold: GroupFold,
    epoch: EpochNumber,
) -> Result<bool, ChallengeError> {
    // A challenge stays inside its group, so a row of another group costs no read.
    if fold.live_group(track.group) != group {
        return Ok(false);
    }

    let info = store.get_object_info(address).map_err(store_error)?;
    let is_active = is_active_track(store, address, track, info, epoch, epoch.next())?;

    Ok(is_active)
}

fn store_error(error: impl std::fmt::Display) -> ChallengeError {
    ChallengeError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tape_core::erasure::MAX_GROUP_SIZE;
    use tape_core::snapshot::replay::ReplayableEvent;
    use tape_core::spooler::VirtualGroup;
    use tape_core::track::types::TrackState;
    use tape_core::types::{RoundNumber, SpoolIndex, TrackNumber};
    use tape_crypto::Hash;
    use tape_store_memory::MemoryStore;

    use crate::config::store::GcConfig;
    use crate::features::assignment::size::{group_weights, GroupWeights};
    use crate::features::block::pending_tracks::PendingTracks;
    use crate::features::challenge::fixture::{
        apply, every_case, fill, put_tape, rewind, tape_at, tape_number, test_store, write_inline,
        Case, EPOCH, PROFILE, VIRTUAL_GROUPS,
    };
    use crate::features::challenge::sample::Sample;
    use crate::features::gc::sweep::sweep_epoch;

    use super::*;

    const ROUNDS: u64 = 40;
    const FILLER_TAPE: u8 = 100;
    const DELETED_TAPE: u8 = 101;
    const CLOSED_TAPE: u8 = 102;
    const FILLER_TRACKS: u64 = 24;

    fn fold(groups: u64) -> GroupFold {
        GroupFold::new(groups).expect("live groups")
    }

    fn seed(round: u64) -> Hash {
        Hash::from([round as u8; 32])
    }

    fn pick(
        store: &TapeStore<MemoryStore>,
        start: Address,
        group: u64,
        groups: u64,
    ) -> Option<Pick> {
        pick_track(store, start, GroupIndex(group), fold(groups), EPOCH).expect("pick")
    }

    /// List every track the challenge can pick for a group, walking from each pick to the next
    fn set_of(store: &TapeStore<MemoryStore>, group: u64, groups: u64) -> Vec<Address> {
        let mut tracks = Vec::new();
        let mut start = Address::from([0u8; 32]);

        while let Some(found) = pick(store, start, group, groups) {
            if tracks.contains(&found.address) {
                break;
            }
            tracks.push(found.address);
            start = found.address;
        }

        tracks.sort();
        tracks
    }

    /// List every track the assignment sizing counts for a group
    fn sized(store: &TapeStore<MemoryStore>, group: u64, groups: u64) -> Vec<Address> {
        let weights = match group_weights(store, EPOCH, EPOCH.next(), fold(groups), PROFILE)
            .expect("weights build")
        {
            GroupWeights::Ready(weights) => Some(weights),
            GroupWeights::Pending { .. } => None,
        };

        let mut tracks = Vec::new();
        for active in weights.expect("weights ready").tracks {
            if active.group == GroupIndex(group) {
                tracks.push(active.track);
            }
        }
        tracks.sort();
        tracks
    }

    /// Build a store with every case and three tapes of tracks from the set
    fn full_store() -> (TapeStore<MemoryStore>, Vec<Case>) {
        let store = test_store();
        let mut cases = every_case(&store);
        for tape in [FILLER_TAPE, DELETED_TAPE, CLOSED_TAPE] {
            cases.extend(fill(&store, tape, FILLER_TRACKS));
        }

        (store, cases)
    }

    async fn sweep(store: &TapeStore<MemoryStore>) {
        let config = GcConfig {
            enabled: true,
            interval_secs: 60,
            track_batch: 8,
            slice_batch: 8,
            reclaim_min_deleted_slices: 20,
        };
        sweep_epoch(store, &config, EPOCH, &HashSet::new(), &PendingTracks::new(), true, PROFILE)
            .await
            .expect("sweep");
    }

    /// Apply what the current epoch does to a store: deletes, a close, a rewind and new writes
    fn churn(store: &TapeStore<MemoryStore>, cases: &[Case]) {
        for case in cases {
            if case.track.tape == tape_at(DELETED_TAPE) && case.track.track_number.0 % 2 == 0 {
                apply(store, ReplayableEvent::DeleteTrack { track: case.address, epoch: EPOCH });
            }
        }

        let closed = ReplayableEvent::CloseTape {
            tape: tape_at(CLOSED_TAPE),
            id: tape_number(CLOSED_TAPE),
            epoch: EPOCH,
        };
        apply(store, closed);
        apply(store, rewind(FILLER_TAPE, FILLER_TRACKS));

        for number in FILLER_TRACKS..FILLER_TRACKS + 8 {
            let group = VirtualGroup(number % VIRTUAL_GROUPS);
            write_inline(store, FILLER_TAPE, number, group, TrackState::Certified, EPOCH);
        }
    }

    // the challenge picks from the tracks the assignment sizing counts, group by group
    #[test]
    fn sizing_set() {
        let (store, cases) = full_store();

        for groups in [1, 2, 4] {
            let mut total = 0;
            for group in 0..groups {
                let picked = set_of(&store, group, groups);

                assert_eq!(picked, sized(&store, group, groups), "group {group} of {groups}");
                total += picked.len();
            }

            let in_set = cases
                .iter()
                .filter(|case| case.is_in_set)
                .count();
            assert_eq!(total, in_set, "{groups} groups");
        }
    }

    // a store the gc has swept, and that has applied this epoch's deletes, close,
    // rewind and writes, gives the picks of a store that has done neither
    #[tokio::test]
    async fn two_stores() {
        let (fresh, _) = full_store();
        let (swept, cases) = full_store();

        sweep(&swept).await;
        churn(&swept, &cases);
        sweep(&swept).await;

        let fresh_rows = fresh.count_tracks().expect("count");
        let swept_rows = swept.count_tracks().expect("count");
        assert_ne!(fresh_rows, swept_rows);

        let groups = 2;
        let mut picked = HashSet::new();
        for round in 1..=ROUNDS {
            for group in 0..groups {
                for position in 0..MAX_GROUP_SIZE {
                    let spool = GroupIndex(group).spool_at(position);
                    let sample = Sample::new(EPOCH, RoundNumber(round), spool, seed(round));

                    let expected = pick(&fresh, sample.start(), group, groups);
                    let found = pick(&swept, sample.start(), group, groups);

                    assert!(expected.is_some());
                    assert_eq!(
                        found.map(|pick| pick.address),
                        expected.map(|pick| pick.address),
                        "round {round} {spool}"
                    );
                    picked.extend(found.map(|pick| pick.address));
                }
            }
        }

        assert!(picked.len() > 20);
    }

    // a spool is only asked for tracks its own group carries
    #[test]
    fn own_group() {
        let (store, _) = full_store();

        for groups in [2, 4] {
            for round in 1..=ROUNDS {
                for group in 0..groups {
                    let spool = GroupIndex(group).spool_at(0);
                    let sample = Sample::new(EPOCH, RoundNumber(round), spool, seed(round));

                    let found = pick(&store, sample.start(), group, groups).expect("a track");

                    assert_eq!(fold(groups).live_group(found.track.group), GroupIndex(group));
                }
            }
        }
    }

    // the fold decides which virtual groups a group answers for
    #[test]
    fn follows_fold() {
        let store = test_store();
        let cases = fill(&store, FILLER_TAPE, FILLER_TRACKS);
        let carried: Vec<Address> = cases
            .iter()
            .filter(|case| case.track.group == VirtualGroup(2))
            .map(|case| case.address)
            .collect();

        let under_two = set_of(&store, 0, 2);
        let under_four = set_of(&store, 0, 4);

        assert!(carried.iter().all(|track| under_two.contains(track)));
        assert!(carried.iter().all(|track| !under_four.contains(track)));
        assert_eq!(set_of(&store, 2, 4).len(), carried.len());
        assert!(pick(&store, Address::from([0u8; 32]), 2, 2).is_none());
    }

    // the walk goes up in address order, passes the end once and stops where it began
    #[test]
    fn wraps() {
        let store = test_store();
        fill(&store, FILLER_TAPE, FILLER_TRACKS);
        let tracks = set_of(&store, 0, 1);
        let lowest = tracks[0];
        let highest = tracks[tracks.len() - 1];

        let after_lowest = pick(&store, lowest, 0, 1).expect("a track");
        let past_end = pick(&store, highest, 0, 1).expect("a track");
        let from_top = pick(&store, Address::from([0xFF; 32]), 0, 1).expect("a track");

        assert_eq!(after_lowest.address, tracks[1]);
        assert_eq!(past_end.address, lowest);
        assert_eq!(from_top.address, lowest);
    }

    // a group with one track in the set gets that track from any start
    #[test]
    fn lone_track() {
        let store = test_store();
        put_tape(&store, FILLER_TAPE, EPOCH.next(), TrackNumber(0));
        let only = write_inline(
            &store,
            FILLER_TAPE,
            0,
            VirtualGroup(0),
            TrackState::Certified,
            EPOCH.prev(),
        );

        for start in [Address::from([0u8; 32]), only.address, Address::from([0xFF; 32])] {
            let found = pick(&store, start, 0, 1).expect("a track");

            assert_eq!(found.address, only.address);
        }
    }

    // a group with rows and nothing in the set has no pick
    #[test]
    fn empty_group() {
        let store = test_store();
        put_tape(&store, FILLER_TAPE, EPOCH.next(), TrackNumber(0));
        for number in 0..FILLER_TRACKS * 2 {
            write_inline(&store, FILLER_TAPE, number, VirtualGroup(0), TrackState::Certified, EPOCH);
        }

        assert!(pick(&store, Address::from([0x80; 32]), 0, 1).is_none());
        assert!(pick(&test_store(), Address::from([0x80; 32]), 0, 1).is_none());
    }

    // every spool of a group gets its own sample, so they are spread over the set
    #[test]
    fn spread() {
        let (store, _) = full_store();
        let mut picked = HashSet::new();

        for position in 0..MAX_GROUP_SIZE {
            let spool: SpoolIndex = GroupIndex(0).spool_at(position);
            let sample = Sample::new(EPOCH, RoundNumber(1), spool, seed(1));
            let found = pick(&store, sample.start(), 0, 2).expect("a track");
            picked.insert(found.address);
        }

        assert!(picked.len() > MAX_GROUP_SIZE / 2);
    }
}
