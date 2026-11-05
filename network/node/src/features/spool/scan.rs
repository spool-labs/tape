//! The scan builds a spool's missing_list, the list on disk of the spool's
//! tracks whose data we don't have yet. After we gain a spool, the only way to
//! find out what we are missing is to look at every track on it, so the scan
//! reads the spool's tracks a page at a time. When a new upload is certified
//! and part of it never reached us, we add it to the list as well.
//!
//! ```text
//! each time a turn reaches the end of the spool's missing_list:
//!   read the next 512 of the spool's tracks
//!   add each track that is missing its track_data or our slice
//!   remember how far we got
//! (we scan only one page each time, so the list grows only as fast as the
//! fetches clear it)
//!
//! when a track is certified on a spool we own and we are missing part of it:
//!   add it to the list as an upload
//! ```

use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, VirtualGroup};
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_recover::limits::SpoolLimits;
use tape_recover::state::{
    get_groups_to_scan, get_scan_position, get_states_after_page,
};
use tape_recover::time::WallTime;
use tape_recover::track::MissingTrack;
use tape_store::ops::{
    track_has_left, SliceOps, SpoolOps, TapeOps, TrackDataOps, TrackOps,
};
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::spool::error::SpoolError;

/// Add one page of a live spool's tracks to its missing_list, while any of
/// the stored spools it carries is still scanning
///
/// The page and the states it moves on go in one write, so a crash reads
/// the page again.
pub fn scan_next_page<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    fold: GroupFold,
    limits: &SpoolLimits,
) -> Result<(), SpoolError> {
    let states = store.get_live_spool_states(spool, fold)?;
    let groups = get_groups_to_scan(&states);
    if groups.is_empty() {
        return Ok(());
    }

    let after = get_scan_position(&states);
    let page = store.get_group_tracks(&groups, after, limits.scan_page)?;
    let position = leaf_position(spool).as_usize();
    let now = WallTime::now();
    let missing = get_missing_on_page(store, position, &page, now)?;
    let moved = get_states_after_page(&states, &page, limits.scan_page);
    Ok(store.put_scan_page(&missing, &moved)?)
}

/// The certify hook: when we own the spool and lack the track's track_data
/// or our slice, an upload missing track added at `now`, when the track was
/// certified. A missing track the scan already added stays as it is.
pub fn add_uploaded_track<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    address: Address,
    track: &CompressedTrack,
    now: WallTime,
) -> Result<(), SpoolError> {
    let Some(state) = store.get_spool_state(spool)? else {
        return Ok(());
    };
    if state.is_lost() || !is_track_missing(store, spool, address, track)? {
        return Ok(());
    }
    if store.has_missing_track(spool, address)? {
        return Ok(());
    }
    let uploaded = MissingTrack::new_upload(spool, address, now);
    Ok(store.put_missing_track(&uploaded)?)
}

/// Whether this node lacks a track's track_data or, for a coded track, the
/// slice its stored spool keeps
pub fn is_track_missing<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    address: Address,
    track: &CompressedTrack,
) -> Result<bool, SpoolError> {
    if !store.has_track_data(address)? {
        return Ok(true);
    }
    if track.is_inline() {
        return Ok(false);
    }
    Ok(!store.has_slice(spool, address)?)
}

/// A track counts while it is certified and still on its tape
pub fn is_track_live<Db: Store>(
    store: &TapeStore<Db>,
    track: &CompressedTrack,
) -> Result<bool, SpoolError> {
    if !track.is_certified() {
        return Ok(false);
    }
    let tape = store.get_tape(track.tape)?;
    Ok(!track_has_left(tape.as_ref(), track))
}

/// Handover missing tracks, added at `now`, for the page's tracks that are
/// live and missing their track_data or our slice
fn get_missing_on_page<Db: Store>(
    store: &TapeStore<Db>,
    position: usize,
    page: &[(VirtualGroup, Address)],
    now: WallTime,
) -> Result<Vec<MissingTrack>, SpoolError> {
    let mut addresses = Vec::with_capacity(page.len());
    for (_, address) in page {
        addresses.push(*address);
    }
    let tracks = store.get_tracks(&addresses)?;

    let mut missing = Vec::new();
    for (address, track) in addresses.into_iter().zip(tracks) {
        let Some(track) = track else {
            continue;
        };
        if !is_track_live(store, &track)? {
            continue;
        }
        let spool = track.group.spool_at(position);
        if is_track_missing(store, spool, address, &track)? {
            let handover = MissingTrack::new_handover(spool, address, now);
            missing.push(handover);
        }
    }
    Ok(missing)
}

#[cfg(test)]
mod tests {
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::spooler::GroupIndex;
    use tape_core::types::{EpochNumber, TrackNumber};
    use tape_recover::state::{is_ready_to_attest, SpoolState};
    use tape_recover::track::TrackOrigin;
    use tape_store_memory::MemoryStore;

    use super::*;
    use crate::features::challenge::fixture::{
        coded, payload, put_tape, test_store, write_coded,
    };

    const SEED: u8 = 3;

    /// With this many live groups, group 0 carries virtual groups 0 and 1024
    const LIVE: u64 = 1_024;

    const FAR: VirtualGroup = VirtualGroup(1_024);

    fn fold() -> GroupFold {
        GroupFold::new(LIVE).expect("fold")
    }

    fn live() -> SpoolIndex {
        GroupIndex(0).spool_at(0)
    }

    /// Limits whose scan reads two tracks a page
    fn limits() -> SpoolLimits {
        SpoolLimits { scan_page: 2, ..SpoolLimits::default() }
    }

    /// Write a coded track at a virtual group, without our slice when it
    /// lacks one
    fn write(
        store: &TapeStore<MemoryStore>,
        number: u64,
        group: VirtualGroup,
        is_lacking: bool,
    ) -> Address {
        let data = payload(SEED, number, 2_000);
        let track = coded(TAPENET_PROFILE, SEED, number, group, &data);
        write_coded(store, &track, EpochNumber(1));
        let address = track.track.address();
        if is_lacking {
            store.delete_slice(group.spool_at(0), address).expect("drop slice");
        }
        address
    }

    /// A store owning group 0's spools at position 0, with four tracks
    fn owning_store() -> (TapeStore<MemoryStore>, Vec<Address>) {
        let store = test_store();
        put_tape(&store, SEED, EpochNumber(100), TrackNumber(0));
        let scanning = SpoolState::new_scanning();
        let near = VirtualGroup(0).spool_at(0);
        let states = [(near, scanning), (FAR.spool_at(0), scanning)];
        store.put_spool_states(&states).expect("states");
        let tracks = vec![
            write(&store, 0, VirtualGroup(0), true),
            write(&store, 1, VirtualGroup(0), false),
            write(&store, 2, VirtualGroup(0), true),
            write(&store, 3, FAR, true),
        ];
        (store, tracks)
    }

    fn listed(store: &TapeStore<MemoryStore>) -> Vec<MissingTrack> {
        let page = store.get_missing_tracks(live(), fold(), None, 10);
        page.expect("page")
    }

    fn is_scanning(store: &TapeStore<MemoryStore>) -> bool {
        let states = store.get_live_spool_states(live(), fold());
        !get_groups_to_scan(&states.expect("states")).is_empty()
    }

    fn is_ready(store: &TapeStore<MemoryStore>) -> bool {
        let states = store.get_live_spool_states(live(), fold());
        let has_handover = store.has_handover_tracks(live(), fold());
        let carried = fold().carried_count(GroupIndex(0));
        let states = states.expect("states");
        is_ready_to_attest(&states, carried, has_handover.expect("handover"))
    }

    fn track_of(store: &TapeStore<MemoryStore>, address: Address) -> CompressedTrack {
        store.get_track(address).expect("read").expect("track")
    }

    // the scan reads a page at a time, adds handover missing tracks for the
    // tracks that lack something, and ends once the track list runs out
    #[test]
    fn scans_by_page() {
        let (store, tracks) = owning_store();

        scan_next_page(&store, live(), fold(), &limits()).expect("first page");
        assert!(is_scanning(&store));
        scan_next_page(&store, live(), fold(), &limits()).expect("second page");
        assert!(is_scanning(&store));
        scan_next_page(&store, live(), fold(), &limits()).expect("last page");

        assert!(!is_scanning(&store));
        let listed = listed(&store);
        assert_eq!(listed.len(), 3);
        for missing in &listed {
            assert_eq!(missing.origin, TrackOrigin::Handover);
            assert_ne!(missing.track, tracks[1]);
        }
    }

    // the node attests once the scan is done and no handover missing track is
    // left, whatever fresh uploads still lack
    #[test]
    fn attest_when_scanned() {
        let (store, tracks) = owning_store();
        assert!(!is_ready(&store));

        for _ in 0..3 {
            scan_next_page(&store, live(), fold(), &limits()).expect("scan");
        }
        assert!(!is_ready(&store));

        for missing in listed(&store) {
            store.delete_missing_track(&missing).expect("delete");
        }
        let spool = VirtualGroup(0).spool_at(0);
        let fresh = MissingTrack::new_upload(spool, tracks[0], WallTime(1));
        store.put_missing_track(&fresh).expect("missing track");
        assert!(is_ready(&store));
    }

    // the certify hook adds an upload missing track on a spool we own that
    // lacks the slice, and leaves a lost spool, a held slice and an earlier
    // missing track alone
    #[test]
    fn uploaded_track() {
        let (store, tracks) = owning_store();
        let spool = VirtualGroup(0).spool_at(0);
        let now = WallTime(9_000);
        let lacking = track_of(&store, tracks[0]);
        let held = track_of(&store, tracks[1]);

        let added = add_uploaded_track(&store, spool, tracks[0], &lacking, now);
        added.expect("hook");
        let added = add_uploaded_track(&store, spool, tracks[1], &held, now);
        added.expect("hook");

        let page = listed(&store);
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].origin, TrackOrigin::Upload);
        assert_eq!(page[0].added_at, now);
        assert!(page[0].last_attempt.is_none());

        let lost_at = EpochNumber(5);
        let lost = SpoolState { lost_at, ..SpoolState::new_scanning() };
        store.put_spool_states(&[(spool, lost)]).expect("lose");
        let later = track_of(&store, tracks[2]);
        add_uploaded_track(&store, spool, tracks[2], &later, now).expect("hook");
        assert!(!store.has_missing_track(spool, tracks[2]).expect("listed"));
    }
}
