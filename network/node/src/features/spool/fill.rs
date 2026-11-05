//! Filling the missing_list, the certify hook's entry, and attesting
//!
//! Filling finds what was on a spool when we took it over: it reads the
//! spool's track list a page at a time and adds an entry, handover = yes,
//! for each track missing its track_data or our slice. The certify hook adds
//! entries for fresh uploads, handover = no.

use std::ops::Range;

use tape_core::erasure::leaf_position;
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup};
use tape_core::system::SpoolState;
use tape_core::track::types::CompressedTrack;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_store::ops::{
    track_has_left, GroupTrack, SliceOps, SpoolOps, TapeOps, TrackDataOps,
    TrackOps,
};
use tape_store::types::{Entry, EntryKey, WallTime};
use tape_store::TapeStore;
use tape_store_api::Store;

use crate::features::spool::error::SpoolError;

/// A stored spool and its row
pub type StoredRow = (SpoolIndex, SpoolState);

/// Whether this node lacks a track's track_data or, for a coded track, the
/// slice its stored spool keeps
pub fn is_missing<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    track: Address,
    info: &CompressedTrack,
) -> Result<bool, SpoolError> {
    if !store.has_track_data(track).map_err(store_error)? {
        return Ok(true);
    }
    if info.is_inline() {
        return Ok(false);
    }
    Ok(!store.has_slice(spool, track).map_err(store_error)?)
}

/// The certify hook's entry: when we own the spool and lack the track's
/// track_data or our slice, an entry whose first attempt is the upload.
/// An entry filling already added stays as it is.
pub fn add_upload_entry<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    track: Address,
    info: &CompressedTrack,
    now: WallTime,
) -> Result<(), SpoolError> {
    let Some(row) = store.get_spool_state(spool).map_err(store_error)? else {
        return Ok(());
    };
    if row.is_locked() || !is_missing(store, spool, track, info)? {
        return Ok(());
    }
    if store.has_entry(spool, track).map_err(store_error)? {
        return Ok(());
    }
    let key = EntryKey { spool, is_handover: false, track };
    store.put_entry(key, Entry::uploaded(now)).map_err(store_error)
}

/// Add one page of a live spool's tracks to its missing_list, while any of
/// its stored spools is still filling
///
/// The page and the rows it moves on go in one write, so a crash reads the
/// page again.
pub fn fill_missing_list<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    fold: GroupFold,
    page: usize,
) -> Result<(), SpoolError> {
    let rows = store.live_spool_states(spool, fold).map_err(store_error)?;
    let runs = filling_runs(&rows);
    if runs.is_empty() {
        return Ok(());
    }

    let start = fill_start(&rows);
    let read = store.group_tracks(&runs, start, page);
    let tracks = read.map_err(store_error)?;
    let position = leaf_position(spool).as_usize();
    let entries = missing_entries(store, position, &tracks)?;
    let moved = moved_rows(&rows, &tracks, page);
    store.write_fill(&entries, &moved).map_err(store_error)
}

/// Ready to attest: filling is done and no handover entry is left
pub fn ready_to_attest<Db: Store>(
    store: &TapeStore<Db>,
    spool: SpoolIndex,
    fold: GroupFold,
) -> Result<bool, SpoolError> {
    let rows = store.live_spool_states(spool, fold).map_err(store_error)?;
    let carried = fold.carried_count(GroupIndex::containing(spool));
    if rows.len() as u64 != carried {
        return Ok(false);
    }
    for (_, row) in &rows {
        if row.is_filling || row.is_locked() {
            return Ok(false);
        }
    }
    let has_handover = store.has_handover_entries(spool, fold);
    Ok(!has_handover.map_err(store_error)?)
}

/// The runs of consecutive virtual groups whose rows still fill
pub fn filling_runs(rows: &[StoredRow]) -> Vec<Range<VirtualGroup>> {
    let mut runs: Vec<Range<VirtualGroup>> = Vec::new();
    for (spool, row) in rows {
        if !row.is_filling {
            continue;
        }
        let group = VirtualGroup::containing(*spool);
        let next = VirtualGroup(group.0 + 1);
        match runs.last_mut() {
            Some(run) if run.end == group => run.end = next,
            Some(_) | None => runs.push(group..next),
        }
    }
    runs
}

/// Where the next page starts: after the first filling row's fill_position
fn fill_start(rows: &[StoredRow]) -> Option<GroupTrack> {
    for (spool, row) in rows {
        if !row.is_filling {
            continue;
        }
        if row.fill_position == Address::default() {
            return None;
        }
        let group = VirtualGroup::containing(*spool);
        return Some(GroupTrack { group, track: row.fill_position });
    }
    None
}

/// The rows a page moves on: a short page ends every filling row; a full
/// one ends the rows before its last track's group and moves that group's
/// fill_position to the last track read
pub fn moved_rows(
    rows: &[StoredRow],
    tracks: &[GroupTrack],
    page: usize,
) -> Vec<StoredRow> {
    let last = match tracks.last() {
        Some(last) if tracks.len() >= page => Some(*last),
        Some(_) | None => None,
    };
    let mut moved = Vec::new();
    for (spool, row) in rows {
        if !row.is_filling {
            continue;
        }
        if let Some(row) = moved_row(*spool, *row, last) {
            moved.push((*spool, row));
        }
    }
    moved
}

fn moved_row(
    spool: SpoolIndex,
    row: SpoolState,
    last: Option<GroupTrack>,
) -> Option<SpoolState> {
    let group = VirtualGroup::containing(spool);
    let Some(last) = last else {
        return Some(SpoolState { is_filling: false, ..row });
    };
    if group < last.group {
        return Some(SpoolState { is_filling: false, ..row });
    }
    if group == last.group {
        return Some(SpoolState { fill_position: last.track, ..row });
    }
    None
}

/// Handover entries for the page's tracks that are certified, still on
/// their tape, and missing their track_data or our slice
fn missing_entries<Db: Store>(
    store: &TapeStore<Db>,
    position: usize,
    tracks: &[GroupTrack],
) -> Result<Vec<EntryKey>, SpoolError> {
    let mut addresses = Vec::with_capacity(tracks.len());
    for row in tracks {
        addresses.push(row.track);
    }
    let infos = store.get_tracks(&addresses).map_err(store_error)?;

    let mut entries = Vec::new();
    for (track, info) in addresses.into_iter().zip(infos) {
        let Some(info) = info else {
            continue;
        };
        if !is_live(store, &info)? {
            continue;
        }
        let spool = info.group.spool_at(position);
        if is_missing(store, spool, track, &info)? {
            entries.push(EntryKey { spool, is_handover: true, track });
        }
    }
    Ok(entries)
}

/// A track counts while it is certified and still on its tape
pub fn is_live<Db: Store>(
    store: &TapeStore<Db>,
    info: &CompressedTrack,
) -> Result<bool, SpoolError> {
    if !info.is_certified() {
        return Ok(false);
    }
    let tape = store.get_tape(info.tape).map_err(store_error)?;
    Ok(!track_has_left(tape.as_ref(), info))
}

fn store_error(error: impl std::fmt::Display) -> SpoolError {
    SpoolError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use tape_api::program::tapedrive::TrackAddress;
    use tape_core::encoding::TAPENET_PROFILE;
    use tape_core::types::{EpochNumber, TrackNumber};
    use tape_store::types::Method;
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
        let gained = SpoolState::gained();
        let near = VirtualGroup(0).spool_at(0);
        let rows = [(near, gained), (FAR.spool_at(0), gained)];
        store.put_spool_states(&rows).expect("rows");
        let tracks = vec![
            write(&store, 0, VirtualGroup(0), true),
            write(&store, 1, VirtualGroup(0), false),
            write(&store, 2, VirtualGroup(0), true),
            write(&store, 3, FAR, true),
        ];
        (store, tracks)
    }

    fn entries(store: &TapeStore<MemoryStore>) -> Vec<EntryKey> {
        let page = store.missing_page(live(), fold(), None, 10).expect("page");
        let mut keys = Vec::new();
        for (key, _) in page {
            keys.push(key);
        }
        keys
    }

    fn is_filling(store: &TapeStore<MemoryStore>) -> bool {
        let rows = store.live_spool_states(live(), fold()).expect("rows");
        !filling_runs(&rows).is_empty()
    }

    fn info(store: &TapeStore<MemoryStore>, track: Address) -> CompressedTrack {
        store.get_track(track).expect("read").expect("track")
    }

    // filling rows of consecutive groups make one run
    #[test]
    fn runs_of_rows() {
        let done = SpoolState { is_filling: false, ..SpoolState::gained() };
        let rows = [
            (VirtualGroup(0).spool_at(2), SpoolState::gained()),
            (VirtualGroup(1).spool_at(2), SpoolState::gained()),
            (VirtualGroup(2).spool_at(2), done),
            (VirtualGroup(5).spool_at(2), SpoolState::gained()),
        ];

        let runs = filling_runs(&rows);

        let first = VirtualGroup(0)..VirtualGroup(2);
        let second = VirtualGroup(5)..VirtualGroup(6);
        assert_eq!(runs, vec![first, second]);
    }

    // filling reads a page at a time, adds handover entries for the tracks
    // that lack something, and ends once the track list runs out
    #[test]
    fn fills_by_page() {
        let (store, tracks) = owning_store();

        fill_missing_list(&store, live(), fold(), 2).expect("first page");
        assert!(is_filling(&store));
        fill_missing_list(&store, live(), fold(), 2).expect("second page");
        assert!(is_filling(&store));
        fill_missing_list(&store, live(), fold(), 2).expect("last page");

        assert!(!is_filling(&store));
        let keys = entries(&store);
        assert_eq!(keys.len(), 3);
        for key in &keys {
            assert!(key.is_handover);
            assert_ne!(key.track, tracks[1]);
        }
    }

    // the node attests once filling is done and no handover entry is left,
    // whatever fresh uploads still lack
    #[test]
    fn attest_when_filled() {
        let (store, tracks) = owning_store();
        assert!(!ready_to_attest(&store, live(), fold()).expect("ready"));

        for _ in 0..3 {
            fill_missing_list(&store, live(), fold(), 2).expect("fill");
        }
        assert!(!ready_to_attest(&store, live(), fold()).expect("ready"));

        for key in entries(&store) {
            store.delete_entry(key).expect("delete");
        }
        let spool = VirtualGroup(0).spool_at(0);
        let fresh = EntryKey { spool, is_handover: false, track: tracks[0] };
        store.put_entry(fresh, Entry::never_tried()).expect("entry");
        assert!(ready_to_attest(&store, live(), fold()).expect("ready"));
    }

    // the certify hook adds an upload entry on a spool we own that lacks the
    // slice, and leaves a lost spool, a held slice and an earlier entry alone
    #[test]
    fn upload_entry() {
        let (store, tracks) = owning_store();
        let spool = VirtualGroup(0).spool_at(0);
        let now = WallTime(9_000);
        let lacking = info(&store, tracks[0]);
        let held = info(&store, tracks[1]);

        let added = add_upload_entry(&store, spool, tracks[0], &lacking, now);
        added.expect("hook");
        let added = add_upload_entry(&store, spool, tracks[1], &held, now);
        added.expect("hook");

        let page = store.missing_page(live(), fold(), None, 10).expect("page");
        assert_eq!(page.len(), 1);
        let (key, entry) = page[0];
        assert!(!key.is_handover);
        assert_eq!(entry.last_attempt.expect("upload").method, Method::Upload);

        let locked_at = EpochNumber(5);
        let locked = SpoolState { locked_at, ..SpoolState::gained() };
        store.put_spool_states(&[(spool, locked)]).expect("lock");
        let lost = info(&store, tracks[2]);
        add_upload_entry(&store, spool, tracks[2], &lost, now).expect("hook");
        assert!(!store.has_entry(spool, tracks[2]).expect("entry"));
    }
}
