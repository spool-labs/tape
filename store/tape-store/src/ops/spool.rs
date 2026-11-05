//! The spool records: spool_states, the missing_list and the owner_history
//!
//! Every record is kept by stored spool. A live spool is a set of stored
//! spools, one per virtual group it carries, and a read that answers for a
//! live spool walks its runs of virtual groups, never one group at a time.

use std::ops::Range;

use tape_core::erasure::{leaf_position, MAX_GROUP_SIZE};
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup, VIRTUAL_GROUP_COUNT};
use tape_core::system::{PackedSpoolState, SpoolState};
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_store_api::{Column, Direction, KeyValue, Store, WriteBatch};

use crate::columns::{SpoolMissingTrackCol, SpoolOwnerHistoryCol, SpoolStateCol, TrackGroupIndexCol};
use crate::error::{Result, TapeStoreError};
use crate::types::missing::{
    entry_prefix, group_bytes, stored_group, stored_position, Entry, EntryKey, PackedEntry,
};
use crate::TapeStore;

/// How many stored spools there are, and so the most spool rows a store holds
pub const STORED_SPOOL_COUNT: usize = VIRTUAL_GROUP_COUNT as usize * MAX_GROUP_SIZE;

/// One track of a virtual group's track list
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupTrack {
    pub group: VirtualGroup,
    pub track: Address,
}

/// The keys from `start` up to `end`
struct KeyRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

/// A live spool as the store keeps it: one position, a few runs of groups
struct LiveRuns {
    position: u8,
    runs: Vec<Range<VirtualGroup>>,
}

/// Operations on the spool records
pub trait SpoolOps {
    /// A stored spool's row
    fn get_spool_state(&self, spool: SpoolIndex) -> Result<Option<SpoolState>>;

    /// Write stored spools' rows in one batch
    fn put_spool_states(&self, rows: &[(SpoolIndex, SpoolState)]) -> Result<()>;

    /// Delete a stored spool's row
    fn delete_spool_state(&self, spool: SpoolIndex) -> Result<()>;

    /// The rows of the stored spools a live spool carries, in key order
    fn live_spool_states(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    /// Up to `limit` of every row the store holds, from the row after `after`
    fn spool_states(
        &self,
        after: Option<SpoolIndex>,
        limit: usize,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    /// Up to `limit` entries of a live spool's missing_list after `after`
    fn missing_page(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
        after: Option<EntryKey>,
        limit: usize,
    ) -> Result<Vec<(EntryKey, Entry)>>;

    /// Whether a stored spool's missing_list has an entry for a track
    fn has_entry(&self, spool: SpoolIndex, track: Address) -> Result<bool>;

    /// Write one entry
    fn put_entry(&self, key: EntryKey, entry: Entry) -> Result<()>;

    /// Delete one entry
    fn delete_entry(&self, key: EntryKey) -> Result<()>;

    /// Whether any entry filling added is left in a live spool's missing_list
    fn has_handover_entries(&self, spool: SpoolIndex, fold: GroupFold) -> Result<bool>;

    /// Delete up to `limit` entries of a stored spool, returning how many went
    fn delete_spool_entries(&self, spool: SpoolIndex, limit: usize) -> Result<usize>;

    /// Up to `limit` tracks of runs of virtual groups, from the track after `after`
    fn group_tracks(
        &self,
        runs: &[Range<VirtualGroup>],
        after: Option<GroupTrack>,
        limit: usize,
    ) -> Result<Vec<GroupTrack>>;

    /// Add the entries filling found that aren't there yet, never tried, and
    /// write the rows it moved on, in one write
    fn write_fill(&self, entries: &[EntryKey], rows: &[(SpoolIndex, SpoolState)]) -> Result<()>;

    /// Record an epoch's owners, keyed by each live group's own stored spools
    fn put_owners(&self, epoch: EpochNumber, owners: &[(SpoolIndex, Address)]) -> Result<()>;

    /// The owners, by position, of the live group that carried a virtual
    /// group in an epoch, or nothing for an epoch the history doesn't hold
    fn group_owners_at(
        &self,
        epoch: EpochNumber,
        group: VirtualGroup,
        width: usize,
    ) -> Result<Option<Vec<Address>>>;

    /// Forget every epoch before `epoch`
    fn forget_owners_before(&self, epoch: EpochNumber) -> Result<()>;

    /// Every node the history records, reading up to `limit` records
    fn history_owners(&self, limit: usize) -> Result<Vec<Address>>;
}

impl<S: Store> SpoolOps for TapeStore<S> {
    fn get_spool_state(&self, spool: SpoolIndex) -> Result<Option<SpoolState>> {
        let key = row_key(spool)?;
        let raw = self.inner().inner();
        let Some(value) = raw.get(SpoolStateCol::CF_NAME, &key)? else {
            return Ok(None);
        };
        Ok(Some(decode_row(&value)?))
    }

    fn put_spool_states(&self, rows: &[(SpoolIndex, SpoolState)]) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (spool, row) in rows {
            let key = row_key(*spool)?;
            batch.put_owned(SpoolStateCol::CF_NAME, key.to_vec(), row.pack().to_vec());
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn delete_spool_state(&self, spool: SpoolIndex) -> Result<()> {
        let key = row_key(spool)?;
        self.inner().inner().delete(SpoolStateCol::CF_NAME, &key)?;
        Ok(())
    }

    fn live_spool_states(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let live = live_runs(spool, fold);
        let mut rows = Vec::new();
        for run in &live.runs {
            let range = row_range(live.position, run)?;
            let width = (run.end.0 - run.start.0) as usize;
            for (key, value) in read_range(self, SpoolStateCol::CF_NAME, &range, &[], width)? {
                rows.push((row_spool(&key)?, decode_row(&value)?));
            }
        }
        Ok(rows)
    }

    fn spool_states(
        &self,
        after: Option<SpoolIndex>,
        limit: usize,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let after_key = match after {
            Some(spool) => row_key(spool)?.to_vec(),
            None => Vec::new(),
        };
        let range = KeyRange { start: Vec::new(), end: vec![u8::MAX; 4] };
        let mut rows = Vec::new();
        for (key, value) in read_range(self, SpoolStateCol::CF_NAME, &range, &after_key, limit)? {
            rows.push((row_spool(&key)?, decode_row(&value)?));
        }
        Ok(rows)
    }

    fn missing_page(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
        after: Option<EntryKey>,
        limit: usize,
    ) -> Result<Vec<(EntryKey, Entry)>> {
        let after_key = match after {
            Some(key) => key.pack()?.to_vec(),
            None => Vec::new(),
        };
        let mut page = Vec::new();
        for range in entry_ranges(live_runs(spool, fold))? {
            let left = limit - page.len();
            for (key, value) in read_range(self, SpoolMissingTrackCol::CF_NAME, &range, &after_key, left)? {
                page.push((EntryKey::unpack(&key)?, decode_entry(&value)?));
            }
            if page.len() == limit {
                break;
            }
        }
        Ok(page)
    }

    fn has_entry(&self, spool: SpoolIndex, track: Address) -> Result<bool> {
        let raw = self.inner().inner();
        for is_handover in [false, true] {
            let key = EntryKey { spool, is_handover, track }.pack()?;
            if raw.contains(SpoolMissingTrackCol::CF_NAME, &key)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn put_entry(&self, key: EntryKey, entry: Entry) -> Result<()> {
        let raw = self.inner().inner();
        raw.put(SpoolMissingTrackCol::CF_NAME, &key.pack()?, &entry.pack())?;
        Ok(())
    }

    fn delete_entry(&self, key: EntryKey) -> Result<()> {
        self.inner().inner().delete(SpoolMissingTrackCol::CF_NAME, &key.pack()?)?;
        Ok(())
    }

    fn has_handover_entries(&self, spool: SpoolIndex, fold: GroupFold) -> Result<bool> {
        let live = live_runs(spool, fold);
        for run in &live.runs {
            let range = entry_range(live.position, true, run)?;
            if !read_range(self, SpoolMissingTrackCol::CF_NAME, &range, &[], 1)?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn delete_spool_entries(&self, spool: SpoolIndex, limit: usize) -> Result<usize> {
        let mut batch = WriteBatch::new();
        let mut count = 0;
        for is_handover in [false, true] {
            let prefix = entry_prefix(spool, is_handover)?;
            let range = KeyRange { start: prefix.to_vec(), end: prefix_end(&prefix) };
            for (key, _) in read_range(self, SpoolMissingTrackCol::CF_NAME, &range, &[], limit - count)? {
                batch.delete_owned(SpoolMissingTrackCol::CF_NAME, key);
                count += 1;
            }
        }
        if count > 0 {
            self.inner().inner().write_batch(batch)?;
        }
        Ok(count)
    }

    fn group_tracks(
        &self,
        runs: &[Range<VirtualGroup>],
        after: Option<GroupTrack>,
        limit: usize,
    ) -> Result<Vec<GroupTrack>> {
        let after_key = match after {
            Some(after) => track_group_key(after.group, after.track)?.to_vec(),
            None => Vec::new(),
        };
        let mut tracks = Vec::new();
        for run in runs {
            let range = group_range(run)?;
            let left = limit - tracks.len();
            for (key, _) in read_range(self, TrackGroupIndexCol::CF_NAME, &range, &after_key, left)? {
                tracks.push(decode_group_track(&key)?);
            }
            if tracks.len() == limit {
                break;
            }
        }
        Ok(tracks)
    }

    fn write_fill(&self, entries: &[EntryKey], rows: &[(SpoolIndex, SpoolState)]) -> Result<()> {
        let mut batch = WriteBatch::new();
        let fresh = Entry::never_tried().pack();
        for key in entries {
            if self.has_entry(key.spool, key.track)? {
                continue;
            }
            batch.put_owned(SpoolMissingTrackCol::CF_NAME, key.pack()?.to_vec(), fresh.to_vec());
        }
        for (spool, row) in rows {
            let key = row_key(*spool)?;
            batch.put_owned(SpoolStateCol::CF_NAME, key.to_vec(), row.pack().to_vec());
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn put_owners(&self, epoch: EpochNumber, owners: &[(SpoolIndex, Address)]) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (spool, owner) in owners {
            let key = owner_key(epoch, *spool)?;
            batch.put_owned(SpoolOwnerHistoryCol::CF_NAME, key.to_vec(), owner.to_bytes().to_vec());
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn group_owners_at(
        &self,
        epoch: EpochNumber,
        group: VirtualGroup,
        width: usize,
    ) -> Result<Option<Vec<Address>>> {
        let Some(carrier) = carrier_at(self, epoch, group)? else {
            return Ok(None);
        };

        let mut keys = Vec::with_capacity(width);
        for position in 0..width.min(MAX_GROUP_SIZE) {
            keys.push(owner_key(epoch, carrier.spool_at(position))?);
        }
        let asked: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let values = self.inner().inner().get_many(SpoolOwnerHistoryCol::CF_NAME, &asked)?;

        let mut owners = Vec::with_capacity(values.len());
        for value in values {
            owners.push(decode_owner(value.as_deref())?);
        }
        Ok(Some(owners))
    }

    fn forget_owners_before(&self, epoch: EpochNumber) -> Result<()> {
        let end = owner_key(epoch, SpoolIndex(0))?;
        self.inner().inner().delete_range(SpoolOwnerHistoryCol::CF_NAME, &[], &end)?;
        Ok(())
    }

    fn history_owners(&self, limit: usize) -> Result<Vec<Address>> {
        let range = KeyRange { start: Vec::new(), end: vec![u8::MAX; 11] };
        let rows = read_range(self, SpoolOwnerHistoryCol::CF_NAME, &range, &[], limit)?;
        let mut owners = Vec::new();
        for (_, value) in rows {
            let owner = decode_owner(Some(&value))?;
            if owner != Address::default() && !owners.contains(&owner) {
                owners.push(owner);
            }
        }
        Ok(owners)
    }
}

/// The live group that carried `group` in `epoch`
///
/// An epoch's records cover every position of every group live in it. The
/// carrier is the first of `group` and its ancestors with a record: under a
/// fold of `n` groups, `live_group` gives the next ancestor below `n`.
fn carrier_at<S: Store>(
    store: &TapeStore<S>,
    epoch: EpochNumber,
    group: VirtualGroup,
) -> Result<Option<VirtualGroup>> {
    let raw = store.inner().inner();
    let mut carrier = group;
    loop {
        let first = owner_key(epoch, carrier.spool_at(0))?;
        if raw.contains(SpoolOwnerHistoryCol::CF_NAME, &first)? {
            return Ok(Some(carrier));
        }
        let Some(fold) = GroupFold::new(carrier.0) else {
            return Ok(None);
        };
        let Some(parent) = fold.virtual_group(fold.live_group(group)) else {
            return Ok(None);
        };
        carrier = parent;
    }
}

/// Up to `limit` rows of `range` that sort after `after`, read in key order
fn read_range<S: Store>(
    store: &TapeStore<S>,
    cf: &str,
    range: &KeyRange,
    after: &[u8],
    limit: usize,
) -> Result<Vec<KeyValue>> {
    let mut rows = Vec::new();
    let start = range.start.as_slice().max(after);
    if limit == 0 || start >= range.end.as_slice() {
        return Ok(rows);
    }

    for (key, value) in store.inner().inner().iter_from(cf, start, Direction::Asc)? {
        if key.as_slice() >= range.end.as_slice() {
            break;
        }
        if key.as_slice() == after {
            continue;
        }
        rows.push((key, value));
        if rows.len() == limit {
            break;
        }
    }
    Ok(rows)
}

fn live_runs(spool: SpoolIndex, fold: GroupFold) -> LiveRuns {
    LiveRuns {
        position: leaf_position(spool).as_u64() as u8,
        runs: fold.carried_ranges(GroupIndex::containing(spool)),
    }
}

/// A live spool's entry ranges in key order: every run without handover,
/// then every run with it
fn entry_ranges(live: LiveRuns) -> Result<Vec<KeyRange>> {
    let mut ranges = Vec::with_capacity(live.runs.len() * 2);
    for is_handover in [false, true] {
        for run in &live.runs {
            ranges.push(entry_range(live.position, is_handover, run)?);
        }
    }
    Ok(ranges)
}

fn entry_range(position: u8, is_handover: bool, run: &Range<VirtualGroup>) -> Result<KeyRange> {
    let head = [position, u8::from(is_handover)];
    Ok(KeyRange {
        start: [head.as_slice(), &group_bytes(run.start)?].concat(),
        end: [head.as_slice(), &group_bytes(run.end)?].concat(),
    })
}

fn row_range(position: u8, run: &Range<VirtualGroup>) -> Result<KeyRange> {
    Ok(KeyRange {
        start: [[position].as_slice(), &group_bytes(run.start)?].concat(),
        end: [[position].as_slice(), &group_bytes(run.end)?].concat(),
    })
}

fn group_range(run: &Range<VirtualGroup>) -> Result<KeyRange> {
    Ok(KeyRange {
        start: group_bytes(run.start)?.to_vec(),
        end: group_bytes(run.end)?.to_vec(),
    })
}

/// The first key past every key that starts with `prefix`
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(u8::MAX);
    end.push(u8::MAX);
    end
}

fn row_key(spool: SpoolIndex) -> Result<[u8; 3]> {
    let [high, low] = stored_group(spool)?;
    Ok([stored_position(spool), high, low])
}

fn row_spool(key: &[u8]) -> Result<SpoolIndex> {
    let [position, high, low] = <[u8; 3]>::try_from(key)
        .map_err(|_| TapeStoreError::Serialization("spool row key length".into()))?;
    let group = VirtualGroup(u64::from(u16::from_be_bytes([high, low])));
    Ok(group.spool_at(usize::from(position)))
}

fn owner_key(epoch: EpochNumber, spool: SpoolIndex) -> Result<[u8; 10]> {
    let spool = u16::try_from(spool.as_u64())
        .map_err(|_| TapeStoreError::SpoolKeyOverflow(spool))?;
    let mut key = [0u8; 10];
    key[..8].copy_from_slice(&epoch.0.to_be_bytes());
    key[8..].copy_from_slice(&spool.to_be_bytes());
    Ok(key)
}

/// The group track key, also written by the track catalog
pub fn track_group_key(group: VirtualGroup, track: Address) -> Result<[u8; 34]> {
    let mut key = [0u8; 34];
    key[..2].copy_from_slice(&group_bytes(group)?);
    key[2..].copy_from_slice(&track.to_bytes());
    Ok(key)
}

fn decode_group_track(key: &[u8]) -> Result<GroupTrack> {
    if key.len() != 34 {
        return Err(TapeStoreError::Serialization("group track key length".into()));
    }
    let mut track = [0u8; 32];
    track.copy_from_slice(&key[2..]);
    Ok(GroupTrack {
        group: VirtualGroup(u64::from(u16::from_be_bytes([key[0], key[1]]))),
        track: Address::new(track),
    })
}

fn decode_row(value: &[u8]) -> Result<SpoolState> {
    let bytes = PackedSpoolState::try_from(value)
        .map_err(|_| TapeStoreError::Serialization("spool row length".into()))?;
    Ok(SpoolState::unpack(bytes))
}

fn decode_entry(value: &[u8]) -> Result<Entry> {
    let bytes = PackedEntry::try_from(value)
        .map_err(|_| TapeStoreError::Serialization("entry length".into()))?;
    Ok(Entry::unpack(bytes))
}

fn decode_owner(value: Option<&[u8]>) -> Result<Address> {
    let Some(value) = value else {
        return Ok(Address::default());
    };
    let bytes = <[u8; 32]>::try_from(value)
        .map_err(|_| TapeStoreError::Serialization("owner length".into()))?;
    Ok(Address::new(bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tape_store_api::{StoreEngine, StoreIter, Value};
    use tape_store_memory::MemoryStore;

    use super::*;
    use crate::types::missing::WallTime;

    const POSITION: usize = 3;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn fold(live: u64) -> GroupFold {
        GroupFold::new(live).expect("fold")
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn key(group: u64, position: usize, is_handover: bool, byte: u8) -> EntryKey {
        EntryKey {
            spool: VirtualGroup(group).spool_at(position),
            is_handover,
            track: address(byte),
        }
    }

    fn put(store: &TapeStore<MemoryStore>, key: EntryKey) {
        store.put_entry(key, Entry::never_tried()).expect("put entry");
    }

    fn tracks(page: &[(EntryKey, Entry)]) -> Vec<u8> {
        page.iter().map(|(key, _)| key.track.to_bytes()[0]).collect()
    }

    // a live spool's page holds its own position's entries from every group
    // it carries, plain ones before handover ones, and none of another's
    #[test]
    fn page_by_runs() {
        let store = test_store();
        let live = GroupIndex(0).spool_at(POSITION);
        put(&store, key(0, POSITION, true, 1));
        put(&store, key(2, POSITION, false, 2));
        put(&store, key(6, POSITION, false, 3));
        put(&store, key(1, POSITION, false, 4));
        put(&store, key(2, POSITION + 1, false, 5));

        let page = store.missing_page(live, fold(2), None, 10).expect("page");

        assert_eq!(tracks(&page), vec![2, 1]);
        let all = store.missing_page(live, fold(1), None, 10).expect("page");
        assert_eq!(tracks(&all), vec![4, 2, 3, 1]);
    }

    // a page resumes after the key it was handed and stops at its limit
    #[test]
    fn page_resume() {
        let store = test_store();
        let live = GroupIndex(0).spool_at(POSITION);
        for byte in 1..=5 {
            put(&store, key(0, POSITION, false, byte));
        }

        let first = store.missing_page(live, fold(1), None, 2).expect("page");
        let after = first.last().map(|(key, _)| *key);
        let rest = store.missing_page(live, fold(1), after, 10).expect("page");

        assert_eq!(tracks(&first), vec![1, 2]);
        assert_eq!(tracks(&rest), vec![3, 4, 5]);
    }

    // the handover check sees only the live spool's handover entries
    #[test]
    fn handover_left() {
        let store = test_store();
        let live = GroupIndex(1).spool_at(POSITION);
        put(&store, key(1, POSITION, false, 1));
        put(&store, key(0, POSITION, true, 2));

        assert!(!store.has_handover_entries(live, fold(2)).expect("check"));

        put(&store, key(3, POSITION, true, 3));
        assert!(store.has_handover_entries(live, fold(2)).expect("check"));
    }

    // filling adds what isn't there, keeps an entry the certify hook wrote,
    // and moves its row in the same write
    #[test]
    fn fill_keeps_entries() {
        let store = test_store();
        let hooked = key(0, POSITION, false, 1);
        let uploaded = Entry::uploaded(WallTime(7));
        store.put_entry(hooked, uploaded).expect("put entry");
        let spool = VirtualGroup(0).spool_at(POSITION);
        let row = SpoolState { is_filling: false, ..SpoolState::gained() };

        let entries = [EntryKey { is_handover: true, ..hooked }, key(0, POSITION, true, 2)];
        store.write_fill(&entries, &[(spool, row)]).expect("fill");

        let live = GroupIndex(0).spool_at(POSITION);
        let page = store.missing_page(live, fold(1), None, 10).expect("page");
        assert_eq!(page, vec![(hooked, uploaded), (entries[1], Entry::never_tried())]);
        assert_eq!(store.get_spool_state(spool).expect("row"), Some(row));
    }

    // a stored spool's entries go a page at a time, and its neighbours stay
    #[test]
    fn delete_by_page() {
        let store = test_store();
        for byte in 1..=3 {
            put(&store, key(5, POSITION, false, byte));
            put(&store, key(5, POSITION, true, byte + 10));
        }
        put(&store, key(6, POSITION, false, 20));
        let spool = VirtualGroup(5).spool_at(POSITION);

        assert_eq!(store.delete_spool_entries(spool, 4).expect("delete"), 4);
        assert_eq!(store.delete_spool_entries(spool, 4).expect("delete"), 2);
        assert_eq!(store.delete_spool_entries(spool, 4).expect("delete"), 0);
        assert!(store.has_entry(VirtualGroup(6).spool_at(POSITION), address(20)).expect("entry"));
    }

    // a live spool's rows come back by its runs, and every row pages in key order
    #[test]
    fn rows_by_runs() {
        let store = test_store();
        let rows: Vec<(SpoolIndex, SpoolState)> = [0, 1, 2, 3, 6]
            .iter()
            .map(|group| (VirtualGroup(*group).spool_at(POSITION), SpoolState::gained()))
            .collect();
        store.put_spool_states(&rows).expect("rows");

        let live = GroupIndex(1).spool_at(POSITION);
        let carried = store.live_spool_states(live, fold(2)).expect("rows");
        let first = store.spool_states(None, 2).expect("page");
        let rest = store.spool_states(Some(first[1].0), 10).expect("page");

        let spools: Vec<SpoolIndex> = carried.iter().map(|(spool, _)| *spool).collect();
        assert_eq!(spools, vec![rows[1].0, rows[3].0, rows[4].0]);
        assert_eq!(first.len() + rest.len(), rows.len());
    }

    // a track list reads across runs of groups from the track after the start
    #[test]
    fn group_track_pages() {
        let store = test_store();
        let raw = store.inner().inner();
        for (group, byte) in [(0, 1), (0, 2), (2, 3), (4, 4), (5, 5)] {
            let key = track_group_key(VirtualGroup(group), address(byte)).expect("key");
            raw.put(TrackGroupIndexCol::CF_NAME, &key, &[]).expect("put");
        }
        let runs = [VirtualGroup(0)..VirtualGroup(1), VirtualGroup(2)..VirtualGroup(5)];
        let after = GroupTrack { group: VirtualGroup(0), track: address(1) };

        let page = store.group_tracks(&runs, Some(after), 2).expect("page");
        let all = store.group_tracks(&runs, None, 10).expect("page");

        let bytes: Vec<u8> = page.iter().map(|row| row.track.to_bytes()[0]).collect();
        assert_eq!(bytes, vec![2, 3]);
        assert_eq!(all.len(), 4);
    }

    // a group's owners at an epoch come from the live group that carried it
    // then, and an epoch the history lacks has none
    #[test]
    fn owners_by_carrier() {
        let store = test_store();
        let width = 4;
        let mut owners = Vec::new();
        for group in 0..2u64 {
            for position in 0..width {
                let owner = address((group * 10 + position as u64 + 1) as u8);
                owners.push((GroupIndex(group).spool_at(position), owner));
            }
        }
        store.put_owners(EpochNumber(8), &owners[..width]).expect("one group");
        store.put_owners(EpochNumber(9), &owners).expect("two groups");

        let split = store.group_owners_at(EpochNumber(9), VirtualGroup(7), width).expect("read");
        let whole = store.group_owners_at(EpochNumber(8), VirtualGroup(7), width).expect("read");
        let unknown = store.group_owners_at(EpochNumber(7), VirtualGroup(7), width).expect("read");

        assert_eq!(split.expect("recorded")[0], address(11));
        assert_eq!(whole.expect("recorded")[0], address(1));
        assert!(unknown.is_none());

        store.forget_owners_before(EpochNumber(9)).expect("forget");
        let gone = store.group_owners_at(EpochNumber(8), VirtualGroup(7), width).expect("read");
        assert!(gone.is_none());
    }

    /// A memory store that counts the rows its walks hand out
    struct CountingStore {
        inner: MemoryStore,
        pulled: AtomicUsize,
    }

    impl Store for CountingStore {
        fn store_engine(&self) -> StoreEngine {
            self.inner.store_engine()
        }
        fn get(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<Option<Value>> {
            self.inner.get(cf, key)
        }
        fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> tape_store_api::Result<()> {
            self.inner.put(cf, key, value)
        }
        fn delete(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<()> {
            self.inner.delete(cf, key)
        }
        fn contains(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<bool> {
            self.inner.contains(cf, key)
        }
        fn write_batch(&self, batch: WriteBatch) -> tape_store_api::Result<()> {
            self.inner.write_batch(batch)
        }
        fn iter(&self, cf: &str) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter(cf)
        }
        fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_prefix(cf, prefix)
        }
        fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> tape_store_api::Result<Option<u64>> {
            self.inner.bytes_prefix(cf, prefix)
        }
        fn iter_from(
            &self,
            cf: &str,
            start: &[u8],
            direction: Direction,
        ) -> tape_store_api::Result<StoreIter<'_>> {
            let rows = self.inner.iter_from(cf, start, direction)?;
            let count = |_: &KeyValue| {
                self.pulled.fetch_add(1, Ordering::Relaxed);
            };
            Ok(Box::new(rows.inspect(count)))
        }
        fn iter_range(
            &self,
            cf: &str,
            start: &[u8],
            end: &[u8],
        ) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_range(cf, start, end)
        }
        fn live_data_size_bytes(&self) -> tape_store_api::Result<Option<u64>> {
            self.inner.live_data_size_bytes()
        }
        fn key_count_estimate(&self, cf: &str) -> tape_store_api::Result<Option<u64>> {
            self.inner.key_count_estimate(cf)
        }
    }

    // with many entries listed, a page takes no more rows than it returns
    #[test]
    fn bounded_page() {
        const LISTED: u64 = 10_000;
        let counting = CountingStore { inner: MemoryStore::new(), pulled: AtomicUsize::new(0) };
        let store = TapeStore::new(counting);
        let spool = VirtualGroup(0).spool_at(POSITION);
        for number in 0..LISTED {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&number.to_be_bytes());
            let key = EntryKey { spool, is_handover: false, track: Address::new(bytes) };
            store.put_entry(key, Entry::never_tried()).expect("put entry");
        }
        let live = GroupIndex(0).spool_at(POSITION);

        let page = store.missing_page(live, fold(1), None, 128).expect("page");

        assert_eq!(page.len(), 128);
        assert_eq!(store.inner().inner().pulled.load(Ordering::Relaxed), 128);
    }
}
