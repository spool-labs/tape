//! The spool records: spool_states, the missing_list and the owner_history
//!
//! Every record is kept by stored spool. A live spool is a set of stored
//! spools, one per virtual group it carries, and a read that answers for a
//! live spool walks its runs of virtual groups, never one group at a time.

use std::ops::Range;

use tape_core::encoding::ErasureCode;
use tape_core::erasure::{leaf_position, MAX_GROUP_SIZE};
use tape_core::spooler::{GroupFold, GroupIndex, VirtualGroup, VIRTUAL_GROUP_COUNT};
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::address::Address;
use tape_recover::state::SpoolState;
use tape_recover::track::{MissingTrack, TrackOrigin};
use tape_store_api::{Column, Store, WriteBatch};

use crate::columns::{SpoolMissingTrackCol, SpoolOwnerHistoryCol, SpoolStateCol};
use crate::error::{Result, TapeStoreError};
use crate::ops::range::{read_range, KeyRange};
use crate::types::keys::{MissingTrackKey, OwnerKey, SpoolStateKey};
use crate::TapeStore;

/// How many stored spools there are, and so the most spool states a store holds
pub const STORED_SPOOL_COUNT: usize = VIRTUAL_GROUP_COUNT as usize * MAX_GROUP_SIZE;

/// Each origin of a missing track, in key order
const ORIGINS: [TrackOrigin; 2] = [TrackOrigin::Upload, TrackOrigin::Handover];

/// Operations on the spool records
pub trait SpoolOps {
    /// A stored spool's state
    fn get_spool_state(&self, spool: SpoolIndex) -> Result<Option<SpoolState>>;

    /// Up to `limit` of every state the store holds, from the one after `after`
    fn get_spool_states(
        &self,
        after: Option<SpoolIndex>,
        limit: usize,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    /// The states of the stored spools a live spool carries, in key order
    fn get_live_spool_states(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>>;

    /// Write stored spools' states in one batch
    fn put_spool_states(&self, states: &[(SpoolIndex, SpoolState)]) -> Result<()>;

    /// Delete a stored spool's state
    fn delete_spool_state(&self, spool: SpoolIndex) -> Result<()>;

    /// Up to `limit` missing tracks of a live spool, from the one after `after`
    fn get_missing_tracks(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
        after: Option<&MissingTrack>,
        limit: usize,
    ) -> Result<Vec<MissingTrack>>;

    /// A stored spool's missing track for a track
    fn get_missing_track(&self, spool: SpoolIndex, track: Address) -> Result<Option<MissingTrack>>;

    /// Whether a stored spool's missing_list holds a track
    fn has_missing_track(&self, spool: SpoolIndex, track: Address) -> Result<bool>;

    /// Write one missing track
    fn put_missing_track(&self, missing: &MissingTrack) -> Result<()>;

    /// Delete one missing track
    fn delete_missing_track(&self, missing: &MissingTrack) -> Result<()>;

    /// Whether any handover missing track is left in a live spool's missing_list
    fn has_handover_tracks(&self, spool: SpoolIndex, fold: GroupFold) -> Result<bool>;

    /// Delete up to `limit` missing tracks of a stored spool, returning how many went
    fn delete_missing_tracks(&self, spool: SpoolIndex, limit: usize) -> Result<usize>;

    /// Add the scan page's missing tracks that aren't listed yet, and write the
    /// states it moved on, in one write
    fn put_scan_page(
        &self,
        missing: &[MissingTrack],
        states: &[(SpoolIndex, SpoolState)],
    ) -> Result<()>;

    /// Record an epoch's owners, keyed by each live group's own stored spools
    fn put_owners(&self, epoch: EpochNumber, owners: &[(SpoolIndex, Address)]) -> Result<()>;

    /// The owners, by position, of a live group's own stored spools in an
    /// epoch, or nothing when the epoch has no record for the group
    fn get_owners(
        &self,
        epoch: EpochNumber,
        group: VirtualGroup,
        profile: ErasureCode,
    ) -> Result<Option<Vec<Address>>>;

    /// Delete every epoch before `epoch`
    fn delete_owners_before(&self, epoch: EpochNumber) -> Result<()>;

    /// Every node the owner_history records, reading up to `limit` records
    fn get_recorded_owners(&self, limit: usize) -> Result<Vec<Address>>;
}

impl<S: Store> SpoolOps for TapeStore<S> {
    fn get_spool_state(&self, spool: SpoolIndex) -> Result<Option<SpoolState>> {
        let key = SpoolStateKey(spool).pack()?;
        let raw = self.inner().inner();
        let Some(value) = raw.get(SpoolStateCol::CF_NAME, &key)? else {
            return Ok(None);
        };
        Ok(Some(decode_state(&value)?))
    }

    fn get_spool_states(
        &self,
        after: Option<SpoolIndex>,
        limit: usize,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let after_key = match after {
            Some(spool) => SpoolStateKey(spool).pack()?.to_vec(),
            None => Vec::new(),
        };
        let range = KeyRange { start: Vec::new(), end: vec![u8::MAX; SpoolStateKey::SIZE + 1] };
        let mut states = Vec::new();
        for (key, value) in read_range(self, SpoolStateCol::CF_NAME, &range, &after_key, limit)? {
            states.push((SpoolStateKey::unpack(&key)?.0, decode_state(&value)?));
        }
        Ok(states)
    }

    fn get_live_spool_states(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
    ) -> Result<Vec<(SpoolIndex, SpoolState)>> {
        let position = leaf_position(spool).as_usize();
        let mut states = Vec::new();
        for run in get_live_runs(spool, fold) {
            let range = get_state_range(position, &run)?;
            let width = (run.end.0 - run.start.0) as usize;
            for (key, value) in read_range(self, SpoolStateCol::CF_NAME, &range, &[], width)? {
                states.push((SpoolStateKey::unpack(&key)?.0, decode_state(&value)?));
            }
        }
        Ok(states)
    }

    fn put_spool_states(&self, states: &[(SpoolIndex, SpoolState)]) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (spool, state) in states {
            let key = SpoolStateKey(*spool).pack()?;
            batch.put_owned(SpoolStateCol::CF_NAME, key.to_vec(), state.pack().to_vec());
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn delete_spool_state(&self, spool: SpoolIndex) -> Result<()> {
        let key = SpoolStateKey(spool).pack()?;
        self.inner().inner().delete(SpoolStateCol::CF_NAME, &key)?;
        Ok(())
    }

    fn get_missing_tracks(
        &self,
        spool: SpoolIndex,
        fold: GroupFold,
        after: Option<&MissingTrack>,
        limit: usize,
    ) -> Result<Vec<MissingTrack>> {
        let after_key = match after {
            Some(missing) => get_missing_key(missing).pack()?.to_vec(),
            None => Vec::new(),
        };
        let mut page = Vec::new();
        for range in get_missing_ranges(spool, fold)? {
            let left = limit - page.len();
            let cf = SpoolMissingTrackCol::CF_NAME;
            for (key, value) in read_range(self, cf, &range, &after_key, left)? {
                page.push(decode_missing(&key, &value)?);
            }
            if page.len() == limit {
                break;
            }
        }
        Ok(page)
    }

    fn get_missing_track(&self, spool: SpoolIndex, track: Address) -> Result<Option<MissingTrack>> {
        let raw = self.inner().inner();
        for origin in ORIGINS {
            let key = MissingTrackKey::new(spool, origin, track).pack()?;
            if let Some(value) = raw.get(SpoolMissingTrackCol::CF_NAME, &key)? {
                return Ok(Some(decode_missing(&key, &value)?));
            }
        }
        Ok(None)
    }

    fn has_missing_track(&self, spool: SpoolIndex, track: Address) -> Result<bool> {
        let raw = self.inner().inner();
        for origin in ORIGINS {
            let key = MissingTrackKey::new(spool, origin, track).pack()?;
            if raw.contains(SpoolMissingTrackCol::CF_NAME, &key)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn put_missing_track(&self, missing: &MissingTrack) -> Result<()> {
        let key = get_missing_key(missing).pack()?;
        let raw = self.inner().inner();
        raw.put(SpoolMissingTrackCol::CF_NAME, &key, &missing.pack())?;
        Ok(())
    }

    fn delete_missing_track(&self, missing: &MissingTrack) -> Result<()> {
        let key = get_missing_key(missing).pack()?;
        self.inner().inner().delete(SpoolMissingTrackCol::CF_NAME, &key)?;
        Ok(())
    }

    fn has_handover_tracks(&self, spool: SpoolIndex, fold: GroupFold) -> Result<bool> {
        let position = leaf_position(spool).as_usize();
        for run in get_live_runs(spool, fold) {
            let range = get_missing_range(position, TrackOrigin::Handover, &run)?;
            let cf = SpoolMissingTrackCol::CF_NAME;
            if !read_range(self, cf, &range, &[], 1)?.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn delete_missing_tracks(&self, spool: SpoolIndex, limit: usize) -> Result<usize> {
        let position = leaf_position(spool).as_usize();
        let group = VirtualGroup::containing(spool);
        let run = group..VirtualGroup(group.0 + 1);
        let mut batch = WriteBatch::new();
        let mut count = 0;
        for origin in ORIGINS {
            let range = get_missing_range(position, origin, &run)?;
            let cf = SpoolMissingTrackCol::CF_NAME;
            for (key, _) in read_range(self, cf, &range, &[], limit - count)? {
                batch.delete_owned(cf, key);
                count += 1;
            }
        }
        if count > 0 {
            self.inner().inner().write_batch(batch)?;
        }
        Ok(count)
    }

    fn put_scan_page(
        &self,
        missing: &[MissingTrack],
        states: &[(SpoolIndex, SpoolState)],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        for listed in missing {
            if self.has_missing_track(listed.spool, listed.track)? {
                continue;
            }
            let key = get_missing_key(listed).pack()?;
            batch.put_owned(SpoolMissingTrackCol::CF_NAME, key.to_vec(), listed.pack().to_vec());
        }
        for (spool, state) in states {
            let key = SpoolStateKey(*spool).pack()?;
            batch.put_owned(SpoolStateCol::CF_NAME, key.to_vec(), state.pack().to_vec());
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn put_owners(&self, epoch: EpochNumber, owners: &[(SpoolIndex, Address)]) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (spool, owner) in owners {
            let key = OwnerKey::new(epoch, *spool).pack()?;
            let value = owner.to_bytes().to_vec();
            batch.put_owned(SpoolOwnerHistoryCol::CF_NAME, key.to_vec(), value);
        }
        self.inner().inner().write_batch(batch)?;
        Ok(())
    }

    fn get_owners(
        &self,
        epoch: EpochNumber,
        group: VirtualGroup,
        profile: ErasureCode,
    ) -> Result<Option<Vec<Address>>> {
        let raw = self.inner().inner();
        let first = OwnerKey::new(epoch, group.spool_at(0)).pack()?;
        if !raw.contains(SpoolOwnerHistoryCol::CF_NAME, &first)? {
            return Ok(None);
        }

        let width = usize::from(profile.n()).min(MAX_GROUP_SIZE);
        let mut keys = Vec::with_capacity(width);
        for position in 0..width {
            keys.push(OwnerKey::new(epoch, group.spool_at(position)).pack()?);
        }
        let asked: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let values = raw.get_many(SpoolOwnerHistoryCol::CF_NAME, &asked)?;

        let mut owners = Vec::with_capacity(values.len());
        for value in values {
            owners.push(decode_owner(value.as_deref())?);
        }
        Ok(Some(owners))
    }

    fn delete_owners_before(&self, epoch: EpochNumber) -> Result<()> {
        let end = OwnerKey::new(epoch, SpoolIndex(0)).pack()?;
        self.inner().inner().delete_range(SpoolOwnerHistoryCol::CF_NAME, &[], &end)?;
        Ok(())
    }

    fn get_recorded_owners(&self, limit: usize) -> Result<Vec<Address>> {
        let range = KeyRange { start: Vec::new(), end: vec![u8::MAX; OwnerKey::SIZE + 1] };
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

/// The runs of virtual groups a live spool carries
fn get_live_runs(spool: SpoolIndex, fold: GroupFold) -> Vec<Range<VirtualGroup>> {
    fold.carried_ranges(GroupIndex::containing(spool))
}

/// A live spool's missing track ranges in key order: every run of the
/// uploads, then every run of the handovers
fn get_missing_ranges(spool: SpoolIndex, fold: GroupFold) -> Result<Vec<KeyRange>> {
    let position = leaf_position(spool).as_usize();
    let runs = get_live_runs(spool, fold);
    let mut ranges = Vec::with_capacity(runs.len() * ORIGINS.len());
    for origin in ORIGINS {
        for run in &runs {
            ranges.push(get_missing_range(position, origin, run)?);
        }
    }
    Ok(ranges)
}

/// The missing track keys of one position and origin over a run of groups
fn get_missing_range(
    position: usize,
    origin: TrackOrigin,
    run: &Range<VirtualGroup>,
) -> Result<KeyRange> {
    let track = Address::default();
    let start = MissingTrackKey::new(run.start.spool_at(position), origin, track);
    let end = MissingTrackKey::new(run.end.spool_at(position), origin, track);
    Ok(KeyRange { start: start.pack()?.to_vec(), end: end.pack()?.to_vec() })
}

/// The state keys of one position over a run of groups
fn get_state_range(position: usize, run: &Range<VirtualGroup>) -> Result<KeyRange> {
    let start = SpoolStateKey(run.start.spool_at(position)).pack()?;
    let end = SpoolStateKey(run.end.spool_at(position)).pack()?;
    Ok(KeyRange { start: start.to_vec(), end: end.to_vec() })
}

fn get_missing_key(missing: &MissingTrack) -> MissingTrackKey {
    MissingTrackKey::new(missing.spool, missing.origin, missing.track)
}

fn decode_state(value: &[u8]) -> Result<SpoolState> {
    let bytes = <[u8; SpoolState::SIZE]>::try_from(value)
        .map_err(|_| TapeStoreError::Serialization("spool state length".into()))?;
    Ok(SpoolState::unpack(bytes))
}

fn decode_missing(key: &[u8], value: &[u8]) -> Result<MissingTrack> {
    let key = MissingTrackKey::unpack(key)?;
    let bytes = <[u8; MissingTrack::SIZE]>::try_from(value)
        .map_err(|_| TapeStoreError::Serialization("missing track length".into()))?;
    Ok(MissingTrack::unpack(key.spool, key.origin, key.track, bytes))
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

    use tape_core::encoding::{RSParams, TAPENET_PROFILE};
    use tape_recover::time::WallTime;
    use tape_store_api::{Direction, KeyValue, StoreEngine, StoreIter, Value};
    use tape_store_memory::MemoryStore;

    use super::*;

    const POSITION: usize = 3;

    /// Four spools a group, so four owners a record
    const FOUR_WIDE: ErasureCode = ErasureCode::ReedSolomon(RSParams::new(3, 1));

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn fold(live: u64) -> GroupFold {
        GroupFold::new(live).expect("fold")
    }

    fn address(byte: u8) -> Address {
        Address::new([byte; 32])
    }

    fn missing(group: u64, position: usize, is_handover: bool, byte: u8) -> MissingTrack {
        let spool = VirtualGroup(group).spool_at(position);
        if is_handover {
            return MissingTrack::new_handover(spool, address(byte), WallTime(1));
        }
        MissingTrack::new_upload(spool, address(byte), WallTime(1))
    }

    fn put(store: &TapeStore<MemoryStore>, listed: MissingTrack) {
        store.put_missing_track(&listed).expect("put missing track");
    }

    fn tracks(page: &[MissingTrack]) -> Vec<u8> {
        page.iter().map(|listed| listed.track.to_bytes()[0]).collect()
    }

    // a live spool's page holds its own position's missing tracks from every
    // group it carries, uploads before handover ones, and none of another's
    #[test]
    fn page_by_runs() {
        let store = test_store();
        let live = GroupIndex(0).spool_at(POSITION);
        put(&store, missing(0, POSITION, true, 1));
        put(&store, missing(2, POSITION, false, 2));
        put(&store, missing(6, POSITION, false, 3));
        put(&store, missing(1, POSITION, false, 4));
        put(&store, missing(2, POSITION + 1, false, 5));

        let page = store.get_missing_tracks(live, fold(2), None, 10).expect("page");

        assert_eq!(tracks(&page), vec![2, 1]);
        let all = store.get_missing_tracks(live, fold(1), None, 10).expect("page");
        assert_eq!(tracks(&all), vec![4, 2, 3, 1]);
    }

    // a page resumes after the missing track it was handed and stops at its limit
    #[test]
    fn page_resume() {
        let store = test_store();
        let live = GroupIndex(0).spool_at(POSITION);
        for byte in 1..=5 {
            put(&store, missing(0, POSITION, false, byte));
        }

        let first = store.get_missing_tracks(live, fold(1), None, 2).expect("page");
        let rest = store.get_missing_tracks(live, fold(1), first.last(), 10).expect("page");

        assert_eq!(tracks(&first), vec![1, 2]);
        assert_eq!(tracks(&rest), vec![3, 4, 5]);
    }

    // the handover check sees only the live spool's handover missing tracks
    #[test]
    fn handover_left() {
        let store = test_store();
        let live = GroupIndex(1).spool_at(POSITION);
        put(&store, missing(1, POSITION, false, 1));
        put(&store, missing(0, POSITION, true, 2));

        assert!(!store.has_handover_tracks(live, fold(2)).expect("check"));

        put(&store, missing(3, POSITION, true, 3));
        assert!(store.has_handover_tracks(live, fold(2)).expect("check"));
    }

    // a missing track reads back under either flag, and goes once deleted
    #[test]
    fn point_reads() {
        let store = test_store();
        let listed = missing(4, POSITION, true, 7);
        put(&store, listed);

        let read = store.get_missing_track(listed.spool, listed.track).expect("read");
        assert_eq!(read, Some(listed));
        assert!(store.has_missing_track(listed.spool, listed.track).expect("has"));

        store.delete_missing_track(&listed).expect("delete");
        assert!(!store.has_missing_track(listed.spool, listed.track).expect("has"));
    }

    // a scan page adds what isn't listed, keeps a missing track the certify
    // hook wrote, and moves its state in the same write
    #[test]
    fn scan_keeps_listed() {
        let store = test_store();
        let uploaded = missing(0, POSITION, false, 1);
        put(&store, uploaded);
        let spool = VirtualGroup(0).spool_at(POSITION);
        let state = SpoolState { is_scanning: false, ..SpoolState::new_scanning() };

        let found = [missing(0, POSITION, true, 1), missing(0, POSITION, true, 2)];
        store.put_scan_page(&found, &[(spool, state)]).expect("scan");

        let live = GroupIndex(0).spool_at(POSITION);
        let page = store.get_missing_tracks(live, fold(1), None, 10).expect("page");
        assert_eq!(page, vec![uploaded, found[1]]);
        assert_eq!(store.get_spool_state(spool).expect("state"), Some(state));
    }

    // a stored spool's missing tracks go a page at a time, and its neighbours stay
    #[test]
    fn delete_by_page() {
        let store = test_store();
        for byte in 1..=3 {
            put(&store, missing(5, POSITION, false, byte));
            put(&store, missing(5, POSITION, true, byte + 10));
        }
        put(&store, missing(6, POSITION, false, 20));
        let spool = VirtualGroup(5).spool_at(POSITION);
        let neighbour = VirtualGroup(6).spool_at(POSITION);

        assert_eq!(store.delete_missing_tracks(spool, 4).expect("delete"), 4);
        assert_eq!(store.delete_missing_tracks(spool, 4).expect("delete"), 2);
        assert_eq!(store.delete_missing_tracks(spool, 4).expect("delete"), 0);
        assert!(store.has_missing_track(neighbour, address(20)).expect("has"));
    }

    // a live spool's states come back by its runs, and every state pages in key order
    #[test]
    fn states_by_runs() {
        let store = test_store();
        let mut states = Vec::new();
        for group in [0, 1, 2, 3, 6] {
            let spool = VirtualGroup(group).spool_at(POSITION);
            states.push((spool, SpoolState::new_scanning()));
        }
        store.put_spool_states(&states).expect("states");

        let live = GroupIndex(1).spool_at(POSITION);
        let carried = store.get_live_spool_states(live, fold(2)).expect("states");
        let first = store.get_spool_states(None, 2).expect("page");
        let rest = store.get_spool_states(Some(first[1].0), 10).expect("page");

        let spools: Vec<SpoolIndex> = carried.iter().map(|(spool, _)| *spool).collect();
        assert_eq!(spools, vec![states[1].0, states[3].0, states[4].0]);
        assert_eq!(first.len() + rest.len(), states.len());
    }

    // a live group's owners at an epoch read back by position, an epoch or a
    // group the history lacks has none, and a forgotten epoch is gone
    #[test]
    fn owners_by_group() {
        let store = test_store();
        let width = usize::from(FOUR_WIDE.n());
        let mut owners = Vec::new();
        for group in 0..2u64 {
            for position in 0..width {
                let owner = address((group * 10 + position as u64 + 1) as u8);
                owners.push((GroupIndex(group).spool_at(position), owner));
            }
        }
        store.put_owners(EpochNumber(8), &owners[..width]).expect("one group");
        store.put_owners(EpochNumber(9), &owners).expect("two groups");

        let second = store.get_owners(EpochNumber(9), VirtualGroup(1), FOUR_WIDE);
        let first = store.get_owners(EpochNumber(8), VirtualGroup(0), FOUR_WIDE);
        let split = store.get_owners(EpochNumber(8), VirtualGroup(1), FOUR_WIDE);
        let unknown = store.get_owners(EpochNumber(7), VirtualGroup(0), FOUR_WIDE);

        assert_eq!(second.expect("read").expect("recorded")[0], address(11));
        assert_eq!(first.expect("read").expect("recorded").len(), width);
        assert!(split.expect("read").is_none());
        assert!(unknown.expect("read").is_none());

        store.delete_owners_before(EpochNumber(9)).expect("forget");
        let gone = store.get_owners(EpochNumber(8), VirtualGroup(0), TAPENET_PROFILE);
        assert!(gone.expect("read").is_none());
        assert_eq!(store.get_recorded_owners(100).expect("owners").len(), 8);
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

    // with many missing tracks listed, a page takes no more rows than it returns
    #[test]
    fn bounded_page() {
        const LISTED: u64 = 10_000;
        let counting = CountingStore { inner: MemoryStore::new(), pulled: AtomicUsize::new(0) };
        let store = TapeStore::new(counting);
        let spool = VirtualGroup(0).spool_at(POSITION);
        for number in 0..LISTED {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&number.to_be_bytes());
            let track = Address::new(bytes);
            let listed = MissingTrack::new_upload(spool, track, WallTime(1));
            store.put_missing_track(&listed).expect("put missing track");
        }
        let live = GroupIndex(0).spool_at(POSITION);

        let page = store.get_missing_tracks(live, fold(1), None, 128).expect("page");

        assert_eq!(page.len(), 128);
        assert_eq!(store.inner().inner().pulled.load(Ordering::Relaxed), 128);
    }
}
