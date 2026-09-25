use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use tape_store_api::{Column, Direction, Store};
use tape_core::erasure::GROUP_SIZE;
use tape_core::spooler::GroupIndex;
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_node::config::gateway::GatewayCacheConfig;
use tape_store::TapeStore;
use tape_store::columns::SliceCol;
use tape_store::ops::SliceOps;
use tape_store::types::{slice, SliceKey};
use tokio::task::JoinSet;
use tracing::{debug, warn};

use super::error::GatewayCacheError;
use super::inflight::InflightFetches;
use super::state::{CacheRead, CacheSource, CacheState, CacheStats, SliceCacheKey};
use crate::blacklist::denials::GatewayBlacklists;

pub struct GatewaySliceCache<Db: Store> {
    core: Arc<CacheCore<Db>>,
    writer: Arc<dyn FillWriter>,
    inflight: InflightFetches,
    writes: Mutex<JoinSet<()>>,
}

/// Writes a fetched slice to the cache store on a blocking thread
trait FillWriter: Send + Sync {
    fn write(&self, key: SliceCacheKey, tape: Address, data: &[u8]);
}

struct CacheCore<Db: Store> {
    store: Arc<TapeStore<Db>>,
    config: GatewayCacheConfig,
    state: Mutex<CacheState>,
    fills: Mutex<HashMap<SliceCacheKey, Bytes>>,
    deleted_since_reclaim: AtomicUsize,
    blacklists: Arc<GatewayBlacklists>,
}

impl<Db: Store + 'static> GatewaySliceCache<Db> {
    pub fn new(
        store: Arc<TapeStore<Db>>,
        config: GatewayCacheConfig,
        blacklists: Arc<GatewayBlacklists>,
    ) -> Result<Self, GatewayCacheError> {
        let state = CacheCore::load_state(store.as_ref())?;
        let core = Arc::new(CacheCore {
            store,
            config,
            state: Mutex::new(state),
            fills: Mutex::new(HashMap::new()),
            deleted_since_reclaim: AtomicUsize::new(0),
            blacklists,
        });
        let evicted = core.evict_to_budget()?;
        if evicted > 0 {
            crate::metrics::inc_cache_evicted(evicted as u64);
            debug!(evicted, "gateway cache evicted startup entries");
        }
        Ok(Self {
            writer: core.clone(),
            core,
            inflight: InflightFetches::default(),
            writes: Mutex::new(JoinSet::new()),
        })
    }
}

impl<Db: Store> GatewaySliceCache<Db> {
    pub fn stats(&self) -> Result<CacheStats, GatewayCacheError> {
        let state = self.core.lock_state()?;
        let inflight = self.inflight.len()?;
        Ok(CacheStats {
            entries: state.entries.len(),
            bytes: state.total_bytes,
            inflight,
        })
    }

    pub async fn get_or_insert_with<F, Fut, E>(
        &self,
        spool_id: SpoolIndex,
        track_address: Address,
        tape: Address,
        fetch: F,
    ) -> Result<CacheRead, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Bytes, E>>,
        E: From<GatewayCacheError>,
    {
        let key = SliceCacheKey::new(spool_id, track_address);
        let mut fetch = Some(fetch);
        let mut waited = false;

        loop {
            // Checked again after every wait, so a waiter never serves content denied meanwhile.
            if self.core.blacklists.denies(track_address, tape) {
                return Err(GatewayCacheError::Removed.into());
            }
            if let Some((data, is_verified)) = self.core.get_cached(key).map_err(E::from)? {
                crate::metrics::inc_cache(if waited { "coalesced" } else { "hit" });
                return Ok(CacheRead {
                    data,
                    source: CacheSource::Hit,
                    is_verified,
                });
            }

            if let Some(wait) = self.inflight.join_or_start_fetch(key).map_err(E::from)? {
                waited = true;
                wait.notified().await;
                continue;
            }

            let result = match fetch.take() {
                Some(fetch) => {
                    let result = fetch().await;
                    match result {
                        Ok(data) => self
                            .accept_fill(key, tape, data)
                            .map(|data| CacheRead {
                                data,
                                source: CacheSource::Miss,
                                is_verified: true,
                            })
                            .map_err(E::from),
                        Err(error) => Err(error),
                    }
                }
                None => Err(GatewayCacheError::State("missing cache fetcher".into()).into()),
            };

            self.inflight.finish_fetch(key);
            if result.is_ok() {
                crate::metrics::inc_cache("miss");
            }
            return result;
        }
    }

    /// Whether a slice is in the cache, from memory alone
    pub fn holds(&self, spool_id: SpoolIndex, track_address: Address) -> bool {
        self.core
            .lock_state()
            .map(|state| state.entries.contains_key(&SliceCacheKey::new(spool_id, track_address)))
            .unwrap_or(false)
    }

    /// Record that a cached slice passed its leaf check, so later hits skip the hash
    pub fn mark_verified(&self, spool_id: SpoolIndex, track_address: Address) -> Result<(), GatewayCacheError> {
        self.core
            .lock_state()?
            .mark_verified(SliceCacheKey::new(spool_id, track_address));
        Ok(())
    }

    /// Delete every cached slice of a track across its group
    pub fn purge_track(&self, track_address: Address, group: GroupIndex) -> Result<(), GatewayCacheError> {
        for position in 0..GROUP_SIZE {
            self.core
                .delete_cached(SliceCacheKey::new(group.spool_at(position), track_address))?;
        }
        Ok(())
    }

    /// Wait for every fill write started so far to land
    pub async fn flush(&self) {
        let mut writes = match self.writes.lock() {
            Ok(mut writes) => std::mem::take(&mut *writes),
            Err(_) => return,
        };
        while writes.join_next().await.is_some() {}
    }

    /// Serve a fetched slice now and store it once the read has its bytes
    fn accept_fill(&self, key: SliceCacheKey, tape: Address, data: Bytes) -> Result<Bytes, GatewayCacheError> {
        if self.core.blacklists.denies(key.track_address, tape) {
            return Err(GatewayCacheError::Removed);
        }
        if self.core.config.max_bytes == 0 {
            return Ok(data);
        }

        self.core.lock_fills()?.insert(key, data.clone());
        self.core.lock_state()?.upsert(key, data.len() as u64, true);

        let (writer, fill) = (self.writer.clone(), data.clone());
        let mut writes = self
            .writes
            .lock()
            .map_err(|_| GatewayCacheError::State("cache write lock poisoned".into()))?;
        while writes.try_join_next().is_some() {}
        writes.spawn_blocking(move || writer.write(key, tape, &fill));
        Ok(data)
    }
}

impl<Db: Store> FillWriter for CacheCore<Db> {
    fn write(&self, key: SliceCacheKey, tape: Address, data: &[u8]) {
        // Held across the check and the write, so a batch applied meanwhile waits
        // for this write and the purge after it deletes what was written.
        let stored = self.blacklists.write_unless_denied(key.track_address, tape, || {
            self.store.put_slice_from(key.spool_id, key.track_address, data)
        });
        if let Ok(mut fills) = self.fills.lock() {
            fills.remove(&key);
        }

        let is_kept = match stored {
            Some(Ok(())) => self.holds(key),
            Some(Err(error)) => {
                warn!(%error, "gateway cache fill write failed");
                false
            }
            None => false,
        };
        if !is_kept {
            if let Err(error) = self.delete_cached(key) {
                warn!(%error, "gateway cache could not drop an unkept fill");
            }
        }

        match self.evict_to_budget() {
            Ok(0) => {}
            Ok(evicted) => {
                crate::metrics::inc_cache_evicted(evicted as u64);
                debug!(evicted, "gateway cache evicted entries after fill");
            }
            Err(error) => warn!(%error, "gateway cache eviction failed"),
        }
    }
}

impl<Db: Store> CacheCore<Db> {
    fn load_state(store: &TapeStore<Db>) -> Result<CacheState, GatewayCacheError> {
        let iter = store
            .inner()
            .inner()
            .iter_from(SliceCol::CF_NAME, &[], Direction::Asc)
            .map_err(store_error)?;
        let mut state = CacheState::default();

        for (key_bytes, value_bytes) in iter {
            let key: SliceKey = wincode::deserialize(&key_bytes)
                .map_err(|error| GatewayCacheError::Codec(format!("slice key: {error}")))?;
            // The budget is the payload the gateway serves, not the sidecar
            // riding in front of it.
            state.upsert(key.into(), slice::payload(&value_bytes).len() as u64, false);
        }

        Ok(state)
    }

    fn holds(&self, key: SliceCacheKey) -> bool {
        self.lock_state()
            .map(|state| state.entries.contains_key(&key))
            .unwrap_or(false)
    }

    fn get_cached(&self, key: SliceCacheKey) -> Result<Option<(Bytes, bool)>, GatewayCacheError> {
        if self.config.max_bytes == 0 {
            return Ok(None);
        }

        let pending = self.lock_fills()?.get(&key).cloned();
        let data = match pending {
            Some(data) => data,
            None => match self.store.get_slice(key.spool_id, key.track_address).map_err(store_error)? {
                Some(data) => Bytes::from(data),
                None => {
                    self.lock_state()?.remove(key);
                    return Ok(None);
                }
            },
        };

        let is_verified = self.lock_state()?.touch(key, data.len() as u64);
        Ok(Some((data, is_verified)))
    }

    fn delete_cached(&self, key: SliceCacheKey) -> Result<(), GatewayCacheError> {
        self.lock_fills()?.remove(&key);
        self.store
            .delete_slice(key.spool_id, key.track_address)
            .map_err(store_error)?;
        self.lock_state()?.remove(key);
        Ok(())
    }

    fn evict_to_budget(&self) -> Result<usize, GatewayCacheError> {
        let max_bytes = self.config.max_bytes;
        let eviction_batch = self.config.eviction_batch.max(1);
        let mut deleted = 0usize;

        loop {
            let mut state = self.lock_state()?;
            let mut batch_deleted = 0usize;
            while batch_deleted < eviction_batch && state.total_bytes > max_bytes {
                let Some(key) = state.oldest() else {
                    break;
                };
                self.store
                    .delete_slice(key.spool_id, key.track_address)
                    .map_err(store_error)?;
                state.remove(key);
                self.lock_fills()?.remove(&key);
                deleted = deleted.saturating_add(1);
                batch_deleted = batch_deleted.saturating_add(1);
            }

            if batch_deleted == 0 || state.total_bytes <= max_bytes {
                break;
            }
        }

        self.maybe_reclaim(deleted)?;
        Ok(deleted)
    }

    fn maybe_reclaim(&self, deleted: usize) -> Result<(), GatewayCacheError> {
        if deleted == 0 || self.config.reclaim_after_deleted_slices == 0 {
            return Ok(());
        }

        let previous = self
            .deleted_since_reclaim
            .fetch_add(deleted, Ordering::Relaxed);
        let pending = previous.saturating_add(deleted);
        if pending < self.config.reclaim_after_deleted_slices {
            return Ok(());
        }

        self.deleted_since_reclaim.store(0, Ordering::Relaxed);
        if let Err(error) = self.store.inner().inner().reclaim_space() {
            warn!(%error, "gateway cache reclaim failed");
            return Err(store_error(error));
        }

        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, CacheState>, GatewayCacheError> {
        self.state
            .lock()
            .map_err(|_| GatewayCacheError::State("cache state lock poisoned".into()))
    }

    fn lock_fills(&self) -> Result<MutexGuard<'_, HashMap<SliceCacheKey, Bytes>>, GatewayCacheError> {
        self.fills
            .lock()
            .map_err(|_| GatewayCacheError::State("cache fill lock poisoned".into()))
    }
}

fn store_error(error: impl Display) -> GatewayCacheError {
    GatewayCacheError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Condvar;

    use tape_store_api::{StoreIter, Value, WriteBatch};
    use tape_store_memory::MemoryStore;
    use std::convert::Infallible;

    use tape_blacklist::batch::{render, render_first};
    use tape_blacklist::blacklists::Blacklists;
    use tape_blacklist::entry::Entry;
    use tape_blacklist::extract::Change;
    use tape_blacklist::subscription::{HonoredAction, Subscription};
    use tape_core::types::TrackNumber;
    use tape_rack::format::render_first as render_rack;
    use tape_rack::record::Record;
    use tape_rack::source::MemorySource;
    use tape_store::ops::SliceOps;

    use super::*;

    /// Blocks slice writes until the test opens it
    #[derive(Clone, Default)]
    struct Gate(Arc<(Mutex<bool>, Condvar)>);

    impl Gate {
        fn open(&self) {
            let (lock, opened) = &*self.0;
            *lock.lock().expect("gate lock") = true;
            opened.notify_all();
        }

        fn is_open(&self) -> bool {
            *self.0.0.lock().expect("gate lock")
        }

        fn wait(&self) {
            let (lock, opened) = &*self.0;
            let mut is_open = lock.lock().expect("gate lock");
            while !*is_open {
                is_open = opened.wait(is_open).expect("gate wait");
            }
        }
    }

    struct GatedStore {
        inner: MemoryStore,
        gate: Gate,
    }

    impl Store for GatedStore {
        fn get(&self, cf: &str, key: &[u8]) -> tape_store_api::Result<Option<Value>> {
            self.inner.get(cf, key)
        }
        fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> tape_store_api::Result<()> {
            if cf == SliceCol::CF_NAME {
                self.gate.wait();
            }
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
        fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_from(cf, start, direction)
        }
        fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> tape_store_api::Result<StoreIter<'_>> {
            self.inner.iter_range(cf, start, end)
        }
        fn live_data_size_bytes(&self) -> tape_store_api::Result<Option<u64>> {
            self.inner.live_data_size_bytes()
        }
        fn key_count_estimate(&self, cf: &str) -> tape_store_api::Result<Option<u64>> {
            self.inner.key_count_estimate(cf)
        }
    }

    fn cache_config(max_bytes: u64) -> GatewayCacheConfig {
        GatewayCacheConfig {
            max_bytes,
            eviction_batch: 16,
            reclaim_after_deleted_slices: 0,
        }
    }

    fn test_store() -> Arc<TapeStore<MemoryStore>> {
        Arc::new(TapeStore::new(MemoryStore::new()))
    }

    fn blacklists() -> Arc<GatewayBlacklists> {
        Arc::new(GatewayBlacklists::default())
    }

    fn test_tape() -> Address {
        Address::new([7u8; 32])
    }

    #[tokio::test]
    async fn cache_hit_avoids_fetch() {
        let store = test_store();
        let track = Address::new_unique();
        let spool = SpoolIndex(7);
        store.put_slice(spool, track, vec![1, 2, 3]).unwrap();

        let cache = GatewaySliceCache::new(store, cache_config(1024), blacklists()).unwrap();
        let fetches = AtomicUsize::new(0);
        let read = cache
            .get_or_insert_with(spool, track, test_tape(), || async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, GatewayCacheError>(Bytes::from(vec![9]))
            })
            .await
            .unwrap();

        assert_eq!(read.source, CacheSource::Hit);
        assert_eq!(read.data, vec![1, 2, 3]);
        assert_eq!(fetches.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_miss_fetches_once() {
        let store = test_store();
        let cache = Arc::new(GatewaySliceCache::new(store, cache_config(1024), blacklists()).unwrap());
        let track = Address::new_unique();
        let spool = SpoolIndex(11);
        let fetches = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let fetches = fetches.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_insert_with(spool, track, test_tape(), || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        Ok::<_, GatewayCacheError>(Bytes::from(vec![4, 5, 6]))
                    })
                    .await
                    .unwrap()
            }));
        }

        let mut misses = 0;
        for task in tasks {
            let read = task.await.unwrap();
            if read.source == CacheSource::Miss {
                misses += 1;
            }
            assert_eq!(read.data, vec![4, 5, 6]);
        }

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
        assert_eq!(misses, 1);
    }

    #[tokio::test]
    async fn evicts_lru_entry_when_budget_is_exceeded() {
        let store = test_store();
        let cache = GatewaySliceCache::new(store.clone(), cache_config(5), blacklists()).unwrap();
        let first = Address::new_unique();
        let second = Address::new_unique();
        let spool = SpoolIndex(3);

        cache
            .get_or_insert_with(spool, first, test_tape(), || async {
                Ok::<_, GatewayCacheError>(Bytes::from(vec![1, 1, 1]))
            })
            .await
            .unwrap();
        cache
            .get_or_insert_with(spool, second, test_tape(), || async {
                Ok::<_, GatewayCacheError>(Bytes::from(vec![2, 2, 2]))
            })
            .await
            .unwrap();
        cache.flush().await;

        assert!(store.get_slice(spool, first).unwrap().is_none());
        assert_eq!(store.get_slice(spool, second).unwrap(), Some(vec![2, 2, 2]));

        let stats = cache.stats().unwrap();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bytes, 3);
    }

    #[tokio::test]
    async fn zero_budget_does_not_persist_fetched_slices() {
        let store = test_store();
        let cache = GatewaySliceCache::new(store.clone(), cache_config(0), blacklists()).unwrap();
        let track = Address::new_unique();
        let spool = SpoolIndex(19);

        let read = cache
            .get_or_insert_with(spool, track, test_tape(), || async {
                Ok::<_, GatewayCacheError>(Bytes::from(vec![7, 8, 9]))
            })
            .await
            .unwrap();

        assert_eq!(read.source, CacheSource::Miss);
        assert_eq!(read.data, vec![7, 8, 9]);
        assert!(store.get_slice(spool, track).unwrap().is_none());
        assert_eq!(cache.stats().unwrap().entries, 0);
    }

    // a miss answers before its slice reaches the store
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_behind() {
        let gate = Gate::default();
        let inner = MemoryStore::new();
        let store = Arc::new(TapeStore::new(GatedStore { inner, gate: gate.clone() }));
        let cache = GatewaySliceCache::new(store.clone(), cache_config(1024), blacklists()).expect("open cache");
        let (track, spool) = (Address::new_unique(), SpoolIndex(8));
        let opener = gate.clone();
        let opening = tokio::task::spawn_blocking(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            opener.open();
        });

        let read = cache
            .get_or_insert_with(spool, track, test_tape(), || async { Ok::<_, GatewayCacheError>(Bytes::from(vec![3])) })
            .await
            .expect("read");
        let answered_first = !gate.is_open();
        opening.await.expect("open gate");
        cache.flush().await;

        assert_eq!(read.data, vec![3]);
        assert!(answered_first);
        assert_eq!(store.get_slice(spool, track).expect("read store"), Some(vec![3]));
    }

    // only slices this process fetched or checked count as verified on a hit
    #[tokio::test]
    async fn verified_hits() {
        let store = test_store();
        let (on_disk, fetched, spool) = (Address::new_unique(), Address::new_unique(), SpoolIndex(2));
        store.put_slice(spool, on_disk, vec![1]).expect("seed store");
        let cache = GatewaySliceCache::new(store, cache_config(1024), blacklists()).expect("open cache");
        let fetch = || async { Ok::<_, GatewayCacheError>(Bytes::from(vec![2])) };
        cache.get_or_insert_with(spool, fetched, test_tape(), fetch).await.expect("fill");

        let old = cache.get_or_insert_with(spool, on_disk, test_tape(), fetch).await.expect("old");
        let new = cache.get_or_insert_with(spool, fetched, test_tape(), fetch).await.expect("new");
        cache.mark_verified(spool, on_disk).expect("mark");
        let checked = cache.get_or_insert_with(spool, on_disk, test_tape(), fetch).await.expect("checked");

        assert_eq!((old.source, old.is_verified), (CacheSource::Hit, false));
        assert_eq!((new.source, new.is_verified), (CacheSource::Hit, true));
        assert!(checked.is_verified);
    }

    // with the cache off a read never touches the store, so a slice written there after startup stays
    #[tokio::test]
    async fn zero_budget() {
        let store = test_store();
        let cache = GatewaySliceCache::new(store.clone(), cache_config(0), blacklists()).expect("open cache");
        let (track, spool) = (Address::new_unique(), SpoolIndex(4));
        store.put_slice(spool, track, vec![5, 5]).expect("seed store");

        let read = cache
            .get_or_insert_with(spool, track, test_tape(), || async {
                Ok::<_, GatewayCacheError>(Bytes::from(vec![6]))
            })
            .await
            .expect("read");

        assert_eq!(read.data, vec![6]);
        assert_eq!(store.get_slice(spool, track).expect("read store"), Some(vec![5, 5]));
    }

    // a fetch that finishes after its track is removed caches nothing, and later reads are refused
    #[tokio::test]
    async fn removed_track() {
        let store = test_store();
        let (rack, list_tape, track) = (Address::new_unique(), Address::new_unique(), Address::new_unique());
        let spool = SpoolIndex(5);
        let mut source = MemorySource::default();
        source.reserve(rack);
        source.write(rack, TrackNumber(0), &render_rack(&[Record::new(list_tape)]));
        source.reserve(list_tape);
        source.write(list_tape, TrackNumber(0), &render_first(&[]).expect("render header"));
        let subscription = Subscription {
            name: "list".to_string(),
            tape: rack,
            actions: vec![HonoredAction::Remove],
        };
        let blacklists = Arc::new(GatewayBlacklists::new(
            Blacklists::try_new(vec![subscription]).expect("subscriptions"),
        ));
        let rebuilt: Result<_, Infallible> = blacklists.rebuild(&source);
        assert!(rebuilt.is_ok());
        let cache = GatewaySliceCache::new(store.clone(), cache_config(1024), blacklists.clone())
            .expect("open cache");
        source.write(list_tape, TrackNumber(1), &render(&[Entry::remove(track)]).expect("render"));
        let removal = [Change::Written {
            tape: list_tape,
            track_number: TrackNumber(1),
        }];

        let read = cache
            .get_or_insert_with(spool, track, test_tape(), || async {
                let applied: Result<_, Infallible> = blacklists.apply(&removal, &source);
                assert!(applied.is_ok());
                Ok::<_, GatewayCacheError>(Bytes::from(vec![1, 2, 3]))
            })
            .await;
        let again = cache
            .get_or_insert_with(spool, track, test_tape(), || async {
                Ok::<_, GatewayCacheError>(Bytes::from(vec![9]))
            })
            .await;

        assert!(matches!(read, Err(GatewayCacheError::Removed)));
        assert!(matches!(again, Err(GatewayCacheError::Removed)));
        assert!(store.get_slice(spool, track).expect("read store").is_none());
    }
}
