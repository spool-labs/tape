use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use store::{Column, Direction, Store};
use tape_core::types::SpoolIndex;
use tape_crypto::address::Address;
use tape_node::config::gateway::GatewayCacheConfig;
use tape_store::columns::SliceCol;
use tape_store::ops::SliceOps;
use tape_store::types::{SliceKey, SliceValue};
use tape_store::TapeStore;
use tokio::sync::Notify;
use tracing::{debug, warn};

#[derive(Debug)]
pub enum GatewayCacheError {
    Store(String),
    Codec(String),
    State(String),
}

impl Display for GatewayCacheError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(message) => write!(f, "store error: {message}"),
            Self::Codec(message) => write!(f, "codec error: {message}"),
            Self::State(message) => write!(f, "cache state error: {message}"),
        }
    }
}

impl Error for GatewayCacheError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SliceCacheKey {
    pub spool_id: SpoolIndex,
    pub track_address: Address,
}

impl SliceCacheKey {
    pub fn new(spool_id: SpoolIndex, track_address: Address) -> Self {
        Self {
            spool_id,
            track_address,
        }
    }
}

impl From<SliceKey> for SliceCacheKey {
    fn from(key: SliceKey) -> Self {
        Self::new(key.spool_id, key.track_address)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheSource {
    Hit,
    Miss,
}

#[derive(Debug)]
pub struct CacheRead {
    pub data: Vec<u8>,
    pub source: CacheSource,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: u64,
    pub inflight: usize,
}

#[derive(Clone, Copy, Debug)]
struct CacheEntry {
    size: u64,
    last_access: u64,
}

#[derive(Debug, Default)]
struct CacheState {
    entries: HashMap<SliceCacheKey, CacheEntry>,
    total_bytes: u64,
    clock: u64,
}

impl CacheState {
    fn next_access(&mut self) -> u64 {
        self.clock = self.clock.saturating_add(1);
        self.clock
    }

    fn upsert(&mut self, key: SliceCacheKey, size: u64) {
        let last_access = self.next_access();
        if let Some(previous) = self.entries.insert(key, CacheEntry { size, last_access }) {
            self.total_bytes = self.total_bytes.saturating_sub(previous.size);
        }
        self.total_bytes = self.total_bytes.saturating_add(size);
    }

    fn touch(&mut self, key: SliceCacheKey, size: u64) {
        let last_access = self.next_access();
        match self.entries.get_mut(&key) {
            Some(entry) => {
                if entry.size != size {
                    self.total_bytes = self.total_bytes.saturating_sub(entry.size);
                    self.total_bytes = self.total_bytes.saturating_add(size);
                    entry.size = size;
                }
                entry.last_access = last_access;
            }
            None => {
                self.entries.insert(key, CacheEntry { size, last_access });
                self.total_bytes = self.total_bytes.saturating_add(size);
            }
        }
    }

    fn remove(&mut self, key: SliceCacheKey) -> Option<CacheEntry> {
        let removed = self.entries.remove(&key)?;
        self.total_bytes = self.total_bytes.saturating_sub(removed.size);
        Some(removed)
    }
}

pub struct GatewaySliceCache<Db: Store> {
    store: Arc<TapeStore<Db>>,
    config: GatewayCacheConfig,
    state: Mutex<CacheState>,
    inflight: Mutex<HashMap<SliceCacheKey, Arc<Notify>>>,
    deleted_since_reclaim: AtomicUsize,
}

impl<Db: Store> GatewaySliceCache<Db> {
    pub fn new(
        store: Arc<TapeStore<Db>>,
        config: GatewayCacheConfig,
    ) -> Result<Self, GatewayCacheError> {
        let state = Self::load_state(store.as_ref())?;
        let cache = Self {
            store,
            config,
            state: Mutex::new(state),
            inflight: Mutex::new(HashMap::new()),
            deleted_since_reclaim: AtomicUsize::new(0),
        };
        let evicted = cache.evict_to_budget()?;
        if evicted > 0 {
            debug!(evicted, "gateway cache evicted startup entries");
        }
        Ok(cache)
    }

    pub fn stats(&self) -> Result<CacheStats, GatewayCacheError> {
        let state = self.lock_state()?;
        let inflight = self.lock_inflight()?.len();
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
        fetch: F,
    ) -> Result<CacheRead, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, E>>,
        E: From<GatewayCacheError>,
    {
        let key = SliceCacheKey::new(spool_id, track_address);
        let mut fetch = Some(fetch);

        loop {
            if let Some(data) = self.get_cached(key).map_err(E::from)? {
                return Ok(CacheRead {
                    data,
                    source: CacheSource::Hit,
                });
            }

            if let Some(wait) = self.join_or_start_fetch(key).map_err(E::from)? {
                wait.notified().await;
                continue;
            }

            let result = match fetch.take() {
                Some(fetch) => {
                    let result = fetch().await;
                    match result {
                        Ok(data) => self
                            .store_fetched(key, data)
                            .map(|data| CacheRead {
                                data,
                                source: CacheSource::Miss,
                            })
                            .map_err(E::from),
                        Err(error) => Err(error),
                    }
                }
                None => Err(GatewayCacheError::State("missing cache fetcher".into()).into()),
            };

            self.finish_fetch(key);
            return result;
        }
    }

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
            let value: SliceValue = wincode::deserialize(&value_bytes)
                .map_err(|error| GatewayCacheError::Codec(format!("slice value: {error}")))?;
            state.upsert(key.into(), value.0.len() as u64);
        }

        Ok(state)
    }

    fn get_cached(&self, key: SliceCacheKey) -> Result<Option<Vec<u8>>, GatewayCacheError> {
        if self.config.max_bytes == 0 {
            self.delete_cached(key)?;
            return Ok(None);
        }

        let data = self
            .store
            .get_slice(key.spool_id, key.track_address)
            .map_err(store_error)?;

        let Some(data) = data else {
            let mut state = self.lock_state()?;
            state.remove(key);
            return Ok(None);
        };

        let mut state = self.lock_state()?;
        state.touch(key, data.len() as u64);
        Ok(Some(data))
    }

    fn store_fetched(
        &self,
        key: SliceCacheKey,
        data: Vec<u8>,
    ) -> Result<Vec<u8>, GatewayCacheError> {
        if self.config.max_bytes == 0 {
            self.delete_cached(key)?;
            return Ok(data);
        }

        self.store
            .put_slice(key.spool_id, key.track_address, data.clone())
            .map_err(store_error)?;

        {
            let mut state = self.lock_state()?;
            state.upsert(key, data.len() as u64);
        }

        let evicted = self.evict_to_budget()?;
        if evicted > 0 {
            debug!(evicted, "gateway cache evicted entries after fill");
        }

        Ok(data)
    }

    fn delete_cached(&self, key: SliceCacheKey) -> Result<(), GatewayCacheError> {
        self.store
            .delete_slice(key.spool_id, key.track_address)
            .map_err(store_error)?;
        let mut state = self.lock_state()?;
        state.remove(key);
        Ok(())
    }

    fn join_or_start_fetch(
        &self,
        key: SliceCacheKey,
    ) -> Result<Option<Arc<Notify>>, GatewayCacheError> {
        let mut inflight = self.lock_inflight()?;
        if let Some(wait) = inflight.get(&key) {
            return Ok(Some(wait.clone()));
        }

        inflight.insert(key, Arc::new(Notify::new()));
        Ok(None)
    }

    fn finish_fetch(&self, key: SliceCacheKey) {
        let notify = self
            .lock_inflight()
            .ok()
            .and_then(|mut inflight| inflight.remove(&key));

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    fn evict_to_budget(&self) -> Result<usize, GatewayCacheError> {
        let max_bytes = self.config.max_bytes;
        let eviction_batch = self.config.eviction_batch.max(1);
        let mut deleted = 0usize;

        loop {
            let mut state = self.lock_state()?;
            if state.total_bytes <= max_bytes || state.entries.is_empty() {
                break;
            }

            let mut victims = state
                .entries
                .iter()
                .map(|(key, entry)| (*key, entry.last_access))
                .collect::<Vec<_>>();
            victims.sort_by_key(|(_, last_access)| *last_access);

            let mut batch_deleted = 0usize;
            for (key, _) in victims.into_iter().take(eviction_batch) {
                self.store
                    .delete_slice(key.spool_id, key.track_address)
                    .map_err(store_error)?;
                state.remove(key);
                deleted = deleted.saturating_add(1);
                batch_deleted = batch_deleted.saturating_add(1);

                if state.total_bytes <= max_bytes {
                    break;
                }
            }

            if batch_deleted == 0 {
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

    fn lock_inflight(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<SliceCacheKey, Arc<Notify>>>, GatewayCacheError> {
        self.inflight
            .lock()
            .map_err(|_| GatewayCacheError::State("in-flight cache lock poisoned".into()))
    }
}

fn store_error(error: impl Display) -> GatewayCacheError {
    GatewayCacheError::Store(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use store_memory::MemoryStore;
    use tape_store::ops::SliceOps;

    use super::*;

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

    #[tokio::test]
    async fn cache_hit_avoids_fetch() {
        let store = test_store();
        let track = Address::new_unique();
        let spool = SpoolIndex(7);
        store.put_slice(spool, track, vec![1, 2, 3]).unwrap();

        let cache = GatewaySliceCache::new(store, cache_config(1024)).unwrap();
        let fetches = AtomicUsize::new(0);
        let read = cache
            .get_or_insert_with(spool, track, || async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, GatewayCacheError>(vec![9])
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
        let cache = Arc::new(GatewaySliceCache::new(store, cache_config(1024)).unwrap());
        let track = Address::new_unique();
        let spool = SpoolIndex(11);
        let fetches = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let fetches = fetches.clone();
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_insert_with(spool, track, || async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                        Ok::<_, GatewayCacheError>(vec![4, 5, 6])
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
        let cache = GatewaySliceCache::new(store.clone(), cache_config(5)).unwrap();
        let first = Address::new_unique();
        let second = Address::new_unique();
        let spool = SpoolIndex(3);

        cache
            .get_or_insert_with(spool, first, || async {
                Ok::<_, GatewayCacheError>(vec![1, 1, 1])
            })
            .await
            .unwrap();
        cache
            .get_or_insert_with(spool, second, || async {
                Ok::<_, GatewayCacheError>(vec![2, 2, 2])
            })
            .await
            .unwrap();

        assert!(store.get_slice(spool, first).unwrap().is_none());
        assert_eq!(store.get_slice(spool, second).unwrap(), Some(vec![2, 2, 2]));

        let stats = cache.stats().unwrap();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bytes, 3);
    }

    #[tokio::test]
    async fn zero_budget_does_not_persist_fetched_slices() {
        let store = test_store();
        let cache = GatewaySliceCache::new(store.clone(), cache_config(0)).unwrap();
        let track = Address::new_unique();
        let spool = SpoolIndex(19);

        let read = cache
            .get_or_insert_with(spool, track, || async {
                Ok::<_, GatewayCacheError>(vec![7, 8, 9])
            })
            .await
            .unwrap();

        assert_eq!(read.source, CacheSource::Miss);
        assert_eq!(read.data, vec![7, 8, 9]);
        assert!(store.get_slice(spool, track).unwrap().is_none());
        assert_eq!(cache.stats().unwrap().entries, 0);
    }
}
