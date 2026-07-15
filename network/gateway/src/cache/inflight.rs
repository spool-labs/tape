use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::Notify;

use super::error::GatewayCacheError;
use super::state::SliceCacheKey;

#[derive(Debug, Default)]
pub struct InflightFetches {
    entries: Mutex<HashMap<SliceCacheKey, Arc<Notify>>>,
}

impl InflightFetches {
    pub fn len(&self) -> Result<usize, GatewayCacheError> {
        Ok(self.lock()?.len())
    }

    pub fn join_or_start_fetch(
        &self,
        key: SliceCacheKey,
    ) -> Result<Option<Arc<Notify>>, GatewayCacheError> {
        let mut entries = self.lock()?;
        if let Some(wait) = entries.get(&key) {
            return Ok(Some(wait.clone()));
        }

        entries.insert(key, Arc::new(Notify::new()));
        Ok(None)
    }

    pub fn finish_fetch(&self, key: SliceCacheKey) {
        let notify = self.lock().ok().and_then(|mut entries| entries.remove(&key));

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    fn lock(
        &self,
    ) -> Result<MutexGuard<'_, HashMap<SliceCacheKey, Arc<Notify>>>, GatewayCacheError> {
        self.entries
            .lock()
            .map_err(|_| GatewayCacheError::State("in-flight cache lock poisoned".into()))
    }
}
