//! Cache policy: per-method TTL table + `moka::future::Cache` holding
//! serialized JSON-RPC result bodies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::future::Cache;
use serde_json::Value;

use crate::key::CacheKey;

/// How we treat an inbound JSON-RPC request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodKind {
    /// Cacheable read. The TTL baked in here already reflects any
    /// per-config override applied at startup.
    Read { ttl: Duration },
    /// Write path — forwarded, logged, never cached.
    Submit,
    /// Unknown method. Forward unchanged, no cache.
    Unknown,
}

/// Default TTL table. Any method not listed here is treated as `Unknown`
/// and passes through. See `docs/rpc-cache.md` for the rationale behind
/// each number.
fn default_ttls() -> HashMap<&'static str, Duration> {
    let sec = Duration::from_secs;
    let ms = Duration::from_millis;
    HashMap::from([
        ("getSlot", sec(2)),
        ("getBlock", sec(300)),
        ("getBlockHeight", sec(2)),
        // Blockhashes expire on-chain after ~90s of slots; by the time
        // we serve a cached value, queue it through the node's tx
        // pipeline, and land it, anything older than a few seconds is
        // likely to be rejected as BlockhashExpired. Keep this tight.
        ("getLatestBlockhash", sec(5)),
        ("getAccountInfo", sec(2)),
        ("getMultipleAccounts", sec(2)),
        ("getProgramAccounts", sec(15)),
        ("getTransaction", sec(300)),
        ("getSignatureStatus", ms(500)),
        ("getSignatureStatuses", ms(500)),
        ("getEpochInfo", sec(30)),
        ("getVersion", sec(3600)),
        ("getGenesisHash", sec(3600)),
        ("getMinimumBalanceForRentExemption", sec(300)),
    ])
}

const SUBMIT_METHODS: &[&str] = &[
    "sendTransaction",
    "sendAndConfirmTransaction",
    "simulateTransaction",
];

/// A cached `result` body plus its freshness deadline. The deadline is
/// per-entry because TTLs vary by method (e.g. `getLatestBlockhash`
/// must be short, `getBlock` can be long). moka's built-in
/// `time_to_live` is per-cache, so we stamp the expiry on insert and
/// check it on read; the global TTL on the builder is a loose ceiling.
#[derive(Clone)]
pub struct CachedEntry {
    pub value: Arc<Value>,
    pub expires_at: Instant,
}

pub struct Policy {
    ttls: HashMap<String, Duration>,
}

impl Policy {
    pub fn new(overrides: HashMap<String, Duration>) -> Self {
        let mut ttls: HashMap<String, Duration> = default_ttls()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        ttls.extend(overrides);
        Self { ttls }
    }

    pub fn classify(&self, method: &str) -> MethodKind {
        if SUBMIT_METHODS.contains(&method) {
            return MethodKind::Submit;
        }
        if let Some(ttl) = self.ttls.get(method) {
            return MethodKind::Read { ttl: *ttl };
        }
        MethodKind::Unknown
    }
}

/// Thin wrapper around the `moka` cache. Holding it in its own struct so
/// the server can inject a test implementation if we ever add one.
pub struct CacheStore {
    inner: Cache<CacheKey, CachedEntry>,
}

impl CacheStore {
    pub fn new(max_entries: u64) -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(max_entries)
                // Ceiling to keep stale entries from hanging around
                // forever if the read rate drops. Per-entry freshness
                // is enforced separately via `expires_at`.
                .time_to_live(Duration::from_secs(3600))
                .build(),
        }
    }

    /// Look up a fresh entry. If the entry is past its per-method TTL
    /// we treat it as a miss and ask the caller to repopulate.
    pub async fn get(&self, key: &CacheKey) -> Option<Arc<Value>> {
        let entry = self.inner.get(key).await?;
        if Instant::now() >= entry.expires_at {
            self.inner.invalidate(key).await;
            return None;
        }
        Some(entry.value)
    }

    pub async fn insert(&self, key: CacheKey, value: Arc<Value>, ttl: Duration) {
        let entry = CachedEntry {
            value,
            expires_at: Instant::now() + ttl,
        };
        self.inner.insert(key, entry).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_known_read() {
        let p = Policy::new(HashMap::new());
        assert!(matches!(p.classify("getSlot"), MethodKind::Read { .. }));
    }

    #[test]
    fn classify_submit() {
        let p = Policy::new(HashMap::new());
        assert_eq!(p.classify("sendTransaction"), MethodKind::Submit);
    }

    #[test]
    fn classify_unknown() {
        let p = Policy::new(HashMap::new());
        assert_eq!(p.classify("totallyMadeUpMethod"), MethodKind::Unknown);
    }

    #[tokio::test]
    async fn entry_expires_after_ttl() {
        let store = CacheStore::new(16);
        let key = CacheKey { method: "getLatestBlockhash".into(), params_hash: 0 };
        store
            .insert(key.clone(), Arc::new(Value::Null), Duration::from_millis(20))
            .await;
        assert!(store.get(&key).await.is_some(), "fresh entry hits");
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(store.get(&key).await.is_none(), "stale entry misses");
    }

    #[test]
    fn override_shadows_default() {
        let overrides =
            HashMap::from([("getSlot".to_string(), Duration::from_secs(10))]);
        let p = Policy::new(overrides);
        match p.classify("getSlot") {
            MethodKind::Read { ttl } => assert_eq!(ttl, Duration::from_secs(10)),
            _ => panic!("should be Read"),
        }
    }
}
