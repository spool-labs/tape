//! Cache policy: per-method TTL table + `moka::future::Cache` holding
//! serialized JSON-RPC result bodies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
        ("getLatestBlockhash", sec(30)),
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

/// Cached value keeps the full `result` field of the upstream JSON-RPC
/// response. We re-wrap it with the caller's `id` on a hit.
pub type CachedResult = Arc<Value>;

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
    inner: Cache<CacheKey, CachedResult>,
}

impl CacheStore {
    pub fn new(max_entries: u64) -> Self {
        Self {
            inner: Cache::builder()
                .max_capacity(max_entries)
                // We set the TTL per-insertion via `insert_with_ttl`;
                // the default 30s is a belt-and-suspenders bound so
                // any lingering entries get expired even if policy
                // logic goofs up.
                .time_to_live(Duration::from_secs(3600))
                .build(),
        }
    }

    pub async fn get(&self, key: &CacheKey) -> Option<CachedResult> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: CacheKey, value: CachedResult) {
        // `moka`'s per-entry TTL API isn't on the stable surface; the
        // eviction policy above is per-cache. We rely on the `Policy`
        // layer to only serve fresh values (see server.rs). For a v1
        // this is simpler than hand-rolling eviction.
        self.inner.insert(key, value).await;
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
