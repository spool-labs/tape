//! Process-global TTL cache for the serialized board and network responses.
//!
//! Many viewers can point at one node at once. Building and serializing a
//! response is the real cost, so it runs at most once per TTL and every other
//! request clones the bytes it produced.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use rpc::Rpc;
use serde::Serialize;
use store::Store;
use tape_protocol::Api;

use super::board;
use crate::context::NodeContext;

/// How long a serialized response is reused before a rebuild. Kept below the
/// dashboard poll cadence so a lone viewer still gets fresh data each poll.
const TTL: Duration = Duration::from_secs(1);

/// Single-value TTL cache over serialized JSON. The build + serialize run under
/// the lock, so a thundering herd triggers exactly one build; the rest clone the
/// bytes it produced.
struct TtlCache {
    slot: Mutex<Option<(Instant, Bytes)>>,
}

impl TtlCache {
    const fn new() -> Self {
        Self { slot: Mutex::new(None) }
    }

    fn get_or_build<T: Serialize>(&self, build: impl FnOnce() -> T) -> Bytes {
        let mut slot = self.slot.lock().unwrap();
        if let Some((built_at, bytes)) = slot.as_ref() {
            if built_at.elapsed() < TTL {
                return bytes.clone();
            }
        }
        let bytes = Bytes::from(serde_json::to_vec(&build()).unwrap_or_default());
        *slot = Some((Instant::now(), bytes.clone()));
        bytes
    }
}

static BOARD_CACHE: TtlCache = TtlCache::new();
static NETWORK_CACHE: TtlCache = TtlCache::new();

/// Serialized per-node board, built at most once per TTL across all callers.
pub fn cached_board<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Bytes
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    BOARD_CACHE.get_or_build(|| board::build(context))
}

/// Serialized whole-network view, built at most once per TTL across all callers.
pub fn cached_network<Db, Cluster, Blockchain>(
    context: &NodeContext<Db, Cluster, Blockchain>,
) -> Bytes
where
    Db: Store + 'static,
    Cluster: Api,
    Blockchain: Rpc,
{
    NETWORK_CACHE.get_or_build(|| board::build_network(context))
}
