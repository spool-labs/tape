//! Per-tape site policy read from a reserved object on the tape
//!
//! A site can carry its own serving policy as a small JSON object named
//! at the tape root. Absent fields fall back to the gateway-wide config,
//! and the operator can disable tenant policy entirely.

use rpc::Rpc;
use serde::Deserialize;
use store::Store;
use tape_core::track::data::BlobData;
use tape_crypto::address::Address;
use tape_node::config::gateway::is_valid_origin;
use tape_protocol::Api;
use tape_store::ops::TrackDataOps;
use tracing::debug;

use tape_store::TapeStore;

use crate::http::handlers::resolve::{Readable, resolve_readable};
use crate::http::state::AppState;
use crate::staging::StagingStore;

pub const SITE_POLICY_OBJECT: &str = "_site.json";

/// The policy stays inline-sized; anything larger is treated as absent.
const MAX_POLICY_BYTES: usize = 4096;

/// Ceiling on a tenant-chosen revalidation window, mirroring the record-TTL
/// clamp on domain bindings: a tape must not pin stale content for a year.
const MAX_TENANT_MAX_AGE_SECS: u64 = 86_400;

/// Site policy a tape sets for itself; every field optional, unknown
/// fields ignored so older gateways serve newer sites.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct TapeSitePolicy {
    pub spa_fallback: Option<bool>,
    pub cors_origins: Option<Vec<String>>,
    pub connect_origins: Option<Vec<String>>,
    pub max_age_secs: Option<u64>,
}

/// Read the tape's site policy, or an empty one when the tape has none,
/// the operator disabled tenant overrides, or the object is unusable.
pub fn tape_site_policy<Db: Store, Cluster: Api, Blockchain: Rpc>(
    state: &AppState<Db, Cluster, Blockchain>,
    tape: Address,
) -> TapeSitePolicy {
    site_policy(
        state.context.store.as_ref(),
        state.staging.as_ref(),
        state.context.config.gateway.site.tenant_overrides,
        tape,
    )
}

/// The policy behind `tape_site_policy`, over the store and queue it reads.
fn site_policy<Db: Store>(
    store: &TapeStore<Db>,
    staging: &StagingStore<Db>,
    has_tenant_overrides: bool,
    tape: Address,
) -> TapeSitePolicy {
    if !has_tenant_overrides {
        return TapeSitePolicy::default();
    }

    // Queue first, or a site's policy is ignored for the minute between its
    // deploy and the drain landing it, and a single-page app 404s its routes.
    let readable = match resolve_readable(store, staging, tape, SITE_POLICY_OBJECT.as_bytes()) {
        Ok(Some(readable)) => readable,
        Ok(None) => return TapeSitePolicy::default(),
        Err(error) => {
            debug!(%tape, %error, "site policy lookup failed");
            return TapeSitePolicy::default();
        }
    };

    let Some(bytes) = policy_bytes(store, staging, tape, readable) else {
        return TapeSitePolicy::default();
    };
    match serde_json::from_slice(&bytes) {
        Ok(policy) => sanitized(policy),
        Err(error) => {
            debug!(%tape, %error, "site policy does not parse, ignored");
            TapeSitePolicy::default()
        }
    }
}

/// The policy object's bytes, from the write queue while it still holds them.
fn policy_bytes<Db: Store>(
    store: &TapeStore<Db>,
    staging: &StagingStore<Db>,
    tape: Address,
    readable: Readable,
) -> Option<Vec<u8>> {
    match readable {
        Readable::Queued(object) => {
            if is_oversize(tape, object.size) {
                return None;
            }
            staging.bytes(tape, SITE_POLICY_OBJECT.as_bytes()).ok().flatten()
        }
        Readable::Track(resolved) => {
            if is_oversize(tape, resolved.size) {
                return None;
            }
            let data = store.get_track_data(resolved.track_address);
            let Ok(Some(BlobData::Inline(bytes))) = data else {
                return None;
            };
            Some(bytes)
        }
    }
}

/// Whether the policy object is past the inline size it is allowed to take.
fn is_oversize(tape: Address, size: u64) -> bool {
    if size <= MAX_POLICY_BYTES as u64 {
        return false;
    }
    debug!(%tape, size, "site policy too large, ignored");
    true
}

/// Tame tenant-supplied values: drop origins that could break out of a
/// header value or name no scheme, and cap the revalidation window. The
/// tape owner controls the values, the gateway controls that they stay
/// well-formed.
fn sanitized(mut policy: TapeSitePolicy) -> TapeSitePolicy {
    if let Some(origins) = policy.cors_origins.as_mut() {
        origins.retain(|origin| is_valid_origin(origin));
    }
    if let Some(origins) = policy.connect_origins.as_mut() {
        origins.retain(|origin| is_valid_origin(origin));
    }
    policy.max_age_secs = policy
        .max_age_secs
        .map(|secs| secs.min(MAX_TENANT_MAX_AGE_SECS));
    policy
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use store_memory::MemoryStore;
    use tape_core::types::ContentType;
    use tape_crypto::Hash;

    use super::*;

    // a policy still in the write queue applies before anything is indexed
    #[tokio::test]
    async fn queued_policy() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let staging = StagingStore::try_new(store.clone()).expect("open queue");
        let tape = Address::new([1u8; 32]);
        staging
            .enqueue_put(
                tape,
                SITE_POLICY_OBJECT.as_bytes(),
                br#"{"spa_fallback": true}"#.to_vec(),
                ContentType::Unknown,
                Hash([9u8; 32]),
                1_700_000_000,
            )
            .await
            .expect("enqueue policy");

        let policy = site_policy(&store, &staging, true, tape);

        assert_eq!(policy.spa_fallback, Some(true));
    }

    // a tape with no policy anywhere serves the gateway defaults
    #[tokio::test]
    async fn absent_policy() {
        let store = Arc::new(TapeStore::new(MemoryStore::new()));
        let staging = StagingStore::try_new(store.clone()).expect("open queue");

        let policy = site_policy(&store, &staging, true, Address::new([2u8; 32]));

        assert!(policy.spa_fallback.is_none());
    }

    // tenant origins keep valid entries and drop malformed or unsafe ones
    #[test]
    fn origin_sanitizing() {
        let policy = sanitized(TapeSitePolicy {
            spa_fallback: None,
            cors_origins: Some(vec!["https://a.example".into(), "javascript:x".into()]),
            connect_origins: Some(vec![
                "wss://rpc.example".into(),
                "https://x; script-src *".into(),
                "*".into(),
            ]),
            max_age_secs: None,
        });

        assert_eq!(policy.cors_origins.as_deref(), Some(&["https://a.example".to_string()][..]));
        assert_eq!(
            policy.connect_origins.as_deref(),
            Some(&["wss://rpc.example".to_string(), "*".to_string()][..])
        );
    }

    // a tenant cannot pin content in caches beyond the ceiling
    #[test]
    fn max_age_clamp() {
        let policy = sanitized(TapeSitePolicy {
            spa_fallback: None,
            cors_origins: None,
            connect_origins: None,
            max_age_secs: Some(31_536_000),
        });
        assert_eq!(policy.max_age_secs, Some(MAX_TENANT_MAX_AGE_SECS));
    }

    // unknown fields are ignored so older gateways serve newer sites
    #[test]
    fn lenient_parsing() {
        let policy: TapeSitePolicy =
            serde_json::from_str(r#"{"max_age_secs": 5, "future_field": true}"#)
                .expect("policy should parse");
        assert_eq!(policy.max_age_secs, Some(5));
        assert!(policy.spa_fallback.is_none());
    }
}
