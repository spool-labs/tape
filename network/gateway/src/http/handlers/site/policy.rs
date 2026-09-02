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

use crate::http::handlers::resolve::resolve_object;
use crate::http::state::AppState;

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
    if !state.context.config.gateway.site.tenant_overrides {
        return TapeSitePolicy::default();
    }

    let resolved = match resolve_object(state.context.store.as_ref(), tape, SITE_POLICY_OBJECT.as_bytes()) {
        Ok(Some(resolved)) => resolved,
        Ok(None) => return TapeSitePolicy::default(),
        Err(error) => {
            debug!(%tape, %error, "site policy lookup failed");
            return TapeSitePolicy::default();
        }
    };
    if resolved.size > MAX_POLICY_BYTES as u64 {
        debug!(%tape, size = resolved.size, "site policy too large, ignored");
        return TapeSitePolicy::default();
    }

    let data = state.context.store.get_track_data(resolved.track_address);
    let Ok(Some(BlobData::Inline(bytes))) = data else {
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
    use super::*;

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
