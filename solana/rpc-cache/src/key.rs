//! Cache-key derivation. Strips the JSON-RPC `id` field (randomly set per
//! call) so two requests for the same method+params share a cache slot.

use std::hash::{Hash, Hasher};

use serde::Deserialize;
use serde_json::Value;

/// What we cache on. Using a hashed 128-bit key keeps the map compact
/// regardless of param size (e.g. a `getProgramAccountsWithConfig`
/// with a filter array).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub method: String,
    /// Stable hash of the normalized params (we avoid keeping the full
    /// params value in memory per cache entry).
    pub params_hash: u64,
}

/// Minimal shape of a JSON-RPC request we need to classify and key.
/// We do NOT use this for parsing — the raw JSON flows through to
/// upstream unchanged. This is purely for inspection.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest<'a> {
    #[serde(borrow, default)]
    pub jsonrpc: Option<&'a str>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    // `id` intentionally absent; we ignore it for key derivation.
}

impl CacheKey {
    pub fn from_request(method: &str, params: &Value) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        canonical_hash(params, &mut hasher);
        CacheKey {
            method: method.to_string(),
            params_hash: hasher.finish(),
        }
    }
}

/// Walk the JSON value and feed it into the hasher in a canonical order.
/// Maps are traversed with keys sorted so `{"a":1,"b":2}` and
/// `{"b":2,"a":1}` hash to the same value.
fn canonical_hash(value: &Value, hasher: &mut impl Hasher) {
    match value {
        Value::Null => 0u8.hash(hasher),
        Value::Bool(b) => {
            1u8.hash(hasher);
            b.hash(hasher);
        }
        Value::Number(n) => {
            2u8.hash(hasher);
            n.to_string().hash(hasher);
        }
        Value::String(s) => {
            3u8.hash(hasher);
            s.hash(hasher);
        }
        Value::Array(items) => {
            4u8.hash(hasher);
            items.len().hash(hasher);
            for item in items {
                canonical_hash(item, hasher);
            }
        }
        Value::Object(map) => {
            5u8.hash(hasher);
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            keys.len().hash(hasher);
            for k in keys {
                k.hash(hasher);
                canonical_hash(&map[k], hasher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn params_order_independent() {
        let a = CacheKey::from_request("getAccountInfo", &json!({"encoding": "base64", "commitment": "confirmed"}));
        let b = CacheKey::from_request("getAccountInfo", &json!({"commitment": "confirmed", "encoding": "base64"}));
        assert_eq!(a, b);
    }

    #[test]
    fn different_params_different_keys() {
        let a = CacheKey::from_request("getSlot", &json!([]));
        let b = CacheKey::from_request("getSlot", &json!([{"commitment": "finalized"}]));
        assert_ne!(a, b);
    }

    #[test]
    fn different_methods_different_keys() {
        let a = CacheKey::from_request("getSlot", &json!([]));
        let b = CacheKey::from_request("getBlockHeight", &json!([]));
        assert_ne!(a, b);
    }
}
