use serde::{Deserialize, Serialize};

/// Node API configuration root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeApiConfig {
    /// Transport security and pinning controls.
    #[serde(default)]
    pub transport_security: TransportSecurityConfig,

    /// Request sizing and endpoint concurrency controls.
    #[serde(default)]
    pub ingress_limits: IngressLimitsConfig,
}

impl Default for NodeApiConfig {
    fn default() -> Self {
        Self {
            transport_security: TransportSecurityConfig::default(),
            ingress_limits: IngressLimitsConfig::default(),
        }
    }
}

/// Runtime controls for mTLS/pinning behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportSecurityConfig {
    /// Grace period (seconds) for prior TLS peer keys during key rotation.
    #[serde(default = "pin_ttl_default")]
    pub pin_ttl_secs: u64,

    /// Maximum accepted keys per peer in grace cache.
    #[serde(default = "pin_keys_default")]
    pub pin_keys_max: usize,

    /// Require peer TLS identity on protected routes.
    #[serde(default = "peer_id_default")]
    pub peer_id_enforce: bool,
}

impl Default for TransportSecurityConfig {
    fn default() -> Self {
        Self {
            pin_ttl_secs: pin_ttl_default(),
            pin_keys_max: pin_keys_default(),
            peer_id_enforce: peer_id_default(),
        }
    }
}

/// Runtime controls for API ingress body sizes and endpoint concurrency limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngressLimitsConfig {
    /// Enable public unauthenticated ingest routes (/v1/tracks/* PUT).
    #[serde(default = "default_public_ingest")]
    pub public_ingest: bool,

    /// Maximum request body size for PUT slice.
    #[serde(default = "slice_body_default")]
    pub slice_body_max: usize,

    /// Maximum request body size for PUT metadata.
    #[serde(default = "metadata_body_default")]
    pub metadata_body_max: usize,

    /// Maximum request body size for sync spool requests.
    #[serde(default = "sync_body_default")]
    pub sync_body_max: usize,

    /// Maximum request body size for repair requests.
    #[serde(default = "repair_body_default")]
    pub repair_body_max: usize,

    /// Maximum request body size for inconsistency proof requests.
    #[serde(default = "inconsistency_body_default")]
    pub inconsistency_body_max: usize,

    /// Optional cap on concurrently handled sync_spool requests.
    #[serde(default = "sync_limit_default")]
    pub sync_spool_limit: Option<usize>,

    /// Optional cap on concurrently handled repair requests.
    #[serde(default = "repair_limit_default")]
    pub repair_limit: Option<usize>,

    /// Optional cap on concurrently handled inconsistency requests.
    #[serde(default = "inconsistency_limit_default")]
    pub inconsistency_limit: Option<usize>,

    /// Optional cap on concurrently handled public PUT slice requests.
    #[serde(default = "public_slice_default")]
    pub public_slice_limit: Option<usize>,

    /// Optional cap on concurrently handled public PUT metadata requests.
    #[serde(default = "public_metadata_default")]
    pub public_metadata_limit: Option<usize>,
}

impl Default for IngressLimitsConfig {
    fn default() -> Self {
        Self {
            public_ingest: default_public_ingest(),
            slice_body_max: slice_body_default(),
            metadata_body_max: metadata_body_default(),
            sync_body_max: sync_body_default(),
            repair_body_max: repair_body_default(),
            inconsistency_body_max: inconsistency_body_default(),
            sync_spool_limit: sync_limit_default(),
            repair_limit: repair_limit_default(),
            inconsistency_limit: inconsistency_limit_default(),
            public_slice_limit: public_slice_default(),
            public_metadata_limit: public_metadata_default(),
        }
    }
}

fn pin_ttl_default() -> u64 {
    90
}

fn pin_keys_default() -> usize {
    2
}

fn peer_id_default() -> bool {
    true
}

fn default_public_ingest() -> bool {
    true
}

fn slice_body_default() -> usize {
    10 * 1024 * 1024
}

fn metadata_body_default() -> usize {
    1024 * 1024
}

fn sync_body_default() -> usize {
    1024 * 1024
}

fn repair_body_default() -> usize {
    1024 * 1024
}

fn inconsistency_body_default() -> usize {
    1024 * 1024
}

fn sync_limit_default() -> Option<usize> {
    Some(64)
}

fn repair_limit_default() -> Option<usize> {
    Some(128)
}

fn inconsistency_limit_default() -> Option<usize> {
    Some(32)
}

fn public_slice_default() -> Option<usize> {
    None
}

fn public_metadata_default() -> Option<usize> {
    None
}
