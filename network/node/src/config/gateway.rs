use serde::Deserialize;

/// Gateway-only runtime settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GatewayConfig {
    /// Slice cache settings for the public read gateway.
    #[serde(default)]
    pub cache: GatewayCacheConfig,

    /// Public gateway request/byte metering.
    #[serde(default)]
    pub metering: GatewayMeteringConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            cache: GatewayCacheConfig::default(),
            metering: GatewayMeteringConfig::default(),
        }
    }
}

/// Gateway slice-cache controls.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GatewayCacheConfig {
    /// Maximum raw slice payload bytes the gateway keeps on disk.
    ///
    /// A value of 0 disables persistent slice caching.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,

    /// Maximum entries deleted in one eviction pass.
    #[serde(default = "default_eviction_batch")]
    pub eviction_batch: usize,

    /// Trigger best-effort backend reclaim after this many evicted slices.
    ///
    /// A value of 0 disables explicit reclaim triggers.
    #[serde(default = "default_reclaim_after_deleted_slices")]
    pub reclaim_after_deleted_slices: usize,
}

impl Default for GatewayCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: default_max_bytes(),
            eviction_batch: default_eviction_batch(),
            reclaim_after_deleted_slices: default_reclaim_after_deleted_slices(),
        }
    }
}

fn default_max_bytes() -> u64 {
    64 * 1024 * 1024 * 1024
}

fn default_eviction_batch() -> usize {
    256
}

fn default_reclaim_after_deleted_slices() -> usize {
    1024
}

/// Gateway public-route metering controls.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GatewayMeteringConfig {
    /// Decoded-object request refill rate, per source IP.
    #[serde(default = "default_object_read_per_sec")]
    pub object_read_per_sec: u32,

    /// Decoded-object request burst, per source IP.
    #[serde(default = "default_object_read_burst")]
    pub object_read_burst: u32,

    /// Decoded-object byte refill rate, per source IP.
    #[serde(default = "default_object_read_bytes_per_sec")]
    pub object_read_bytes_per_sec: u64,

    /// Decoded-object byte burst, per source IP.
    #[serde(default = "default_object_read_byte_burst")]
    pub object_read_byte_burst: u64,

    /// Short block window after a caller exceeds its bucket.
    #[serde(default = "default_over_budget_penalty_secs")]
    pub over_budget_penalty_secs: u64,

    /// Remove idle meter entries after this many seconds.
    #[serde(default = "default_stale_entry_secs")]
    pub stale_entry_secs: u64,
}

impl Default for GatewayMeteringConfig {
    fn default() -> Self {
        Self {
            object_read_per_sec: default_object_read_per_sec(),
            object_read_burst: default_object_read_burst(),
            object_read_bytes_per_sec: default_object_read_bytes_per_sec(),
            object_read_byte_burst: default_object_read_byte_burst(),
            over_budget_penalty_secs: default_over_budget_penalty_secs(),
            stale_entry_secs: default_stale_entry_secs(),
        }
    }
}

fn default_object_read_per_sec() -> u32 {
    10
}

fn default_object_read_burst() -> u32 {
    50
}

fn default_object_read_bytes_per_sec() -> u64 {
    64 * 1024 * 1024
}

fn default_object_read_byte_burst() -> u64 {
    128 * 1024 * 1024
}

fn default_over_budget_penalty_secs() -> u64 {
    5
}

fn default_stale_entry_secs() -> u64 {
    300
}
