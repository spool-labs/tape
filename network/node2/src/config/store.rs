use std::path::PathBuf;

use serde::Deserialize;

use super::helpers::deserialize_pathbuf;

/// Local RocksDB store settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StoreConfig {
    /// Filesystem path for the primary RocksDB database.
    #[serde(default = "default_store_path", deserialize_with = "deserialize_pathbuf")]
    pub path: PathBuf,

    /// Global RocksDB compaction rate limit in MB/s.
    #[serde(default = "default_compaction_mb_per_sec")]
    pub compaction_mb_per_sec: u64,

    /// Local garbage-collection settings.
    #[serde(default)]
    pub gc: GcConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: default_store_path(),
            compaction_mb_per_sec: default_compaction_mb_per_sec(),
            gc: GcConfig::default(),
        }
    }
}

/// Garbage-collection settings for local store cleanup.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct GcConfig {
    /// Whether the background GC worker is enabled.
    #[serde(default = "default_gc_enabled")]
    pub enabled: bool,
    /// Interval between GC passes in seconds.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Track-iteration batch size per sweep.
    #[serde(default = "default_track_batch")]
    pub track_batch: usize,
    /// Slice-iteration batch size per sweep.
    #[serde(default = "default_slice_batch")]
    pub slice_batch: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: default_gc_enabled(),
            interval_secs: default_interval_secs(),
            track_batch: default_track_batch(),
            slice_batch: default_slice_batch(),
        }
    }
}

fn default_store_path() -> PathBuf {
    super::helpers::expand_path("~/.tape/data")
}

fn default_compaction_mb_per_sec() -> u64 {
    100
}

fn default_gc_enabled() -> bool {
    true
}

fn default_interval_secs() -> u64 {
    60
}

fn default_track_batch() -> usize {
    256
}

fn default_slice_batch() -> usize {
    256
}
