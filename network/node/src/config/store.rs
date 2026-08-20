use std::path::PathBuf;

use reel::IoBackend;
use reel_store::{default_backend, Reserve, DEFAULT_SYNC_BYTES};
use serde::Deserialize;

use super::helpers::deserialize_pathbuf;

/// Local store settings.
///
/// A node built with the `rocks` feature serves the same root on RocksDB
/// instead, in a meta and a bulk subdirectory. It reads the path, the compaction
/// ceiling and the free-space floor, and ignores the rest, which name knobs only
/// the reel has.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StoreConfig {
    /// Filesystem path for the store root. One reel volume holds every column
    /// family, so this is the only store path a node takes.
    #[serde(default = "default_store_path", deserialize_with = "deserialize_pathbuf")]
    pub path: PathBuf,

    /// Compaction rate limit in MB/s. 0 lets the engine pace itself.
    #[serde(default)]
    pub compaction_mb_per_sec: u64,

    /// Bytes written between durability syncs. 0 syncs on every put, which costs
    /// far more latency than it is worth.
    #[serde(default = "default_sync_bytes")]
    pub sync_bytes: u64,

    /// File backend the volume opens with. A ring falls back to posix with a
    /// warning where the kernel cannot give one.
    #[serde(default = "default_backend")]
    pub io_backend: IoBackend,

    /// Reject new uploads when the store volume has fewer free bytes than this.
    /// 0 disables the check.
    #[serde(default)]
    pub min_free_bytes: u64,

    /// What a fresh volume reserves before it holds a byte. `fleet` pre-writes a
    /// gibibyte segment per tail; `small` sizes the reservation to the run, which
    /// is what a laptop wants and what a local fleet of twenty needs.
    #[serde(default)]
    pub reserve: Reserve,

    /// Local garbage-collection settings.
    #[serde(default)]
    pub gc: GcConfig,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: default_store_path(),
            compaction_mb_per_sec: 0,
            sync_bytes: default_sync_bytes(),
            io_backend: default_backend(),
            min_free_bytes: 0,
            reserve: Reserve::default(),
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
    /// Minimum deleted slices in a sweep before reclaim is triggered.
    #[serde(default = "default_reclaim_min_deleted_slices")]
    pub reclaim_min_deleted_slices: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: default_gc_enabled(),
            interval_secs: default_interval_secs(),
            track_batch: default_track_batch(),
            slice_batch: default_slice_batch(),
            reclaim_min_deleted_slices: default_reclaim_min_deleted_slices(),
        }
    }
}

fn default_store_path() -> PathBuf {
    super::helpers::expand_path("~/.tape/data")
}

fn default_sync_bytes() -> u64 {
    DEFAULT_SYNC_BYTES
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

fn default_reclaim_min_deleted_slices() -> usize {
    20
}

#[cfg(test)]
mod tests {
    use super::*;

    // unspecified keys fall back to the shipped fleet defaults
    #[test]
    fn yaml_defaults() {
        let config: StoreConfig = serde_yaml::from_str("path: /data/tape").unwrap();
        assert_eq!(config.path, PathBuf::from("/data/tape"));
        assert_eq!(config.compaction_mb_per_sec, 0);
        assert_eq!(config.sync_bytes, 16 * 1024 * 1024);
        assert_eq!(config.io_backend, default_backend());
        assert_eq!(config.reserve, Reserve::Fleet);
    }

    // a volume that cannot afford the shipped reservation says so in one word
    #[test]
    fn yaml_reserve() {
        let config: StoreConfig =
            serde_yaml::from_str("path: /data/tape\nreserve: small").unwrap();
        assert_eq!(config.reserve, Reserve::Small);
    }

    // an operator naming a backend gets that backend
    #[test]
    fn yaml_backend() {
        let config: StoreConfig =
            serde_yaml::from_str("path: /data/tape\nio_backend: posix").unwrap();
        assert_eq!(config.io_backend, IoBackend::Posix);
    }

    // the ring is what a linux node opens with, since the fallback is automatic
    #[cfg(target_os = "linux")]
    #[test]
    fn linux_ring() {
        assert_eq!(StoreConfig::default().io_backend, IoBackend::Uring);
    }
}
