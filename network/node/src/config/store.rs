use std::path::PathBuf;

use serde::Deserialize;

use super::helpers::{deserialize_option_pathbuf, deserialize_pathbuf};

/// Local RocksDB store settings.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct StoreConfig {
    /// Filesystem path for the store root. Holds the metadata store in a meta
    /// subdirectory, and the bulk store in a bulk subdirectory unless a separate
    /// bulk path is set.
    #[serde(default = "default_store_path", deserialize_with = "deserialize_pathbuf")]
    pub path: PathBuf,

    /// Optional separate root for the bulk store, for a large secondary device
    #[serde(default, deserialize_with = "deserialize_option_pathbuf")]
    pub bulk_path: Option<PathBuf>,

    /// Global RocksDB compaction rate limit in MB/s.
    #[serde(default = "default_compaction_mb_per_sec")]
    pub compaction_mb_per_sec: u64,

    /// Reject new uploads when the metadata tier has fewer free bytes than this.
    /// 0 disables the check.
    #[serde(default)]
    pub min_free_bytes: u64,

    /// Reject new uploads when the bulk tier has fewer free bytes than this.
    /// 0 disables the check.
    #[serde(default)]
    pub bulk_min_free_bytes: u64,

    /// Local garbage-collection settings.
    #[serde(default)]
    pub gc: GcConfig,
}

impl StoreConfig {
    /// Directory of the metadata (fast tier) database
    pub fn meta_dir(&self) -> PathBuf {
        self.path.join(tape_store::config::META_SUBDIR)
    }

    /// Directory of the bulk (large tier) database, under the bulk path if set
    pub fn bulk_dir(&self) -> PathBuf {
        self.bulk_path
            .as_ref()
            .unwrap_or(&self.path)
            .join(tape_store::config::BULK_SUBDIR)
    }
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            path: default_store_path(),
            bulk_path: None,
            compaction_mb_per_sec: default_compaction_mb_per_sec(),
            min_free_bytes: 0,
            bulk_min_free_bytes: 0,
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

fn default_reclaim_min_deleted_slices() -> usize {
    20
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_device_layout() {
        let config = StoreConfig {
            path: PathBuf::from("/mnt/nvme/tape"),
            bulk_path: None,
            ..StoreConfig::default()
        };
        // Both tiers live under the store root in separate subdirectories.
        assert_eq!(config.meta_dir(), PathBuf::from("/mnt/nvme/tape/meta"));
        assert_eq!(config.bulk_dir(), PathBuf::from("/mnt/nvme/tape/bulk"));
    }

    #[test]
    fn separate_bulk_device_layout() {
        let config = StoreConfig {
            path: PathBuf::from("/mnt/nvme/tape"),
            bulk_path: Some(PathBuf::from("/mnt/hdd/tape")),
            ..StoreConfig::default()
        };
        assert_eq!(config.meta_dir(), PathBuf::from("/mnt/nvme/tape/meta"));
        assert_eq!(config.bulk_dir(), PathBuf::from("/mnt/hdd/tape/bulk"));
    }

    #[test]
    fn bulk_path_defaults_to_none() {
        let config: StoreConfig = serde_yaml::from_str("path: /data/tape").unwrap();
        assert_eq!(config.path, PathBuf::from("/data/tape"));
        assert_eq!(config.bulk_path, None);
        assert_eq!(config.bulk_dir(), PathBuf::from("/data/tape/bulk"));
    }
}
