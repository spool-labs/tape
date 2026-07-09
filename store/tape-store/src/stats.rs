//! Store size and composition stats.
//!
//! Reads the cheap RocksDB introspection surface — per-column-family on-disk
//! size and estimated key counts, per-volume disk usage — and derives the
//! metadata-versus-slice-data split and the metadata overhead the store CLI
//! reports. Every figure is a cheap property read, safe to call on a live
//! store; nothing here scans keys or values.

use serde::{Deserialize, Serialize};

use store::{Column, DiskVolume, Result, Store, StoreVolume};

use crate::columns::{ObjectInfoCol, SliceCol, TapeCol, TrackCol};
use crate::config::BULK_COLUMN_FAMILIES;

/// Which class of data a column family holds, for the metadata/payload split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnClass {
    /// Small index or metadata column; the store's overhead over raw payload.
    Metadata,
    /// Erasure-coded slice payload.
    Slice,
    /// Other bulk payload such as local track data and staged snapshot artifacts.
    Bulk,
}

/// Physical volume a column family lives on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Volume {
    /// The metadata volume, or the whole store when not split.
    Primary,
    /// The bulk volume for large payloads.
    Bulk,
}

impl From<StoreVolume> for Volume {
    fn from(volume: StoreVolume) -> Self {
        match volume {
            StoreVolume::Primary => Volume::Primary,
            StoreVolume::Bulk => Volume::Bulk,
        }
    }
}

/// On-disk usage for one column family, tagged with its data class and volume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column family name.
    pub cf: String,
    /// Whether the column holds metadata, slice payload, or other bulk data.
    pub class: ColumnClass,
    /// Physical volume the column lives on.
    pub volume: Volume,
    /// Bytes held in SST files.
    pub sst_bytes: u64,
    /// Bytes held in blob files, zero for columns that store values inline.
    pub blob_bytes: u64,
    /// Estimated live key count.
    pub num_keys: u64,
}

impl ColumnStats {
    /// SST plus blob bytes.
    pub fn total_bytes(&self) -> u64 {
        self.sst_bytes.saturating_add(self.blob_bytes)
    }
}

/// One physical volume's footprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeStats {
    pub volume: Volume,
    pub used_bytes: u64,
    pub free_bytes: Option<u64>,
}

impl From<DiskVolume> for VolumeStats {
    fn from(volume: DiskVolume) -> Self {
        Self {
            volume: volume.volume.into(),
            used_bytes: volume.used_bytes,
            free_bytes: volume.free_bytes,
        }
    }
}

/// Cheap key-count estimates for the headline entities.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCounts {
    pub tapes: u64,
    pub tracks: u64,
    pub objects: u64,
    pub slices: u64,
}

/// A composition breakdown of a store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreStats {
    /// Per physical volume footprint; one entry unless the store is split.
    pub volumes: Vec<VolumeStats>,
    /// Per column family usage, largest first.
    pub columns: Vec<ColumnStats>,
    /// Key-count estimates for tapes, tracks, objects, and slices.
    pub counts: StoreCounts,
    /// On-disk bytes across all metadata column families.
    pub metadata_bytes: u64,
    /// On-disk bytes of the slice payload column family.
    pub slice_bytes: u64,
    /// On-disk bytes of non-slice bulk column families.
    pub other_bulk_bytes: u64,
    /// Sum of per-volume used bytes, including engine overhead the per-column
    /// figures exclude.
    pub disk_used_bytes: u64,
    /// Tightest free space across volumes, if any volume reports it.
    pub disk_free_bytes: Option<u64>,
}

impl StoreStats {
    /// Sum of per-column-family bytes.
    pub fn column_total_bytes(&self) -> u64 {
        self.metadata_bytes
            .saturating_add(self.slice_bytes)
            .saturating_add(self.other_bulk_bytes)
    }

    /// Fraction of column-family bytes that is metadata, zero when empty.
    pub fn metadata_fraction(&self) -> f64 {
        ratio(self.metadata_bytes, self.column_total_bytes())
    }

    /// Fraction of column-family bytes that is slice payload.
    pub fn slice_fraction(&self) -> f64 {
        ratio(self.slice_bytes, self.column_total_bytes())
    }

    /// Metadata bytes carried per byte of slice payload, zero when no slice
    /// payload is stored yet.
    pub fn metadata_overhead(&self) -> f64 {
        ratio(self.metadata_bytes, self.slice_bytes)
    }
}

/// The data class a column family belongs to.
pub fn classify(cf: &str) -> ColumnClass {
    if cf == SliceCol::CF_NAME {
        ColumnClass::Slice
    } else if BULK_COLUMN_FAMILIES.contains(&cf) {
        ColumnClass::Bulk
    } else {
        ColumnClass::Metadata
    }
}

/// Collect a composition breakdown from any store backend.
///
/// Backends that cannot introspect cheaply report zeros; a split backend tags
/// each column family with its owning volume.
pub fn collect(store: &impl Store) -> Result<StoreStats> {
    let mut columns: Vec<ColumnStats> = store
        .cf_disk_usage()?
        .into_iter()
        .map(|usage| ColumnStats {
            class: classify(&usage.cf),
            cf: usage.cf,
            volume: usage.volume.into(),
            sst_bytes: usage.sst_bytes,
            blob_bytes: usage.blob_bytes,
            num_keys: usage.num_keys,
        })
        .collect();
    columns.sort_by(|a, b| b.total_bytes().cmp(&a.total_bytes()).then(a.cf.cmp(&b.cf)));

    // The byte split and headline counts both come from the columns just built,
    // so no column family is probed twice.
    let class_bytes = |class: ColumnClass| -> u64 {
        columns
            .iter()
            .filter(|column| column.class == class)
            .map(ColumnStats::total_bytes)
            .sum()
    };
    let count = |name: &str| -> u64 {
        columns.iter().find(|column| column.cf == name).map_or(0, |column| column.num_keys)
    };

    let metadata_bytes = class_bytes(ColumnClass::Metadata);
    let slice_bytes = class_bytes(ColumnClass::Slice);
    let other_bulk_bytes = class_bytes(ColumnClass::Bulk);
    let counts = StoreCounts {
        tapes: count(TapeCol::CF_NAME),
        tracks: count(TrackCol::CF_NAME),
        objects: count(ObjectInfoCol::CF_NAME),
        slices: count(SliceCol::CF_NAME),
    };

    let volumes: Vec<VolumeStats> = store
        .disk_volumes()?
        .into_iter()
        .map(VolumeStats::from)
        .collect();
    let disk_used_bytes = volumes.iter().map(|v| v.used_bytes).sum();
    let disk_free_bytes = volumes.iter().filter_map(|v| v.free_bytes).min();

    Ok(StoreStats {
        volumes,
        columns,
        counts,
        metadata_bytes,
        slice_bytes,
        other_bulk_bytes,
        disk_used_bytes,
        disk_free_bytes,
    })
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::{SliceOps, TrackOps};
    use crate::TapeStore;
    use tape_core::track::types::{CompressedTrack, TrackKind, TrackState};
    use tape_core::types::{GroupIndex, SpoolIndex, StorageUnits, TrackNumber};
    use tape_crypto::address::Address;
    use tape_crypto::Hash;
    use tempfile::TempDir;

    // slice payload above the 256 KiB BlobDB threshold, so it lands in blob files
    const SLICE_SIZE: usize = 512 * 1024;

    fn certified_track(tape: Address) -> CompressedTrack {
        CompressedTrack {
            tape,
            key: Hash::new_unique(),
            track_number: TrackNumber(0),
            kind: TrackKind::Coded as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(SLICE_SIZE as u64),
            group: GroupIndex(0),
            value_hash: Hash::new_unique(),
        }
    }

    #[test]
    fn classify_by_column_family() {
        assert_eq!(classify(SliceCol::CF_NAME), ColumnClass::Slice);
        assert_eq!(classify("track_data"), ColumnClass::Bulk);
        assert_eq!(classify("snapshot_artifact"), ColumnClass::Bulk);
        assert_eq!(classify(TrackCol::CF_NAME), ColumnClass::Metadata);
        assert_eq!(classify(TapeCol::CF_NAME), ColumnClass::Metadata);
    }

    // a split store attributes slice payload to the bulk volume and tracks to
    // metadata, and reports both physical volumes
    #[test]
    fn collect_splits_metadata_from_slices() {
        let dir = TempDir::new().unwrap();
        let store = TapeStore::open_primary(dir.path().join("db")).unwrap();

        let spool = SpoolIndex(0);
        for _ in 0..4 {
            let address = Address::new_unique();
            store.put_track(address, certified_track(Address::new_unique())).unwrap();
            store.put_slice(spool, address, vec![7u8; SLICE_SIZE]).unwrap();
        }
        store.inner().inner().flush().unwrap();

        let stats = collect(store.inner().inner()).unwrap();

        // Two volumes for a split store.
        assert_eq!(stats.volumes.len(), 2);

        // The slice column family is bulk payload; the track column is metadata.
        let slice = stats.columns.iter().find(|c| c.cf == SliceCol::CF_NAME).unwrap();
        assert_eq!(slice.class, ColumnClass::Slice);
        assert_eq!(slice.volume, Volume::Bulk);
        assert!(slice.blob_bytes > 0, "slice payload should occupy blob files");

        let track = stats.columns.iter().find(|c| c.cf == TrackCol::CF_NAME).unwrap();
        assert_eq!(track.class, ColumnClass::Metadata);
        assert_eq!(track.volume, Volume::Primary);

        // Counts come from the same column scan, not a second probe.
        assert_eq!(stats.counts.slices, 4);
        assert_eq!(stats.counts.tracks, 4);

        assert!(stats.slice_bytes > 0);
        assert!(stats.slice_bytes > stats.metadata_bytes);
        assert!(stats.metadata_overhead() > 0.0);
    }
}
