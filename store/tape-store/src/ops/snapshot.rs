//! Snapshot build-state operations.

use crate::columns::{SnapshotCol, SnapshotSliceCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochKey, SliceValue, SnapshotSliceKey};
use crate::TapeStore;
use store::{Column, Store};
use tape_core::snapshot::info::SnapshotInfo;
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::EpochNumber;

pub trait SnapshotOps {
    fn get_snapshot_info(&self, snapshot_epoch: EpochNumber) -> Result<Option<SnapshotInfo>>;
    fn put_snapshot_info(&self, snapshot_epoch: EpochNumber, info: SnapshotInfo) -> Result<()>;
    fn get_snapshot_slice(
        &self,
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
    ) -> Result<Option<Vec<u8>>>;
    fn put_snapshot_slice(
        &self,
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
        data: Vec<u8>,
    ) -> Result<()>;
    fn delete_snapshot(&self, snapshot_epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> SnapshotOps for TapeStore<S> {
    fn get_snapshot_info(&self, snapshot_epoch: EpochNumber) -> Result<Option<SnapshotInfo>> {
        Ok(self.get::<SnapshotCol>(&EpochKey(snapshot_epoch.0))?)
    }

    fn put_snapshot_info(&self, snapshot_epoch: EpochNumber, info: SnapshotInfo) -> Result<()> {
        self.put::<SnapshotCol>(&EpochKey(snapshot_epoch.0), &info)?;
        Ok(())
    }

    fn get_snapshot_slice(
        &self,
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
    ) -> Result<Option<Vec<u8>>> {
        let key = SnapshotSliceKey::new(snapshot_epoch, group, spool);
        Ok(self.get::<SnapshotSliceCol>(&key)?.map(|value: SliceValue| value.0))
    }

    fn put_snapshot_slice(
        &self,
        snapshot_epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = SnapshotSliceKey::new(snapshot_epoch, group, spool);
        self.put::<SnapshotSliceCol>(&key, &SliceValue(data))?;
        Ok(())
    }

    fn delete_snapshot(&self, snapshot_epoch: EpochNumber) -> Result<()> {
        let raw = self.inner().inner();

        let snapshot_key = EpochKey(snapshot_epoch.0);
        let snapshot_key_bytes = wincode::serialize(&snapshot_key)
            .map_err(|error| TapeStoreError::Serialization(format!("snapshot key: {error}")))?;
        if raw.contains(SnapshotCol::CF_NAME, &snapshot_key_bytes)? {
            raw.delete(SnapshotCol::CF_NAME, &snapshot_key_bytes)?;
        }

        let slice_prefix = SnapshotSliceKey::epoch_prefix(snapshot_epoch);
        let slice_keys: Vec<Vec<u8>> = raw
            .iter_prefix(SnapshotSliceCol::CF_NAME, &slice_prefix)?
            .map(|(key, _)| key)
            .collect();
        for key in slice_keys {
            raw.delete(SnapshotSliceCol::CF_NAME, &key)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::{SPOOL_GROUP_COUNT, SPOOL_GROUP_SIZE};
    use tape_core::snapshot::info::{
        SnapshotGroupInfo, SnapshotGroupStatus, SnapshotInfo, SnapshotStatus,
    };
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{SnapshotGroupBitmap, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn group_info() -> SnapshotGroupInfo {
        SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            blob: BlobInfo {
                size: StorageUnits::from_bytes(4_096),
                commitment: Hash::new_unique(),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(1024),
                stripe_count: StripeCount(4),
                leaves: [Hash::new_unique(); SPOOL_GROUP_SIZE],
            },
            track_number: Some(TrackNumber(7)),
        }
    }

    fn snapshot_info() -> SnapshotInfo {
        let mut snapshot = SnapshotInfo::new(SnapshotStatus::Initialized);
        snapshot.certified_groups =
            SnapshotGroupBitmap::from_indices(&[0, 2], SPOOL_GROUP_COUNT);
        *snapshot.group_mut(SpoolGroup(2)) = group_info();
        snapshot
    }

    #[test]
    fn snapshot_info_roundtrip() {
        let store = test_store();
        let info = snapshot_info();
        let epoch = EpochNumber(42);

        assert!(store.get_snapshot_info(epoch).unwrap().is_none());
        store.put_snapshot_info(epoch, info.clone()).unwrap();

        let recovered = store.get_snapshot_info(epoch).unwrap().unwrap();
        assert_eq!(recovered, info);
    }

    #[test]
    fn snapshot_slice_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let group = SpoolGroup(3);
        let spool = 17;
        let data = vec![0xAB; 1024];

        assert!(store.get_snapshot_slice(epoch, group, spool).unwrap().is_none());
        store
            .put_snapshot_slice(epoch, group, spool, data.clone())
            .unwrap();

        let recovered = store.get_snapshot_slice(epoch, group, spool).unwrap().unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn delete_snapshot_is_isolated() {
        let store = test_store();

        let epoch_a = EpochNumber(41);
        let epoch_b = EpochNumber(42);

        store.put_snapshot_info(epoch_a, snapshot_info()).unwrap();
        store.put_snapshot_info(epoch_b, snapshot_info()).unwrap();
        store
            .put_snapshot_slice(epoch_a, SpoolGroup(0), 1, vec![1, 2, 3])
            .unwrap();
        store
            .put_snapshot_slice(epoch_b, SpoolGroup(0), 2, vec![4, 5, 6])
            .unwrap();

        store.delete_snapshot(epoch_a).unwrap();

        assert!(store.get_snapshot_info(epoch_a).unwrap().is_none());
        assert!(store.get_snapshot_slice(epoch_a, SpoolGroup(0), 1).unwrap().is_none());

        assert!(store.get_snapshot_info(epoch_b).unwrap().is_some());
        assert!(store.get_snapshot_slice(epoch_b, SpoolGroup(0), 2).unwrap().is_some());
    }
}
