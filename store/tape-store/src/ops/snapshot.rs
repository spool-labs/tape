//! Snapshot build-state operations.

use crate::columns::{SnapshotEpochCol, SnapshotGroupCol, SnapshotSliceCol};
use crate::error::{Result, TapeStoreError};
use crate::types::{EpochKey, SnapshotGroupKey, SnapshotSliceKey, SliceValue};
use crate::TapeStore;
use store::{Column, Store};
use tape_core::snapshot::info::{SnapshotEpochInfo, SnapshotGroupInfo};
use tape_core::spooler::{SpoolGroup, SpoolIndex};
use tape_core::types::EpochNumber;

pub trait SnapshotOps {
    fn get_epoch_info(&self, epoch: EpochNumber) -> Result<Option<SnapshotEpochInfo>>;
    fn put_epoch_info(&self, epoch: EpochNumber, info: SnapshotEpochInfo) -> Result<()>;
    fn get_group_info( &self, epoch: EpochNumber, group: SpoolGroup,) -> Result<Option<SnapshotGroupInfo>>;
    fn put_group_info(&self, epoch: EpochNumber, group: SpoolGroup, info: SnapshotGroupInfo) -> Result<()>;
    fn iter_groups_for_epoch(&self, epoch: EpochNumber) -> Result<Vec<(SpoolGroup, SnapshotGroupInfo)>>;
    fn get_group_slice( &self, epoch: EpochNumber, group: SpoolGroup, spool: SpoolIndex,) -> Result<Option<Vec<u8>>>;
    fn put_group_slice( &self, epoch: EpochNumber, group: SpoolGroup, spool: SpoolIndex, data: Vec<u8>,) -> Result<()>;
    fn delete_epoch_data(&self, epoch: EpochNumber) -> Result<()>;
}

impl<S: Store> SnapshotOps for TapeStore<S> {
    fn get_epoch_info(&self, epoch: EpochNumber) -> Result<Option<SnapshotEpochInfo>> {
        Ok(self.get::<SnapshotEpochCol>(&EpochKey(epoch.0))?)
    }

    fn put_epoch_info(&self, epoch: EpochNumber, info: SnapshotEpochInfo) -> Result<()> {
        self.put::<SnapshotEpochCol>(&EpochKey(epoch.0), &info)?;
        Ok(())
    }

    fn get_group_info(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
    ) -> Result<Option<SnapshotGroupInfo>> {
        let key = SnapshotGroupKey::new(epoch, group);
        Ok(self.get::<SnapshotGroupCol>(&key)?)
    }

    fn put_group_info(&self, epoch: EpochNumber, group: SpoolGroup, info: SnapshotGroupInfo) -> Result<()> {
        let key = SnapshotGroupKey::new(epoch, group);
        self.put::<SnapshotGroupCol>(&key, &info)?;
        Ok(())
    }

    fn iter_groups_for_epoch(&self, epoch: EpochNumber) -> Result<Vec<(SpoolGroup, SnapshotGroupInfo)>> {
        let prefix = SnapshotGroupKey::epoch_prefix(epoch);
        let iter = self
            .inner()
            .inner()
            .iter_prefix(SnapshotGroupCol::CF_NAME, &prefix)?;

        let mut groups = Vec::new();
        for (key_bytes, value_bytes) in iter {
            let key: SnapshotGroupKey = wincode::deserialize(&key_bytes)
                .map_err(|error| TapeStoreError::Serialization(format!(
                    "snapshot group key: {error}"
                )))?;
            let info: SnapshotGroupInfo = wincode::deserialize(&value_bytes)
                .map_err(|error| TapeStoreError::Serialization(format!(
                    "snapshot group value: {error}"
                )))?;
            groups.push((key.group, info));
        }

        Ok(groups)
    }

    fn get_group_slice(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
    ) -> Result<Option<Vec<u8>>> {
        let key = SnapshotSliceKey::new(epoch, group, spool);
        Ok(self.get::<SnapshotSliceCol>(&key)?.map(|value: SliceValue| value.0))
    }

    fn put_group_slice(
        &self,
        epoch: EpochNumber,
        group: SpoolGroup,
        spool: SpoolIndex,
        data: Vec<u8>,
    ) -> Result<()> {
        let key = SnapshotSliceKey::new(epoch, group, spool);
        self.put::<SnapshotSliceCol>(&key, &SliceValue(data))?;
        Ok(())
    }

    fn delete_epoch_data(&self, epoch: EpochNumber) -> Result<()> {
        let raw = self.inner().inner();

        let epoch_key = EpochKey(epoch.0);
        let epoch_key_bytes = wincode::serialize(&epoch_key)
            .map_err(|error| TapeStoreError::Serialization(format!("snapshot epoch key: {error}")))?;
        if raw.contains(SnapshotEpochCol::CF_NAME, &epoch_key_bytes)? {
            raw.delete(SnapshotEpochCol::CF_NAME, &epoch_key_bytes)?;
        }

        let group_prefix = SnapshotGroupKey::epoch_prefix(epoch);
        let group_keys: Vec<Vec<u8>> = raw
            .iter_prefix(SnapshotGroupCol::CF_NAME, &group_prefix)?
            .map(|(key, _)| key)
            .collect();
        for key in group_keys {
            raw.delete(SnapshotGroupCol::CF_NAME, &key)?;
        }

        let slice_prefix = SnapshotSliceKey::epoch_prefix(epoch);
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
        SnapshotEpochInfo, SnapshotEpochStatus, SnapshotGroupInfo, SnapshotGroupStatus,
    };
    use tape_core::spooler::SpoolGroup;
    use tape_core::track::blob::BlobInfo;
    use tape_core::types::{SnapshotGroupBitmap, StorageUnits, StripeCount, TrackNumber};
    use tape_crypto::Hash;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    fn epoch_info(epoch: EpochNumber) -> SnapshotEpochInfo {
        SnapshotEpochInfo {
            parent_epoch: epoch - EpochNumber(1),
            status: SnapshotEpochStatus::Initialized,
            certified_groups: SnapshotGroupBitmap::from_indices(&[0, 2], SPOOL_GROUP_COUNT),
        }
    }

    fn group_info(epoch: EpochNumber, group: SpoolGroup) -> SnapshotGroupInfo {
        let _ = (epoch, group);
        SnapshotGroupInfo {
            status: SnapshotGroupStatus::Built,
            blob: BlobInfo {
                size: StorageUnits::from_bytes(4_096),
                root: Hash::new_unique(),
                commitment: Hash::new_unique(),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(1024),
                stripe_count: StripeCount(4),
                leaves: [Hash::new_unique(); SPOOL_GROUP_SIZE],
            },
            track_number: Some(TrackNumber(7)),
        }
    }

    #[test]
    fn epoch_info_roundtrip() {
        let store = test_store();
        let info = epoch_info(EpochNumber(42));
        let epoch = EpochNumber(42);

        assert!(store.get_epoch_info(epoch).unwrap().is_none());
        store.put_epoch_info(epoch, info).unwrap();

        let recovered = store.get_epoch_info(epoch).unwrap().unwrap();
        assert_eq!(recovered, info);
    }

    #[test]
    fn group_info_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let group = SpoolGroup(3);
        let info = group_info(epoch, group);

        assert!(store.get_group_info(epoch, group).unwrap().is_none());
        store.put_group_info(epoch, group, info).unwrap();

        let recovered = store.get_group_info(epoch, group).unwrap().unwrap();
        assert_eq!(recovered, info);
    }

    #[test]
    fn iter_groups_are_epoch_ordered() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let group_2 = group_info(epoch, SpoolGroup(2));
        let group_0 = group_info(epoch, SpoolGroup(0));
        let group_1 = group_info(epoch, SpoolGroup(1));

        store.put_group_info(epoch, SpoolGroup(2), group_2).unwrap();
        store.put_group_info(epoch, SpoolGroup(0), group_0).unwrap();
        store.put_group_info(epoch, SpoolGroup(1), group_1).unwrap();

        let groups = store.iter_groups_for_epoch(epoch).unwrap();
        let ordered: Vec<SpoolGroup> = groups.into_iter().map(|(group, _)| group).collect();
        assert_eq!(ordered, vec![SpoolGroup(0), SpoolGroup(1), SpoolGroup(2)]);
    }

    #[test]
    fn group_slice_roundtrip() {
        let store = test_store();
        let epoch = EpochNumber(42);
        let group = SpoolGroup(3);
        let spool = 17;
        let data = vec![0xAB; 1024];

        assert!(store.get_group_slice(epoch, group, spool).unwrap().is_none());
        store
            .put_group_slice(epoch, group, spool, data.clone())
            .unwrap();

        let recovered = store.get_group_slice(epoch, group, spool).unwrap().unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn delete_epoch_data_is_isolated() {
        let store = test_store();

        let epoch_a = EpochNumber(41);
        let epoch_b = EpochNumber(42);

        store.put_epoch_info(epoch_a, epoch_a_info()).unwrap();
        store.put_epoch_info(epoch_b, epoch_b_info()).unwrap();
        store
            .put_group_info(epoch_a, SpoolGroup(0), group_info(epoch_a, SpoolGroup(0)))
            .unwrap();
        store
            .put_group_info(epoch_b, SpoolGroup(0), group_info(epoch_b, SpoolGroup(0)))
            .unwrap();
        store
            .put_group_slice(epoch_a, SpoolGroup(0), 1, vec![1, 2, 3])
            .unwrap();
        store
            .put_group_slice(epoch_b, SpoolGroup(0), 2, vec![4, 5, 6])
            .unwrap();

        store.delete_epoch_data(epoch_a).unwrap();

        assert!(store.get_epoch_info(epoch_a).unwrap().is_none());
        assert!(store.get_group_info(epoch_a, SpoolGroup(0)).unwrap().is_none());
        assert!(store.get_group_slice(epoch_a, SpoolGroup(0), 1).unwrap().is_none());

        assert!(store.get_epoch_info(epoch_b).unwrap().is_some());
        assert!(store.get_group_info(epoch_b, SpoolGroup(0)).unwrap().is_some());
        assert!(store.get_group_slice(epoch_b, SpoolGroup(0), 2).unwrap().is_some());
    }

    fn epoch_a_info() -> SnapshotEpochInfo {
        epoch_info(EpochNumber(41))
    }

    fn epoch_b_info() -> SnapshotEpochInfo {
        epoch_info(EpochNumber(42))
    }
}
