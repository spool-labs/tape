//! ObjectInfo operations for tracked object metadata

use tape_store_api::Store;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;

use crate::columns::ObjectInfoCol;
use crate::error::Result;
use crate::types::{ObjectInfo, TrackKey};
use crate::TapeStore;

/// Operations for object info
pub trait ObjectInfoOps {
    /// Get object info by track address and track number
    fn get_object_info(
        &self,
        address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<ObjectInfo>>;

    /// Get object info for several tracks, answered in the order asked
    fn get_object_infos(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<ObjectInfo>>>;

    /// Store object info
    fn put_object_info(
        &self,
        address: Address,
        track_number: TrackNumber,
        info: ObjectInfo,
    ) -> Result<()>;

    /// Delete object info
    fn delete_object_info(&self, address: Address, track_number: TrackNumber) -> Result<()>;

    /// Check if object info exists
    fn has_object_info(&self, address: Address, track_number: TrackNumber) -> Result<bool>;
}

impl<S: Store> ObjectInfoOps for TapeStore<S> {
    fn get_object_info(
        &self,
        address: Address,
        track_number: TrackNumber,
    ) -> Result<Option<ObjectInfo>> {
        Ok(self.get::<ObjectInfoCol>(&TrackKey::new(address, track_number))?)
    }

    fn get_object_infos(&self, tracks: &[(Address, TrackNumber)]) -> Result<Vec<Option<ObjectInfo>>> {
        let keys: Vec<TrackKey> = tracks
            .iter()
            .map(|(address, track_number)| TrackKey::new(*address, *track_number))
            .collect();
        Ok(self.get_many::<ObjectInfoCol>(&keys)?)
    }

    fn put_object_info(
        &self,
        address: Address,
        track_number: TrackNumber,
        info: ObjectInfo,
    ) -> Result<()> {
        self.put::<ObjectInfoCol>(&TrackKey::new(address, track_number), &info)?;
        Ok(())
    }

    fn delete_object_info(&self, address: Address, track_number: TrackNumber) -> Result<()> {
        self.delete::<ObjectInfoCol>(&TrackKey::new(address, track_number))?;
        Ok(())
    }

    fn has_object_info(&self, address: Address, track_number: TrackNumber) -> Result<bool> {
        Ok(self.contains::<ObjectInfoCol>(&TrackKey::new(address, track_number))?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SystemObjectKind;
    use tape_core::types::{EpochNumber, SlotNumber};
    use tape_store_memory::MemoryStore;

    const NUMBER: TrackNumber = TrackNumber(3);

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_object_info_blacklisted() {
        let store = test_store();
        let addr = Address::new_unique();

        store
            .put_object_info(addr, NUMBER, ObjectInfo::Blacklisted)
            .unwrap();
        let retrieved = store.get_object_info(addr, NUMBER).unwrap().unwrap();
        assert_eq!(retrieved, ObjectInfo::Blacklisted);
        assert_eq!(store.get_object_info(addr, TrackNumber(4)).unwrap(), None);
    }

    #[test]
    fn test_object_info_valid() {
        let store = test_store();
        let addr = Address::new_unique();

        let info = ObjectInfo::Valid {
            track_address: Address::new_unique(),
            registered_epoch: EpochNumber(5),
            certified_epoch: Some(EpochNumber(6)),
            slot: SlotNumber(50),
        };

        store.put_object_info(addr, NUMBER, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr, NUMBER).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_object_info_snapshot() {
        let store = test_store();
        let addr = Address::new_unique();

        let info = ObjectInfo::System {
            kind: SystemObjectKind::Snapshot {
                epoch: EpochNumber(7),
            },
            track_address: addr,
            registered_epoch: EpochNumber(7),
            certified_epoch: None,
            slot: SlotNumber(70),
        };

        store.put_object_info(addr, NUMBER, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr, NUMBER).unwrap().unwrap();
        assert_eq!(retrieved, info);
        assert!(retrieved.is_certified());
        assert!(retrieved.is_live());
    }

    #[test]
    fn test_object_info_invalid() {
        let store = test_store();
        let addr = Address::new_unique();

        let info = ObjectInfo::Invalid {
            epoch: EpochNumber(10),
            slot: SlotNumber(100),
        };

        store.put_object_info(addr, NUMBER, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr, NUMBER).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_object_info_has_and_delete() {
        let store = test_store();
        let addr = Address::new_unique();

        assert!(!store.has_object_info(addr, NUMBER).unwrap());

        store
            .put_object_info(addr, NUMBER, ObjectInfo::Blacklisted)
            .unwrap();
        assert!(store.has_object_info(addr, NUMBER).unwrap());

        store.delete_object_info(addr, NUMBER).unwrap();
        assert!(!store.has_object_info(addr, NUMBER).unwrap());
        assert!(store.get_object_info(addr, NUMBER).unwrap().is_none());
    }
}
