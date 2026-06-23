//! ObjectInfo operations for tracked object metadata

use store::Store;
use tape_crypto::address::Address;

use crate::columns::ObjectInfoCol;
use crate::error::Result;
use crate::types::ObjectInfo;
use crate::TapeStore;

/// Operations for object info
pub trait ObjectInfoOps {
    /// Get object info by address
    fn get_object_info(&self, address: Address) -> Result<Option<ObjectInfo>>;

    /// Store object info
    fn put_object_info(&self, address: Address, info: ObjectInfo) -> Result<()>;

    /// Delete object info
    fn delete_object_info(&self, address: Address) -> Result<()>;

    /// Check if object info exists
    fn has_object_info(&self, address: Address) -> Result<bool>;
}

impl<S: Store> ObjectInfoOps for TapeStore<S> {
    fn get_object_info(&self, address: Address) -> Result<Option<ObjectInfo>> {
        Ok(self.get::<ObjectInfoCol>(&address)?)
    }

    fn put_object_info(&self, address: Address, info: ObjectInfo) -> Result<()> {
        self.put::<ObjectInfoCol>(&address, &info)?;
        Ok(())
    }

    fn delete_object_info(&self, address: Address) -> Result<()> {
        self.delete::<ObjectInfoCol>(&address)?;
        Ok(())
    }

    fn has_object_info(&self, address: Address) -> Result<bool> {
        Ok(self.contains::<ObjectInfoCol>(&address)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SystemObjectKind;
    use tape_core::types::{EpochNumber, SlotNumber};
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_object_info_blacklisted() {
        let store = test_store();
        let addr = Address::new_unique();

        store
            .put_object_info(addr, ObjectInfo::Blacklisted)
            .unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
        assert_eq!(retrieved, ObjectInfo::Blacklisted);
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

        store.put_object_info(addr, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
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

        store.put_object_info(addr, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
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

        store.put_object_info(addr, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_object_info_has_and_delete() {
        let store = test_store();
        let addr = Address::new_unique();

        assert!(!store.has_object_info(addr).unwrap());

        store
            .put_object_info(addr, ObjectInfo::Blacklisted)
            .unwrap();
        assert!(store.has_object_info(addr).unwrap());

        store.delete_object_info(addr).unwrap();
        assert!(!store.has_object_info(addr).unwrap());
        assert!(store.get_object_info(addr).unwrap().is_none());
    }
}
