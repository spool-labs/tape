//! ObjectInfo operations for tracked object metadata

use crate::columns::ObjectInfoCol;
use crate::error::Result;
use crate::types::{ObjectInfo, Pubkey};
use crate::TapeStore;
use store::Store;

/// Operations for object info
pub trait ObjectInfoOps {
    /// Get object info by address
    fn get_object_info(&self, address: Pubkey) -> Result<Option<ObjectInfo>>;

    /// Store object info
    fn put_object_info(&self, address: Pubkey, info: ObjectInfo) -> Result<()>;

    /// Delete object info
    fn delete_object_info(&self, address: Pubkey) -> Result<()>;

    /// Check if object info exists
    fn has_object_info(&self, address: Pubkey) -> Result<bool>;
}

impl<S: Store> ObjectInfoOps for TapeStore<S> {
    fn get_object_info(&self, address: Pubkey) -> Result<Option<ObjectInfo>> {
        Ok(self.get::<ObjectInfoCol>(&address)?)
    }

    fn put_object_info(&self, address: Pubkey, info: ObjectInfo) -> Result<()> {
        self.put::<ObjectInfoCol>(&address, &info)?;
        Ok(())
    }

    fn delete_object_info(&self, address: Pubkey) -> Result<()> {
        self.delete::<ObjectInfoCol>(&address)?;
        Ok(())
    }

    fn has_object_info(&self, address: Pubkey) -> Result<bool> {
        Ok(self.contains::<ObjectInfoCol>(&address)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EpochNumber, SlotNumber};
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_object_info_blacklisted() {
        let store = test_store();
        let addr = Pubkey::new_unique();

        store
            .put_object_info(addr, ObjectInfo::Blacklisted)
            .unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
        assert_eq!(retrieved, ObjectInfo::Blacklisted);
    }

    #[test]
    fn test_object_info_valid() {
        let store = test_store();
        let addr = Pubkey::new_unique();

        let info = ObjectInfo::Valid {
            is_stored: true,
            track_address: Pubkey::new_unique(),
            registered_epoch: EpochNumber(5),
            certified_epoch: Some(EpochNumber(6)),
            slot: SlotNumber(50),
        };

        store.put_object_info(addr, info.clone()).unwrap();
        let retrieved = store.get_object_info(addr).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_object_info_invalid() {
        let store = test_store();
        let addr = Pubkey::new_unique();

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
        let addr = Pubkey::new_unique();

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
