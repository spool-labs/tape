//! TapeInfo operations for tape metadata

use crate::columns::TapeCol;
use crate::error::Result;
use crate::types::{Pubkey, TapeInfo};
use crate::TapeStore;
use store::Store;

/// Operations for tape info
pub trait TapeOps {
    /// Get tape info by address
    fn get_tape(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>>;

    /// Store tape info
    fn put_tape(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()>;

    /// Delete tape info
    fn delete_tape(&self, tape_address: Pubkey) -> Result<()>;
}

impl<S: Store> TapeOps for TapeStore<S> {
    fn get_tape(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>> {
        Ok(self.get::<TapeCol>(&tape_address)?)
    }

    fn put_tape(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()> {
        self.put::<TapeCol>(&tape_address, &info)?;
        Ok(())
    }

    fn delete_tape(&self, tape_address: Pubkey) -> Result<()> {
        self.delete::<TapeCol>(&tape_address)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EpochNumber;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_tape_roundtrip() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            end_epoch: EpochNumber(200),
        };

        assert!(store.get_tape(tape).unwrap().is_none());

        store.put_tape(tape, info.clone()).unwrap();

        let retrieved = store.get_tape(tape).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_tape_delete() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            end_epoch: EpochNumber(150),
        };

        store.put_tape(tape, info).unwrap();
        assert!(store.get_tape(tape).unwrap().is_some());

        store.delete_tape(tape).unwrap();
        assert!(store.get_tape(tape).unwrap().is_none());
    }
}
