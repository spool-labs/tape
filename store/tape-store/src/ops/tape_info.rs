//! TapeInfo operations for tape metadata
//!
//! Provides storage for tape (storage allocation) information.

use crate::columns::TapeInfoCol;
use crate::error::Result;
use crate::types::{EpochNumber, Pubkey, TapeInfo};
use crate::TapeStore;
use store::Store;

/// Operations for tape info
pub trait TapeInfoOps {
    /// Get tape info by address
    fn get_tape_info(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>>;

    /// Store tape info
    fn put_tape_info(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()>;

    /// Delete tape info
    fn delete_tape_info(&self, tape_address: Pubkey) -> Result<()>;

    /// Iterate over expired tapes (expiry_epoch <= given epoch)
    fn iter_expired_tapes(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>>;
}

impl<S: Store> TapeInfoOps for TapeStore<S> {
    fn get_tape_info(&self, tape_address: Pubkey) -> Result<Option<TapeInfo>> {
        Ok(self.get::<TapeInfoCol>(&tape_address)?)
    }

    fn put_tape_info(&self, tape_address: Pubkey, info: TapeInfo) -> Result<()> {
        self.put::<TapeInfoCol>(&tape_address, &info)?;
        Ok(())
    }

    fn delete_tape_info(&self, tape_address: Pubkey) -> Result<()> {
        self.delete::<TapeInfoCol>(&tape_address)?;
        Ok(())
    }

    fn iter_expired_tapes(
        &self,
        epoch: EpochNumber,
    ) -> Result<impl Iterator<Item = Result<Pubkey>>> {
        let iter = self.iter::<TapeInfoCol>()?;
        Ok(iter.into_iter().filter_map(move |(addr, info)| {
            if info.expiry_epoch <= epoch {
                Some(Ok(addr))
            } else {
                None
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use store_memory::MemoryStore;

    fn test_store() -> TapeStore<MemoryStore> {
        TapeStore::new(MemoryStore::new())
    }

    #[test]
    fn test_tape_info_roundtrip() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            active_epoch: EpochNumber(100),
            expiry_epoch: EpochNumber(200),
            authority: Pubkey::new_unique(),
        };

        assert!(store.get_tape_info(tape).unwrap().is_none());

        store.put_tape_info(tape, info.clone()).unwrap();

        let retrieved = store.get_tape_info(tape).unwrap().unwrap();
        assert_eq!(retrieved, info);
    }

    #[test]
    fn test_tape_info_delete() {
        let store = test_store();
        let tape = Pubkey::new_unique();

        let info = TapeInfo {
            active_epoch: EpochNumber(50),
            expiry_epoch: EpochNumber(150),
            authority: Pubkey::new_unique(),
        };

        store.put_tape_info(tape, info).unwrap();
        assert!(store.get_tape_info(tape).unwrap().is_some());

        store.delete_tape_info(tape).unwrap();
        assert!(store.get_tape_info(tape).unwrap().is_none());
    }

    #[test]
    fn test_iter_expired_tapes() {
        let store = test_store();

        // Create tapes with different expiry epochs
        let tape1 = Pubkey::new_unique();
        let tape2 = Pubkey::new_unique();
        let tape3 = Pubkey::new_unique();

        store
            .put_tape_info(
                tape1,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(50),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        store
            .put_tape_info(
                tape2,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(100),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        store
            .put_tape_info(
                tape3,
                TapeInfo {
                    active_epoch: EpochNumber(0),
                    expiry_epoch: EpochNumber(200),
                    authority: Pubkey::new_unique(),
                },
            )
            .unwrap();

        // Check expired at epoch 100
        let expired: Vec<Pubkey> = store
            .iter_expired_tapes(EpochNumber(100))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(expired.len(), 2);
        assert!(expired.contains(&tape1));
        assert!(expired.contains(&tape2));
        assert!(!expired.contains(&tape3));
    }
}
