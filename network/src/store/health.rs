use rocksdb::WriteBatch;
use super::{TapeStore, error::StoreError, layout::ColumnFamily};

pub enum StoreStaticKeys {
    LastProcessedSlot,
    Drift,
}

impl StoreStaticKeys {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            StoreStaticKeys::LastProcessedSlot => b"last_processed_slot",
            StoreStaticKeys::Drift => b"drift",
        }
    }
}


pub trait HealthOps {
    fn update_health(&self, last_processed_slot: u64, drift: u64) -> Result<(), StoreError>;
    fn get_health(&self) -> Result<(u64, u64), StoreError>;
}

impl HealthOps for TapeStore {
    fn update_health(&self, last_processed_slot: u64, drift: u64) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Health)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, StoreStaticKeys::LastProcessedSlot.as_bytes(), last_processed_slot.to_be_bytes());
        batch.put_cf(&cf, StoreStaticKeys::Drift.as_bytes(), drift.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    fn get_health(&self) -> Result<(u64, u64), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Health)?;
        let bh = self
            .db
            .get_cf(&cf, StoreStaticKeys::LastProcessedSlot.as_bytes())?
            .ok_or(StoreError::HealthCfNotFound)?;
        let dr = self
            .db
            .get_cf(&cf, StoreStaticKeys::Drift.as_bytes())?
            .ok_or(StoreError::HealthCfNotFound)?;
        let height = u64::from_be_bytes(bh[..].try_into().unwrap());
        let drift = u64::from_be_bytes(dr[..].try_into().unwrap());
        Ok((height, drift))
    }
}
