use std::fs;
use solana_sdk::pubkey::Pubkey;
use super::{TapeStore, error::StoreError, layout::ColumnFamily};

#[derive(Debug)]
pub struct LocalStats {
    pub tapes: usize,
    pub sectors: usize,
    pub size_bytes: u64,
}

pub trait StatsOps {
    fn get_local_stats(&self) -> Result<LocalStats, StoreError>;
    fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError>;
}

impl StatsOps for TapeStore {
    fn get_local_stats(&self) -> Result<LocalStats, StoreError> {
        let tapes = self.count_tapes()?;
        let sectors = self.count_sectors()?;
        let size_bytes = self.db_size()?;
        Ok(LocalStats { tapes, sectors, size_bytes })
    }

    fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        Ok(iter.count())
    }
}

impl TapeStore {
    fn count_tapes(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn count_sectors(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn db_size(&self) -> Result<u64, StoreError> {
        let mut size = 0u64;
        for entry in fs::read_dir(self.db.path())? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                size += entry.metadata()?.len();
            }
        }
        Ok(size)
    }
}
