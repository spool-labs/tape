use std::fs;
use std::path::Path;
use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DB, Options, WriteBatch};
use solana_sdk::pubkey::Pubkey;

use tape_api::PACKED_SEGMENT_SIZE;
use crate::metrics::{inc_total_segments_written, inc_total_tapes_written};
use super::{
    column_family::ColumnFamily,
    consts::*,
    error::StoreError,
};

pub enum StoreStaticKeys {
    LastProcessedSlot,
    Drift,
}

impl StoreStaticKeys {
    fn as_bytes(&self) -> &'static [u8] {
        match self {
            StoreStaticKeys::LastProcessedSlot => b"last_processed_slot",
            StoreStaticKeys::Drift => b"drift",
        }
    }
}

pub struct TapeStore {
    pub db: DB,
}

#[derive(Debug)]
pub struct LocalStats {
    pub tapes: usize,
    pub sectors: usize,
    pub size_bytes: u64,
}

impl TapeStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let cfs = super::cf_layout::create_cf_descriptors();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(TAPE_STORE_MAX_WRITE_BUFFER_SIZE);
        db_opts.set_max_write_buffer_number(TAPE_STORE_MAX_WRITE_BUFFERS as i32);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
    }

    pub fn try_init_store() -> Result<(), StoreError> {
        if let Ok(_store) = super::helpers::primary() {
            log::debug!("Primary store initialized successfully");
        }
        Ok(())
    }

    pub fn get_cf_handle(&self, column_family: ColumnFamily) -> Result<Arc<BoundColumnFamily<'_>>, StoreError> {
        self.db
            .cf_handle(column_family.as_str())
            .ok_or(StoreError::from(&column_family))
    }

    pub fn new_read_only<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let cfs = super::cf_layout::create_cf_descriptors();
        let db_opts = Options::default();
        let db = DB::open_cf_descriptors_read_only(&db_opts, path, cfs, false)?;
        Ok(Self { db })
    }

    pub fn new_secondary<P: AsRef<Path>>(
        primary_path: P,
        secondary_path: P,
    ) -> Result<Self, StoreError> {
        let primary_path = primary_path.as_ref();
        let secondary_path = secondary_path.as_ref();
        let cfs = super::cf_layout::create_cf_descriptors();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(TAPE_STORE_MAX_WRITE_BUFFER_SIZE);
        db_opts.set_max_write_buffer_number(TAPE_STORE_MAX_WRITE_BUFFERS as i32);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors_as_secondary(&db_opts, primary_path, secondary_path, cfs)?;
        Ok(Self { db })
    }

    pub fn catch_up_with_primary(&self) -> Result<(), StoreError> {
        self.db.try_catch_up_with_primary()?;
        Ok(())
    }

    pub fn update_health(&self, last_processed_slot: u64, drift: u64) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Health)?;
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf, StoreStaticKeys::LastProcessedSlot.as_bytes(), last_processed_slot.to_be_bytes());
        batch.put_cf(&cf, StoreStaticKeys::Drift.as_bytes(), drift.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn get_health(&self) -> Result<(u64, u64), StoreError> {
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

    pub fn put_tape(&self, tape_number: u64, address: &Pubkey) -> Result<(), StoreError> {
        let cf_tape_by_number = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let cf_tape_by_address = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let tape_number_key = tape_number.to_be_bytes().to_vec();
        let address_key = address.to_bytes().to_vec();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_tape_by_number, &tape_number_key, address.to_bytes());
        batch.put_cf(&cf_tape_by_address, &address_key, tape_number.to_be_bytes());
        self.db.write(batch)?;
        inc_total_tapes_written();
        Ok(())
    }

    pub fn put_segment(&self, tape: &Pubkey, global_seg_idx: u64, seg: Vec<u8>) -> Result<(), StoreError> {
        if seg.len() > PACKED_SEGMENT_SIZE {
            return Err(StoreError::SegmentSizeExceeded(PACKED_SEGMENT_SIZE));
        }
        
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let cf_sectors = self.get_cf_handle(ColumnFamily::Sectors)?;
        let cf_tape_stats = self.get_cf_handle(ColumnFamily::TapeStats)?;
        
        let mut sector = self.get_sector(tape, sector_number).unwrap_or_else(|_| {
            vec![0u8; SECTOR_HEADER_BYTES + SECTOR_LEAVES * PACKED_SEGMENT_SIZE]
        });
        
        // Check if segment bit is already set
        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        let is_new_segment = (sector[bitmap_idx] & (1 << bit_pos)) == 0;
        
        // Set bitmap bit
        sector[bitmap_idx] |= 1 << bit_pos;
        
        // Write segment data
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        sector[seg_start..seg_start + PACKED_SEGMENT_SIZE].copy_from_slice(&seg);
        
        // Update segment count if new segment
        let mut batch = WriteBatch::default();
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        batch.put_cf(&cf_sectors, &key, &sector);
        
        if is_new_segment {
            let current_count = self.get_segment_count(tape).unwrap_or(0);
            batch.put_cf(&cf_tape_stats, tape.to_bytes(), (current_count + 1).to_be_bytes());
        }
        
        self.db.write(batch)?;
        inc_total_segments_written();

        Ok(())
    }

    pub fn get_segment(&self, tape: &Pubkey, global_seg_idx: u64) -> Result<Vec<u8>, StoreError> {
        let sector_number = global_seg_idx / SECTOR_LEAVES as u64;
        let local_seg_idx = (global_seg_idx % SECTOR_LEAVES as u64) as usize;
        
        let sector = self.get_sector(tape, sector_number)?;
        
        // Check bitmap
        let bitmap_idx = local_seg_idx / 8;
        let bit_pos = local_seg_idx % 8;
        if (sector[bitmap_idx] & (1 << bit_pos)) == 0 {
            return Err(StoreError::SegmentNotFoundForAddress(tape.to_string(), global_seg_idx));
        }
        
        let seg_start = SECTOR_HEADER_BYTES + local_seg_idx * PACKED_SEGMENT_SIZE;
        Ok(sector[seg_start..seg_start + PACKED_SEGMENT_SIZE].to_vec())
    }

    pub fn get_sector(&self, tape: &Pubkey, sector_number: u64) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&sector_number.to_be_bytes());
        
        self.db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::SegmentNotFoundForAddress(tape.to_string(), sector_number))
    }

    pub fn put_sector(&self, tape: &Pubkey, sector: &[u8]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape.to_bytes());
        key.extend_from_slice(&[0; 8]); // Assuming sector number is part of the sector data
        self.db.put_cf(&cf, &key, sector)?;
        Ok(())
    }

    pub fn get_l13(&self, tape: &Pubkey) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[0; 3]); // padding
        
        self.db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(tape.to_string()))
    }

    pub fn put_l13(&self, tape: &Pubkey, l13: &[u8]) -> Result<(), StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::MerkleLayers)?;
        let mut key = Vec::with_capacity(36);
        key.extend_from_slice(&tape.to_bytes());
        key.push(13); // layer_id
        key.extend_from_slice(&[0; 3]); // padding
        self.db.put_cf(&cf, &key, l13)?;
        Ok(())
    }

    pub fn get_tape_number(&self, address: &Pubkey) -> Result<u64, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let key = address.to_bytes().to_vec();
        let tape_number_bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(address.to_string()))?;
        Ok(u64::from_be_bytes(
            tape_number_bytes
                .try_into()
                .map_err(|_| StoreError::InvalidSegmentKey)?,
        ))
    }

    pub fn get_tape_address(&self, tape_number: u64) -> Result<Pubkey, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let key = tape_number.to_be_bytes().to_vec();
        let address_bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::TapeNotFound(tape_number))?;
        Pubkey::try_from(address_bytes.as_slice())
            .map_err(|e| StoreError::InvalidPubkey(e.to_string()))
    }

    pub fn get_segment_count(&self, tape: &Pubkey) -> Result<u64, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeStats)?;
        let count_bytes = self
            .db
            .get_cf(&cf, tape.to_bytes())?
            .unwrap_or_else(|| vec![0; 8]);
        Ok(u64::from_be_bytes(count_bytes[..].try_into().unwrap()))
    }

    pub fn get_sector_count(&self, tape_address: &Pubkey) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Sectors)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        Ok(iter.count())
    }

    pub fn get_local_stats(&self) -> Result<LocalStats, StoreError> {
        let tapes = self.count_tapes()?;
        let sectors = self.count_sectors()?;
        let size_bytes = self.db_size()?;
        Ok(LocalStats { tapes, sectors, size_bytes })
    }

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

impl Drop for TapeStore {
    fn drop(&mut self) {
        // RocksDB handles cleanup automatically
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use tempdir::TempDir;

    fn setup_store() -> Result<(TapeStore, TempDir), StoreError> {
        let temp_dir = TempDir::new("rocksdb_test").map_err(StoreError::IoError)?;
        let store = TapeStore::new(temp_dir.path())?;
        Ok((store, temp_dir))
    }

    #[test]
    fn test_put_tape() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();

        store.put_tape(tape_number, &address)?;
        let retrieved_number = store.get_tape_number(&address)?;
        assert_eq!(retrieved_number, tape_number);
        let retrieved_address = store.get_tape_address(tape_number)?;
        assert_eq!(retrieved_address, address);
        Ok(())
    }

    #[test]
    fn test_put_segment() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let global_seg_idx = 0;
        let data = vec![0u8; tape_api::PACKED_SEGMENT_SIZE];

        store.put_segment(&address, global_seg_idx, data.clone())?;
        let retrieved_data = store.get_segment(&address, global_seg_idx)?;
        assert_eq!(retrieved_data, data);
        Ok(())
    }

    #[test]
    fn test_put_segment_count() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let data = vec![0u8; tape_api::PACKED_SEGMENT_SIZE];

        // Write two new segments
        store.put_segment(&address, 0, data.clone())?;
        store.put_segment(&address, 1, data.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Overwrite existing segment (should not increment count)
        store.put_segment(&address, 0, data.clone())?;
        assert_eq!(store.get_segment_count(&address)?, 2);

        // Write new segment
        store.put_segment(&address, 2, data)?;
        assert_eq!(store.get_segment_count(&address)?, 3);
        Ok(())
    }

    #[test]
    fn test_put_l13() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let l13_data = vec![0u8; L13_NODES_PER_TAPE * 32];

        store.put_l13(&address, &l13_data)?;
        let retrieved_data = store.get_l13(&address)?;
        assert_eq!(retrieved_data, l13_data);
        Ok(())
    }

    #[test]
    fn test_get_local_stats() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 0);
        assert_eq!(stats.sectors, 0);

        let tape_number = 1;
        let address = Pubkey::new_unique();
        store.put_tape(tape_number, &address)?;
        store.put_segment(&address, 0, vec![0u8; tape_api::PACKED_SEGMENT_SIZE])?;

        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 1);
        assert_eq!(stats.sectors, 1);
        assert!(stats.size_bytes > 0);
        Ok(())
    }
}
