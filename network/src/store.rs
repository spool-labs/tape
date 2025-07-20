use num_cpus;
use std::env;
use std::path::Path;
use std::fs;
use rocksdb::{
    BlockBasedOptions, ColumnFamilyDescriptor, DBCompressionType, Options,
    PlainTableFactoryOptions, SliceTransform, WriteBatch, DB,
};
use solana_sdk::pubkey::Pubkey;
use tape_api::SEGMENT_SIZE;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("RocksDB error: {0}")]
    RocksDB(#[from] rocksdb::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Health column family not found")]
    HealthCfNotFound,
    
    #[error("Tape by number column family not found")]
    TapeByNumberCfNotFound,
    #[error("Tape by address column family not found")]
    TapeByAddressCfNotFound,
    #[error("Segments column family not found")]
    SegmentsCfNotFound,
    #[error("Slots column family not found")]
    SlotsCfNotFound,
    #[error("Tape not found: number {0}")]
    TapeNotFound(u64),
    #[error("Segment not found for tape number {0}, segment {1}")]
    SegmentNotFound(u64, u64),
    #[error("Tape not found for address: {0}")]
    TapeNotFoundForAddress(String),
    #[error("Segment not found for address {0}, segment {1}")]
    SegmentNotFoundForAddress(String, u64),
    #[error("Invalid pubkey: {0}")]
    InvalidPubkey(String),
    #[error("Segment data exceeds maximum size of {0} bytes")]
    SegmentSizeExceeded(usize),
    #[error("Invalid segment key format")]
    InvalidSegmentKey,
    #[error("Invalid path")]
    InvalidPath,
}

pub struct TapeStore {
    pub db: DB,
}

#[derive(Debug)]
pub struct LocalStats {
    pub tapes: usize,
    pub segments: usize,
    pub size_bytes: u64,
}

impl TapeStore {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();

        let cfs = create_cf_descriptors();

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(8 * 1024 * 1024); // 8 MB
        db_opts.set_max_write_buffer_number(4);
        db_opts.increase_parallelism(num_cpus::get() as i32);

        let db = DB::open_cf_descriptors(
            &db_opts,
            path,
            cfs,
        )?;

        Ok(Self { db })
    }

    pub fn new_read_only<P: AsRef<Path>>(path: P) -> Result<Self, StoreError> {
        let path = path.as_ref();

        let cfs = create_cf_descriptors();

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

        let cfs = create_cf_descriptors();

        let db_opts = Options::default();

        let db = DB::open_cf_descriptors_as_secondary(
            &db_opts,
            primary_path,
            secondary_path,
            cfs,
        )?;
        Ok(Self { db })
    }

    pub fn catch_up_with_primary(&self) -> Result<(), StoreError> {
        self.db.try_catch_up_with_primary()?;
        Ok(())
    }

    /// Update the health values in the database.
    pub fn update_health(&self, last_processed_slot: u64, drift: u64) -> Result<(), StoreError> {
        let cf = self
            .db
            .cf_handle("health")
            .ok_or(StoreError::HealthCfNotFound)?;

        let mut batch = WriteBatch::default();
        batch.put_cf(cf, b"last_processed_slot", &last_processed_slot.to_be_bytes());
        batch.put_cf(cf, b"drift", &drift.to_be_bytes());

        self.db.write(batch)?;

        Ok(())
    }

    /// Load the last‐written health values.
    pub fn get_health(&self) -> Result<(u64, u64), StoreError> {
        let cf = self
            .db
            .cf_handle("health")
            .ok_or(StoreError::HealthCfNotFound)?;

        let bh = self
            .db
            .get_cf(cf, b"last_processed_slot")?
            .ok_or(StoreError::HealthCfNotFound)?;

        let dr = self
            .db
            .get_cf(cf, b"drift")?
            .ok_or(StoreError::HealthCfNotFound)?;

        let height = u64::from_be_bytes(bh[..].try_into().unwrap());
        let drift = u64::from_be_bytes(dr[..].try_into().unwrap());

        Ok((height, drift))
    }

    pub fn add_tape(&self, tape_number: u64, address: &Pubkey) -> Result<(), StoreError> {
        let cf_tape_by_number = self
            .db
            .cf_handle("tape_by_number")
            .ok_or(StoreError::TapeByNumberCfNotFound)?;

        let cf_tape_by_address = self
            .db
            .cf_handle("tape_by_address")
            .ok_or(StoreError::TapeByAddressCfNotFound)?;

        let tape_number_key = tape_number.to_be_bytes().to_vec();
        let address_key = address.to_bytes().to_vec();

        let mut batch = WriteBatch::default();
        batch.put_cf(cf_tape_by_number, &tape_number_key, &address.to_bytes());
        batch.put_cf(cf_tape_by_address, &address_key, &tape_number.to_be_bytes());
        self.db.write(batch)?;

        Ok(())
    }

    pub fn add_segment(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
        data: Vec<u8>,
    ) -> Result<(), StoreError> {
        if data.len() > SEGMENT_SIZE {
            return Err(StoreError::SegmentSizeExceeded(SEGMENT_SIZE));
        }

        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        self.db.put_cf(cf, &key, &data)?;

        Ok(())
    }

    pub fn add_slot(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
        slot: u64,
    ) -> Result<(), StoreError> {
        let cf = self
            .db
            .cf_handle("slots")
            .ok_or(StoreError::SlotsCfNotFound)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        self.db.put_cf(cf, &key, &slot.to_be_bytes())?;

        Ok(())
    }

    pub fn get_tape_number(&self, address: &Pubkey) -> Result<u64, StoreError> {
        let cf = self
            .db
            .cf_handle("tape_by_address")
            .ok_or(StoreError::TapeByAddressCfNotFound)?;

        let key = address.to_bytes().to_vec();
        let tape_number_bytes = self
            .db
            .get_cf(cf, &key)?
            .ok_or_else(|| StoreError::TapeNotFoundForAddress(address.to_string()))?;

        Ok(u64::from_be_bytes(
            tape_number_bytes
                .try_into()
                .map_err(|_| StoreError::InvalidSegmentKey)?,
        ))
    }

    pub fn get_tape_address(&self, tape_number: u64) -> Result<Pubkey, StoreError> {
        let cf = self
            .db
            .cf_handle("tape_by_number")
            .ok_or(StoreError::TapeByNumberCfNotFound)?;

        let key = tape_number.to_be_bytes().to_vec();
        let address_bytes = self
            .db
            .get_cf(cf, &key)?
            .ok_or(StoreError::TapeNotFound(tape_number))?;

        Pubkey::try_from(address_bytes.as_slice())
            .map_err(|e| StoreError::InvalidPubkey(e.to_string()))
    }

    pub fn get_tape_segments(
        &self,
        tape_address: &Pubkey,
    ) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;

        let prefix = tape_address.to_bytes().to_vec();

        let mut segments = Vec::new();
        let iter = self.db.prefix_iterator_cf(cf, &prefix);
        for item in iter {
            let (key, value) = item?;
            if key.len() != 40 {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }

            let segment_number = u64::from_be_bytes(
                key[32..40]
                    .try_into()
                    .map_err(|_| StoreError::InvalidSegmentKey)?,
            );

            // TODO: this requires that the iterator returns keys in order. Potentially dangerous.
            segments.push((segment_number, value.to_vec()));
        }

        Ok(segments)
    }

    pub fn get_segment_by_address(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        let segment_data = self
            .db
            .get_cf(cf, &key)?
            .ok_or(StoreError::SegmentNotFoundForAddress(tape_address.to_string(), segment_number))?;

        Ok(segment_data)
    }

    pub fn get_segment(
        &self,
        tape_number: u64,
        segment_number: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;

        let address = self.get_tape_address(tape_number)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        let segment_data = self
            .db
            .get_cf(cf, &key)?
            .ok_or(StoreError::SegmentNotFound(tape_number, segment_number))?;

        Ok(segment_data)
    }

    pub fn get_slot(
        &self,
        tape_number: u64,
        segment_number: u64,
    ) -> Result<u64, StoreError> {
        let cf = self
            .db
            .cf_handle("slots")
            .ok_or(StoreError::SlotsCfNotFound)?;

        let address = self.get_tape_address(tape_number)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        let slot_bytes = self
            .db
            .get_cf(cf, &key)?
            .ok_or(StoreError::SegmentNotFound(tape_number, segment_number))?;

        Ok(u64::from_be_bytes(
            slot_bytes.try_into().map_err(|_| StoreError::InvalidSegmentKey)?,
        ))
    }

    pub fn get_slot_by_address(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
    ) -> Result<u64, StoreError> {
        let cf = self
            .db
            .cf_handle("slots")
            .ok_or(StoreError::SlotsCfNotFound)?;

        let mut key = Vec::with_capacity(40);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());

        let slot_bytes = self
            .db
            .get_cf(cf, &key)?
            .ok_or(StoreError::SegmentNotFoundForAddress(tape_address.to_string(), segment_number))?;

        Ok(u64::from_be_bytes(
            slot_bytes.try_into().map_err(|_| StoreError::InvalidSegmentKey)?,
        ))
    }

    pub fn get_local_stats(&self) -> Result<LocalStats, StoreError> {
        let tapes = self.count_tapes()?;
        let segments = self.count_segments()?;
        let size_bytes = self.db_size()?;
        Ok(LocalStats { tapes, segments, size_bytes })
    }

    fn count_tapes(&self) -> Result<usize, StoreError> {
        let cf = self
            .db
            .cf_handle("tape_by_number")
            .ok_or(StoreError::TapeByNumberCfNotFound)?;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn count_segments(&self) -> Result<usize, StoreError> {
        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
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

fn create_cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    // Options for tape_by_number CF
    let mut cf_tape_by_number_opts = Options::default();
    cf_tape_by_number_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
    cf_tape_by_number_opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 8,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    });
    cf_tape_by_number_opts.set_compression_type(DBCompressionType::None);

    // Options for tape_by_address CF
    let mut cf_tape_by_address_opts = Options::default();
    cf_tape_by_address_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    cf_tape_by_address_opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 32,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    });
    cf_tape_by_address_opts.set_compression_type(DBCompressionType::None);

    // Options for segments CF
    let mut cf_segments_opts = Options::default();
    cf_segments_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    let mut bbt_segments = BlockBasedOptions::default();
    bbt_segments.set_bloom_filter(10.0, false);
    bbt_segments.set_block_size(256);
    cf_segments_opts.set_block_based_table_factory(&bbt_segments);
    cf_segments_opts.set_compression_type(DBCompressionType::None);

    // Options for slots CF
    let mut cf_slots_opts = Options::default();
    cf_slots_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    cf_slots_opts.set_plain_table_factory(&PlainTableFactoryOptions {
        user_key_length: 40,
        bloom_bits_per_key: 10,
        hash_table_ratio: 0.75,
        index_sparseness: 16,
        huge_page_tlb_size: 0,
        encoding_type: rocksdb::KeyEncodingType::Prefix,
        full_scan_mode: false,
        store_index_in_file: false,
    });
    cf_slots_opts.set_compression_type(DBCompressionType::None);

    // Options for health CF
    let mut cf_health_opts = Options::default();
    cf_health_opts.set_compression_type(DBCompressionType::None);

    let cf_tape_by_number = ColumnFamilyDescriptor::new("tape_by_number", cf_tape_by_number_opts);
    let cf_tape_by_address = ColumnFamilyDescriptor::new("tape_by_address", cf_tape_by_address_opts);
    let cf_segments = ColumnFamilyDescriptor::new("segments", cf_segments_opts);
    let cf_slots = ColumnFamilyDescriptor::new("slots", cf_slots_opts);
    let cf_health = ColumnFamilyDescriptor::new("health", cf_health_opts);

    vec![
        cf_tape_by_number,
        cf_tape_by_address,
        cf_segments,
        cf_slots,
        cf_health,
    ]
}

pub fn primary() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(|e| StoreError::IoError(e))?;
    let db_primary = current_dir.join("db_tapestore");
    std::fs::create_dir_all(&db_primary).map_err(|e| StoreError::IoError(e))?;
    TapeStore::new(&db_primary)
}

pub fn secondary() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(|e| StoreError::IoError(e))?;
    let db_primary = current_dir.join("db_tapestore");
    let db_secondary = current_dir.join("db_tapestore_read");
    std::fs::create_dir_all(&db_secondary).map_err(|e| StoreError::IoError(e))?;
    TapeStore::new_secondary(&db_primary, &db_secondary)
}

pub fn read_only() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(|e| StoreError::IoError(e))?;
    let db_primary = current_dir.join("db_tapestore");
    TapeStore::new_read_only(&db_primary)
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
    fn test_add_tape() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();

        store.add_tape(tape_number, &address)?;
        let retrieved_number = store.get_tape_number(&address)?;
        assert_eq!(retrieved_number, tape_number);
        let retrieved_address = store.get_tape_address(tape_number)?;
        assert_eq!(retrieved_address, address);

        Ok(())
    }

    #[test]
    fn test_add_segment_and_slot() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let segment_number = 0;
        let address = Pubkey::new_unique();
        let data = vec![1, 2, 3];
        let slot = 100;

        store.add_tape(tape_number, &address)?;
        store.add_segment(&address, segment_number, data.clone())?;
        store.add_slot(&address, segment_number, slot)?;

        let retrieved_data = store.get_segment(tape_number, segment_number)?;
        assert_eq!(retrieved_data, data);
        let retrieved_slot = store.get_slot(tape_number, segment_number)?;
        assert_eq!(retrieved_slot, slot);

        Ok(())
    }

    #[test]
    fn test_add_and_get_segments() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();

        let segment_data_1 = vec![1, 2, 3];
        let segment_data_2 = vec![4, 5, 6];
        let slot_1 = 100;
        let slot_2 = 101;

        store.add_segment(&address, 1, segment_data_2.clone())?;
        store.add_slot(&address, 1, slot_2)?;
        store.add_segment(&address, 0, segment_data_1.clone())?;
        store.add_slot(&address, 0, slot_1)?;

        store.add_tape(tape_number, &address)?;

        let segments = store.get_tape_segments(&address)?;
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], (0, segment_data_1));
        assert_eq!(segments[1], (1, segment_data_2));

        let slot_retrieved_0 = store.get_slot(tape_number, 0)?;
        assert_eq!(slot_retrieved_0, slot_1);
        let slot_retrieved_1 = store.get_slot(tape_number, 1)?;
        assert_eq!(slot_retrieved_1, slot_2);

        let non_address = Pubkey::new_unique();
        let segments = store.get_tape_segments(&non_address)?;
        assert_eq!(segments.len(), 0);

        Ok(())
    }

    #[test]
    fn test_get_segment_by_address() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let segment_number = 0;
        let data = vec![1, 2, 3];
        let slot = 100;

        store.add_segment(&address, segment_number, data.clone())?;
        store.add_slot(&address, segment_number, slot)?;

        let retrieved_data = store.get_segment_by_address(&address, segment_number)?;
        assert_eq!(retrieved_data, data);
        let retrieved_slot = store.get_slot_by_address(&address, segment_number)?;
        assert_eq!(retrieved_slot, slot);

        Ok(())
    }

    #[test]
    fn test_segment_size_limit() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();

        let oversized_data = vec![0; SEGMENT_SIZE + 1];
        let result = store.add_segment(&address, 0, oversized_data);
        assert!(matches!(result, Err(StoreError::SegmentSizeExceeded(_))));

        Ok(())
    }

    #[test]
    fn test_error_cases() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();

        let result = store.get_tape_number(&address);
        assert!(matches!(result, Err(StoreError::TapeNotFoundForAddress(_))));

        let result = store.get_tape_address(1);
        assert!(matches!(result, Err(StoreError::TapeNotFound(1))));

        Ok(())
    }

    #[test]
    fn test_multiple_tapes() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;

        let tape1_number = 1;
        let tape1_address = Pubkey::new_unique();
        let tape2_number = 2;
        let tape2_address = Pubkey::new_unique();

        store.add_segment(&tape1_address, 0, vec![1, 2, 3])?;
        store.add_slot(&tape1_address, 0, 100)?;
        store.add_tape(tape1_number, &tape1_address)?;

        store.add_segment(&tape2_address, 0, vec![4, 5, 6])?;
        store.add_slot(&tape2_address, 0, 101)?;
        store.add_tape(tape2_number, &tape2_address)?;

        assert_eq!(store.get_tape_number(&tape1_address)?, tape1_number);
        assert_eq!(store.get_tape_address(tape1_number)?, tape1_address);
        let tape1_segments = store.get_tape_segments(&tape1_address)?;
        assert_eq!(tape1_segments.len(), 1);
        assert_eq!(tape1_segments[0], (0, vec![1, 2, 3]));

        assert_eq!(store.get_tape_number(&tape2_address)?, tape2_number);
        assert_eq!(store.get_tape_address(tape2_number)?, tape2_address);
        let tape2_segments = store.get_tape_segments(&tape2_address)?;
        assert_eq!(tape2_segments.len(), 1);
        assert_eq!(tape2_segments[0], (0, vec![4, 5, 6]));

        Ok(())
    }

    #[test]
    fn test_get_segment() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let segment_number = 0;
        let address = Pubkey::new_unique();
        let segment_data = vec![1, 2, 3];

        store.add_segment(&address, segment_number, segment_data.clone())?;
        store.add_slot(&address, segment_number, 100)?;
        store.add_tape(tape_number, &address)?;

        let retrieved_data = store.get_segment(tape_number, segment_number)?;
        assert_eq!(retrieved_data, segment_data);

        Ok(())
    }

    #[test]
    fn test_get_segment_non_existent() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let segment_number = 0;
        let address = Pubkey::new_unique();

        store.add_tape(tape_number, &address)?;

        let result = store.get_segment(tape_number, segment_number);
        assert!(matches!(result, Err(StoreError::SegmentNotFound(_, s)) if s == segment_number));

        Ok(())
    }

    #[test]
    fn test_get_multiple_segments() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();
        let segment_data_1 = vec![1, 2, 3];
        let segment_data_2 = vec![4, 5, 6];

        store.add_segment(&address, 0, segment_data_1.clone())?;
        store.add_slot(&address, 0, 100)?;
        store.add_segment(&address, 1, segment_data_2.clone())?;
        store.add_slot(&address, 1, 101)?;
        store.add_tape(tape_number, &address)?;

        let retrieved_data_1 = store.get_segment(tape_number, 0)?;
        assert_eq!(retrieved_data_1, segment_data_1);

        let retrieved_data_2 = store.get_segment(tape_number, 1)?;
        assert_eq!(retrieved_data_2, segment_data_2);

        Ok(())
    }

    #[test]
    fn test_get_local_stats() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 0);
        assert_eq!(stats.segments, 0);

        let tape_number = 1;
        let address = Pubkey::new_unique();
        store.add_tape(tape_number, &address)?;
        store.add_segment(&address, 0, vec![1, 2, 3])?;
        store.add_segment(&address, 1, vec![4, 5, 6])?;

        let stats = store.get_local_stats()?;
        assert_eq!(stats.tapes, 1);
        assert_eq!(stats.segments, 2);
        assert!(stats.size_bytes > 0);

        Ok(())
    }
}
