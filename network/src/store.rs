use num_cpus;
use log::debug;
use std::{env, sync::Arc};
use std::path::Path;
use std::fs;
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options,
    PlainTableFactoryOptions, SliceTransform, WriteBatch, DB
};
use solana_sdk::pubkey::Pubkey;
use tape_api::PACKED_SEGMENT_SIZE;
use thiserror::Error;

pub const TAPE_STORE_PRIMARY_DB: &str = "db_tapestore";
pub const TAPE_STORE_SECONDARY_DB_MINE: &str = "db_tapestore_read_mine";
pub const TAPE_STORE_SECONDARY_DB_WEB: &str = "db_tapestore_read_web";
pub const TAPE_STORE_SLOTS_KEY_SIZE: usize = 40; // 40 bytes
pub const TAPE_STORE_MAX_WRITE_BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MB
pub const TAPE_STORE_MAX_WRITE_BUFFERS: usize = 4; // This is related to concurrency

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
    #[error("KeyValuesLenMismatch")]
    InvalidKeyValuePairLen
}

impl From<&ColumnFamily> for StoreError {
    fn from(value: &ColumnFamily) -> Self {
        match value {
            ColumnFamily::TapeByNumber => StoreError::TapeByNumberCfNotFound,
            ColumnFamily::TapeByAddress => StoreError::TapeByAddressCfNotFound,
            ColumnFamily::Segments => StoreError::SegmentsCfNotFound,
            ColumnFamily::Health => StoreError::HealthCfNotFound,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ColumnFamily {
    TapeByNumber,
    TapeByAddress,
    Segments,
    Health,
}

impl ColumnFamily {
    pub fn into_cf_descriptor(&self) -> &'static str {
        match self {
            ColumnFamily::TapeByNumber => "tape_by_number",
            ColumnFamily::TapeByAddress => "tape_by_address",
            ColumnFamily::Segments => "segments",
            ColumnFamily::Health => "health",
        }
    }
}

pub enum StoreStaticKeys {
    LastProcessedSlot,
    Drift
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
        db_opts.set_write_buffer_size(TAPE_STORE_MAX_WRITE_BUFFER_SIZE);
        db_opts.set_max_write_buffer_number(TAPE_STORE_MAX_WRITE_BUFFERS as i32);
        db_opts.increase_parallelism(num_cpus::get() as i32);
        let db = DB::open_cf_descriptors(&db_opts, path, cfs)?;
        Ok(Self { db })
    }

    pub fn try_init_store() -> Result<(), StoreError> {
        if let Ok(_store) = primary() {
            debug!("Primary store initialized successfully");
        }

        Ok(())
    }

    pub fn get_cf_handle(&self, column_family: ColumnFamily) -> Result<Arc<BoundColumnFamily<'_>>, StoreError> {
        self.db
            .cf_handle(column_family.into_cf_descriptor())
            .ok_or(StoreError::from(&column_family))
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

    pub fn write_tape(&self, tape_number: u64, address: &Pubkey) -> Result<(), StoreError> {
        let cf_tape_by_number = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let cf_tape_by_address = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let tape_number_key = tape_number.to_be_bytes().to_vec();
        let address_key = address.to_bytes().to_vec();
        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_tape_by_number, &tape_number_key, address.to_bytes());
        batch.put_cf(&cf_tape_by_address, &address_key, tape_number.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn write_tapes_batch(
        &self,
        tape_number_vec: &[u64],
        address_vec: &[Pubkey],
    ) -> Result<(), StoreError> {
        if tape_number_vec.len() != address_vec.len() {
            return Err(StoreError::InvalidKeyValuePairLen);
        }
        let cf_tape_by_number = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let cf_tape_by_address = self.get_cf_handle(ColumnFamily::TapeByAddress)?;
        let mut batch = WriteBatch::default();
        for i in 0..tape_number_vec.len() {
            let number_bytes = tape_number_vec[i].to_be_bytes();
            let address_bytes = address_vec[i].to_bytes();
            batch.put_cf(&cf_tape_by_number, number_bytes, address_bytes);
            batch.put_cf(&cf_tape_by_address, address_bytes, number_bytes);
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn write_segment(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
        data: Vec<u8>,
    ) -> Result<(), StoreError> {
        if data.len() > PACKED_SEGMENT_SIZE {
            return Err(StoreError::SegmentSizeExceeded(PACKED_SEGMENT_SIZE));
        }
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());
        self.db.put_cf(&cf, &key, &data)?;
        Ok(())
    }

    pub fn write_segments_batch(
        &self,
        tape_address_vec: &[Pubkey],
        segment_number_vec: &[u64],
        data_vec: Vec<Vec<u8>>
    ) -> Result<(), StoreError> {
        for d in data_vec.iter(){
            if d.len() > PACKED_SEGMENT_SIZE{
                return Err(StoreError::SegmentSizeExceeded(PACKED_SEGMENT_SIZE));
            }
        }
        if tape_address_vec.len() != segment_number_vec.len() || data_vec.len() != segment_number_vec.len() {
            return Err(StoreError::InvalidKeyValuePairLen)
        }
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let mut batch = WriteBatch::default();
        for (i,d) in data_vec.into_iter().enumerate(){
            let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
            key.extend_from_slice(&tape_address_vec[i].to_bytes());
            key.extend_from_slice(&segment_number_vec[i].to_be_bytes());
            batch.put_cf(&cf, key, d);
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn read_tape_number(&self, address: &Pubkey) -> Result<u64, StoreError> {
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

    pub fn read_tape_address(&self, tape_number: u64) -> Result<Pubkey, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let key = tape_number.to_be_bytes().to_vec();
        let address_bytes = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::TapeNotFound(tape_number))?;
        Pubkey::try_from(address_bytes.as_slice())
            .map_err(|e| StoreError::InvalidPubkey(e.to_string()))
    }

    pub fn read_segment_count(
        &self,
        tape_address: &Pubkey,
    ) -> Result<usize, StoreError> {
        let cf = self
            .db
            .cf_handle("segments")
            .ok_or(StoreError::SegmentsCfNotFound)?;
        let prefix = tape_address.to_bytes().to_vec();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        let count = iter.count();
        Ok(count)
    }

    pub fn read_tape_segments(
        &self,
        tape_address: &Pubkey,
    ) -> Result<Vec<(u64, Vec<u8>)>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let prefix = tape_address.to_bytes().to_vec();
        let mut segments = Vec::new();
        let iter = self.db.prefix_iterator_cf(&cf, &prefix);
        for item in iter {
            let (key, value) = item?;
            if key.len() != TAPE_STORE_SLOTS_KEY_SIZE {
                continue;
            }
            if !key.starts_with(&prefix) {
                continue;
            }
            let segment_number = u64::from_be_bytes(
                key[32..TAPE_STORE_SLOTS_KEY_SIZE]
                    .try_into()
                    .map_err(|_| StoreError::InvalidSegmentKey)?,
            );
            segments.push((segment_number, value.to_vec()));
        }
        Ok(segments)
    }

    pub fn read_segment_by_address(
        &self,
        tape_address: &Pubkey,
        segment_number: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&tape_address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());
        let segment_data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::SegmentNotFoundForAddress(tape_address.to_string(), segment_number))?;
        Ok(segment_data)
    }

    pub fn read_segment(
        &self,
        tape_number: u64,
        segment_number: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
        let address = self.read_tape_address(tape_number)?;
        let mut key = Vec::with_capacity(TAPE_STORE_SLOTS_KEY_SIZE);
        key.extend_from_slice(&address.to_bytes());
        key.extend_from_slice(&segment_number.to_be_bytes());
        let segment_data = self
            .db
            .get_cf(&cf, &key)?
            .ok_or(StoreError::SegmentNotFound(tape_number, segment_number))?;
        Ok(segment_data)
    }

    pub fn read_local_stats(&self) -> Result<LocalStats, StoreError> {
        let tapes = self.count_tapes()?;
        let segments = self.count_segments()?;
        let size_bytes = self.db_size()?;
        Ok(LocalStats { tapes, segments, size_bytes })
    }

    fn count_tapes(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::TapeByNumber)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        Ok(iter.count())
    }

    fn count_segments(&self) -> Result<usize, StoreError> {
        let cf = self.get_cf_handle(ColumnFamily::Segments)?;
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

pub fn run_refresh_store(store: &Arc<TapeStore>) {
    let store = Arc::clone(store);
    tokio::spawn(async move {
        let interval = std::time::Duration::from_secs(15);
        loop {
            store.catch_up_with_primary().unwrap();
            tokio::time::sleep(interval).await;
        }
    });
}

fn create_cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
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

    let mut cf_segments_opts = Options::default();
    cf_segments_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
    let mut bbt_segments = BlockBasedOptions::default();
    bbt_segments.set_bloom_filter(10.0, false);
    bbt_segments.set_block_size(256);
    cf_segments_opts.set_block_based_table_factory(&bbt_segments);
    cf_segments_opts.set_compression_type(DBCompressionType::None);

    let mut cf_health_opts = Options::default();
    cf_health_opts.set_compression_type(DBCompressionType::None);

    let cf_tape_by_number = ColumnFamilyDescriptor::new(ColumnFamily::TapeByNumber.into_cf_descriptor(), cf_tape_by_number_opts);
    let cf_tape_by_address = ColumnFamilyDescriptor::new(ColumnFamily::TapeByAddress.into_cf_descriptor(), cf_tape_by_address_opts);
    let cf_segments = ColumnFamilyDescriptor::new(ColumnFamily::Segments.into_cf_descriptor(), cf_segments_opts);
    let cf_health = ColumnFamilyDescriptor::new(ColumnFamily::Health.into_cf_descriptor(), cf_health_opts);

    vec![
        cf_tape_by_number,
        cf_tape_by_address,
        cf_segments,
        cf_health,
    ]
}

pub fn primary() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
    std::fs::create_dir_all(&db_primary).map_err(StoreError::IoError)?;
    TapeStore::new(&db_primary)
}

pub fn secondary_mine() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
    let db_secondary = current_dir.join(TAPE_STORE_SECONDARY_DB_MINE);
    std::fs::create_dir_all(&db_secondary).map_err(StoreError::IoError)?;
    TapeStore::new_secondary(&db_primary, &db_secondary)
}

pub fn secondary_web() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
    let db_secondary = current_dir.join(TAPE_STORE_SECONDARY_DB_WEB);
    std::fs::create_dir_all(&db_secondary).map_err(StoreError::IoError)?;
    TapeStore::new_secondary(&db_primary, &db_secondary)
}

pub fn read_only() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
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

        store.write_tape(tape_number, &address)?;
        let retrieved_number = store.read_tape_number(&address)?;
        assert_eq!(retrieved_number, tape_number);
        let retrieved_address = store.read_tape_address(tape_number)?;
        assert_eq!(retrieved_address, address);
        Ok(())
    }

    #[test]
    fn test_add_segment() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let segment_number = 0;
        let address = Pubkey::new_unique();
        let data = vec![1, 2, 3];

        store.write_tape(tape_number, &address)?;
        store.write_segment(&address, segment_number, data.clone())?;
        let retrieved_data = store.read_segment(tape_number, segment_number)?;
        assert_eq!(retrieved_data, data);
        Ok(())
    }

    #[test]
    fn test_add_and_get_segments() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let address = Pubkey::new_unique();
        let segment_data_1 = vec![1, 2, 3];
        let segment_data_2 = vec![4, 5, 6];

        store.write_segment(&address, 1, segment_data_2.clone())?;
        store.write_segment(&address, 0, segment_data_1.clone())?;
        store.write_tape(tape_number, &address)?;

        let segments = store.read_tape_segments(&address)?;
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0], (0, segment_data_1));
        assert_eq!(segments[1], (1, segment_data_2));

        let non_address = Pubkey::new_unique();
        let segments = store.read_tape_segments(&non_address)?;
        assert_eq!(segments.len(), 0);
        Ok(())
    }

    #[test]
    fn test_get_segment_by_address() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let segment_number = 0;
        let data = vec![1, 2, 3];

        store.write_segment(&address, segment_number, data.clone())?;
        let retrieved_data = store.read_segment_by_address(&address, segment_number)?;
        assert_eq!(retrieved_data, data);
        Ok(())
    }

    #[test]
    fn test_segment_size_limit() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let oversized_data = vec![0; PACKED_SEGMENT_SIZE + 1];
        let result = store.write_segment(&address, 0, oversized_data);
        assert!(matches!(result, Err(StoreError::SegmentSizeExceeded(_))));
        Ok(())
    }

    #[test]
    fn test_error_cases() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let address = Pubkey::new_unique();
        let result = store.read_tape_number(&address);
        assert!(matches!(result, Err(StoreError::TapeNotFoundForAddress(_))));
        let result = store.read_tape_address(1);
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

        store.write_segment(&tape1_address, 0, vec![1, 2, 3])?;
        store.write_tape(tape1_number, &tape1_address)?;
        store.write_segment(&tape2_address, 0, vec![4, 5, 6])?;
        store.write_tape(tape2_number, &tape2_address)?;

        assert_eq!(store.read_tape_number(&tape1_address)?, tape1_number);
        assert_eq!(store.read_tape_address(tape1_number)?, tape1_address);
        let tape1_segments = store.read_tape_segments(&tape1_address)?;
        assert_eq!(tape1_segments.len(), 1);
        assert_eq!(tape1_segments[0], (0, vec![1, 2, 3]));

        assert_eq!(store.read_tape_number(&tape2_address)?, tape2_number);
        assert_eq!(store.read_tape_address(tape2_number)?, tape2_address);
        let tape2_segments = store.read_tape_segments(&tape2_address)?;
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

        store.write_segment(&address, segment_number, segment_data.clone())?;
        store.write_tape(tape_number, &address)?;
        let retrieved_data = store.read_segment(tape_number, segment_number)?;
        assert_eq!(retrieved_data, segment_data);
        Ok(())
    }

    #[test]
    fn test_get_segment_non_existent() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let tape_number = 1;
        let segment_number = 0;
        let address = Pubkey::new_unique();

        store.write_tape(tape_number, &address)?;
        let result = store.read_segment(tape_number, segment_number);
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

        store.write_segment(&address, 0, segment_data_1.clone())?;
        store.write_segment(&address, 1, segment_data_2.clone())?;
        store.write_tape(tape_number, &address)?;

        let retrieved_data_1 = store.read_segment(tape_number, 0)?;
        assert_eq!(retrieved_data_1, segment_data_1);
        let retrieved_data_2 = store.read_segment(tape_number, 1)?;
        assert_eq!(retrieved_data_2, segment_data_2);
        Ok(())
    }

    #[test]
    fn test_get_local_stats() -> Result<(), StoreError> {
        let (store, _temp_dir) = setup_store()?;
        let stats = store.read_local_stats()?;
        assert_eq!(stats.tapes, 0);
        assert_eq!(stats.segments, 0);

        let tape_number = 1;
        let address = Pubkey::new_unique();
        store.write_tape(tape_number, &address)?;
        store.write_segment(&address, 0, vec![1, 2, 3])?;
        store.write_segment(&address, 1, vec![4, 5, 6])?;

        let stats = store.read_local_stats()?;
        assert_eq!(stats.tapes, 1);
        assert_eq!(stats.segments, 2);
        assert!(stats.size_bytes > 0);
        Ok(())
    }
}
