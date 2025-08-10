use std::path::Path;
use std::sync::Arc;
use rocksdb::{BoundColumnFamily, DB, Options};
use super::{
    consts::*,
    layout::{ColumnFamily, create_cf_descriptors},
    error::StoreError,
};

pub struct TapeStore {
    pub db: DB,
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
}

impl Drop for TapeStore {
    fn drop(&mut self) {
        // RocksDB handles cleanup automatically
    }
}

