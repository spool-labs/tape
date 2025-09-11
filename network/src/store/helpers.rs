use super::{TapeStore, StoreError, consts::*};
use std::{env, sync::Arc};

pub fn primary() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
    std::fs::create_dir_all(&db_primary).map_err(StoreError::IoError)?;
    TapeStore::new(&db_primary)
}

pub fn secondary_mine(tape_store_primary_db: &str, tape_store_secondary_db_mine: &str) -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(tape_store_primary_db);
    let db_secondary = current_dir.join(tape_store_secondary_db_mine);
    std::fs::create_dir_all(&db_secondary).map_err(StoreError::IoError)?;
    TapeStore::new_secondary(&db_primary, &db_secondary)
}

pub fn secondary_web(tape_store_primary_db: &str, tape_store_secondary_db_web: &str) -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(tape_store_primary_db);
    let db_secondary = current_dir.join(tape_store_secondary_db_web);
    std::fs::create_dir_all(&db_secondary).map_err(StoreError::IoError)?;
    TapeStore::new_secondary(&db_primary, &db_secondary)
}

pub fn read_only() -> Result<TapeStore, StoreError> {
    let current_dir = env::current_dir().map_err(StoreError::IoError)?;
    let db_primary = current_dir.join(TAPE_STORE_PRIMARY_DB);
    TapeStore::new_read_only(&db_primary)
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
