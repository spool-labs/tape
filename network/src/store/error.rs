use thiserror::Error;
use super::column_family::ColumnFamily;

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
    #[error("Sectors column family not found")]
    SectorsCfNotFound,
    #[error("Merkle layers column family not found")]
    MerkleLayersCfNotFound,
    #[error("Tape stats column family not found")]
    TapeStatsCfNotFound,
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

impl From<&ColumnFamily> for StoreError {
    fn from(value: &ColumnFamily) -> Self {
        match value {
            ColumnFamily::TapeByNumber => StoreError::TapeByNumberCfNotFound,
            ColumnFamily::TapeByAddress => StoreError::TapeByAddressCfNotFound,
            ColumnFamily::TapeStats => StoreError::TapeStatsCfNotFound,
            ColumnFamily::Sectors => StoreError::SectorsCfNotFound,
            ColumnFamily::MerkleLayers => StoreError::MerkleLayersCfNotFound,
            ColumnFamily::Health => StoreError::HealthCfNotFound,
        }
    }
}
