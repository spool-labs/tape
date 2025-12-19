use thiserror::Error;

pub mod reed_solomon;


pub const TOTAL_SLICES: usize = 1024;
pub const DATA_SLICES: usize = 683;
pub const CODING_SLICES: usize = 341;
pub const MAX_SLICE_SIZE: usize = 32 * 1024 * 1024;  // 32 MiB
pub const MERKLE_HEIGHT: usize = 10;  // For 1024 capacity
pub const MAX_BLOB_SIZE: usize = DATA_SLICES * MAX_SLICE_SIZE;  // ~21.78 GiB


#[derive(Error, Debug)]
pub enum ShredError {
    #[error("blob too large: {0} bytes exceeds max {MAX_BLOB_SIZE}")]
    BlobTooLarge(usize),
    #[error("blob empty")]
    EmptyBlob,
    #[error("RS encoding failed: {0}")]
    RsError(String),
    //#[error("Merkle error: {0}")]
    //Merkle(#[from] MerkleError),
}

#[derive(Error, Debug)]
pub enum DeshredError {
    #[error("not enough slices: need at least {DATA_SLICES}")]
    NotEnoughSlices,
    #[error("invalid slice size mismatch")]
    InvalidSliceSize,
    #[error("invalid Merkle proof")]
    InvalidProof,
    #[error("invalid padding")]
    InvalidPadding,
    #[error("RS decoding failed: {0}")]
    RsError(String),
    #[error("Merkle rebuild failed")]
    MerkleRebuildFailed,
}

#[derive(Debug)]
pub struct RawSlices {
    pub data: Vec<Vec<u8>>,
    pub coding: Vec<Vec<u8>>,
}
