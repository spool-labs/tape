use thiserror::Error;

use crate::SliceIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    #[error("too much data to encode in a single stripe/coder configuration")]
    TooMuchData,
    #[error("empty input data")]
    EmptyInput,
    #[error("encoded chunks differ in size")]
    UnevenChunks,
    #[error("an encode thread panicked")]
    Worker,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("not enough slices to reconstruct (need at least DATA_SLICES)")]
    NotEnoughSlices,
    #[error("too much data for configured limits")]
    TooMuchData,
    #[error("invalid padding in recovered data")]
    BadEncoding,
    #[error("invalid layout or inconsistent slices")]
    InvalidLayout,
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum RepairError {
    #[error("not enough helpers: need {needed}, have {available}")]
    NotEnoughHelpers { needed: u32, available: u32 },
    #[error("invalid slice index")]
    InvalidSlice,
    #[error("invalid layout: {0}")]
    InvalidLayout(String),
    #[error("clay error: {0}")]
    Clay(String),
    #[error("helper {slice} sent {actual} bytes, expected {expected}")]
    HelperLength { slice: SliceIndex, expected: usize, actual: usize },
    #[error("{} stripes lack a plan or helper data", .0.len())]
    ShortStripes(Vec<u32>),
}
