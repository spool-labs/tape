use thiserror::Error;

use crate::SliceIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Error)]
pub enum EncodeError {
    #[error("too much data to encode in a single stripe/coder configuration")]
    TooMuchData,
    #[error("empty input data")]
    EmptyInput,
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
    #[error("missing helper data for slice {0}")]
    MissingHelper(SliceIndex),
}
