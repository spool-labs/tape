use thiserror::Error;

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
