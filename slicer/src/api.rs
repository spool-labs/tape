use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Slice};
use super::consts::SLICE_COUNT;

pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;
    const DATA_OUTPUT_SLICES: usize;
    const PARITY_OUTPUT_SLICES: usize;

    /// Encode a blob into SLICE_COUNT slices (DATA_SLICES data + PARITY_SLICES parity).
    fn encode(&mut self, blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError>;

    /// Decode slices back into the original blob.
    /// Requires at least DATA_SLICES valid slices for reconstruction.
    fn decode(
        &mut self,
        slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError>;
}
