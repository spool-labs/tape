use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Slice};
use super::consts::SPOOL_GROUP_SIZE;

pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;
    const DATA_OUTPUT_SLICES: usize;
    const PARITY_OUTPUT_SLICES: usize;

    /// Encode a blob into SPOOL_GROUP_SIZE slices (DATA_SLICES data + PARITY_SLICES parity).
    fn encode(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError>;

    /// Decode slices back into the original blob.
    /// Requires at least DATA_SLICES valid slices for reconstruction.
    fn decode(
        &mut self,
        slices: &[Option<Slice>; SPOOL_GROUP_SIZE],
    ) -> Result<Blob, DecodeError>;
}
