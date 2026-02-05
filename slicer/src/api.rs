use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Slice};
use super::consts::SPOOL_GROUP_SIZE;

pub trait Slicer {
    /// Maximum data size this slicer can handle.
    const MAX_DATA_SIZE: usize;

    /// Data slices (k) for reconstruction.
    fn k(&self) -> usize;

    /// Parity slices (m).
    fn m(&self) -> usize;

    /// Encode a blob into SPOOL_GROUP_SIZE slices.
    fn encode(&mut self, blob: Blob) -> Result<[Slice; SPOOL_GROUP_SIZE], EncodeError>;

    /// Decode slices back into the original blob.
    /// Requires at least k valid slices for reconstruction.
    fn decode(
        &mut self,
        slices: &[Option<Slice>; SPOOL_GROUP_SIZE],
    ) -> Result<Blob, DecodeError>;
}
