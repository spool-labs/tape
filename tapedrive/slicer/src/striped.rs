use super::api::Slicer;
use super::consts::{CODING_SLICES, DATA_SLICES, SLICE_COUNT};
use super::errors::{DecodeError, EncodeError};
use super::reed_solomon::ReedSolomonCoder;
use super::types::{Blob, Slice};

/// A striped slicer (stub) that would split the blob into multiple stripes.
/// Each stripe is encoded into SLICE_COUNT slices and appended to the corresponding output slices.
/// This keeps per-stripe memory bounded at the cost of multiple RS passes.
///
/// Not implemented yet.
pub struct StripedSlicer(ReedSolomonCoder);

impl Default for StripedSlicer {
    fn default() -> Self {
        Self(ReedSolomonCoder::new(DATA_SLICES, CODING_SLICES))
    }
}

impl Slicer for StripedSlicer {
    const MAX_DATA_SIZE: usize = usize::MAX;
    const DATA_OUTPUT_SLICES: usize = DATA_SLICES;
    const CODING_OUTPUT_SLICES: usize = CODING_SLICES;

    fn encode(&mut self, _blob: Blob) -> Result<[Slice; SLICE_COUNT], EncodeError> {
        // TODO: implement multi-stripe encode
        todo!()
    }

    fn decode(
        &mut self,
        _slices: &[Option<Slice>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError> {
        // TODO: implement multi-stripe decode
        todo!()
    }
}
