use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Shard};
use super::consts::TOTAL_SLICES;

pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;
    const DATA_OUTPUT_SHREDS: usize;
    const CODING_OUTPUT_SHREDS: usize;

    fn encode(&mut self, blob: Blob) -> Result<[Shard; TOTAL_SLICES], EncodeError>;

    fn decode(
        &mut self,
        shards: &[Option<Shard>; TOTAL_SLICES],
    ) -> Result<Blob, DecodeError>;
}
