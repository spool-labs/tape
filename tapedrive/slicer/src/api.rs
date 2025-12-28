use super::errors::{EncodeError, DecodeError};
use super::types::{Blob, Shard};
use super::consts::SLICE_COUNT;

pub trait Slicer: Default {
    const MAX_DATA_SIZE: usize;
    const DATA_OUTPUT_SHREDS: usize;
    const CODING_OUTPUT_SHREDS: usize;

    fn encode(&mut self, blob: Blob) -> Result<[Shard; SLICE_COUNT], EncodeError>;

    fn decode(
        &mut self,
        shards: &[Option<Shard>; SLICE_COUNT],
    ) -> Result<Blob, DecodeError>;
}
