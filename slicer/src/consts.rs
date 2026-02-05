/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per slice.
/// With SPOOL_GROUP_SIZE=20, we use height 5 (2^5 = 32 leaves, 20 used).
pub const MERKLE_HEIGHT: usize = 5;

// Re-export erasure coding constants from tape-core.
pub use tape_core::erasure::{DATA_SLICES, PARITY_SLICES, SPOOL_GROUP_SIZE};
