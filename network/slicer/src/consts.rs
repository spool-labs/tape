/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per slice.
pub const MERKLE_HEIGHT: usize = 10;

// Re-export erasure coding constants from tape-core.
// These are derived using BFT tolerance functions (max_faulty, min_correct).
pub use tape_core::erasure::{DATA_SLICES, PARITY_SLICES, SLICE_COUNT};

/// BFT fault tolerance parameter f = max_faulty(SLICE_COUNT).
/// This is the maximum number of faulty/missing slices we can tolerate.
/// Alias for PARITY_SLICES for code clarity in BFT contexts.
pub const F: usize = PARITY_SLICES;

/// Number of coding (parity) slices per blob.
/// Alias for PARITY_SLICES.
pub const CODING_SLICES: usize = PARITY_SLICES;
