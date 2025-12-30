/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per slice.
pub const MERKLE_HEIGHT: usize = 10;

/// Total slices (data + coding) produced by Reed-Solomon encoding.
/// Each blob is encoded into exactly SLICE_COUNT slices.
/// Slice index N of any blob is stored in spool N on the network.
pub const SLICE_COUNT: usize = 1 << MERKLE_HEIGHT; // 1024

/// BFT fault tolerance parameter.
/// Derived from SLICE_COUNT using the formula: f = (n - 1) / 3
/// This is the maximum number of faulty/missing slices we can tolerate.
pub const F: usize = (SLICE_COUNT - 1) / 3;

/// Number of coding (parity) slices per blob.
/// With 3f+1 layout: coding = f slices.
pub const CODING_SLICES: usize = F;

/// Number of data slices per blob.
/// With 3f+1 layout: data = 2f+1 slices.
/// This is also the minimum number of slices needed for reconstruction.
pub const DATA_SLICES: usize = SLICE_COUNT - CODING_SLICES;
