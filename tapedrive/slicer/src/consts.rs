/// Merkle tree height for a blob commitment.
/// There are 2^MERKLE_HEIGHT leaves; one leaf per shard.
pub const MERKLE_HEIGHT : usize = 10;

/// Total slices (data + coding) produced by Reed–Solomon per stripe.
/// Must match on-chain apportionment logic.
pub const SLICE_COUNT  : usize = 1 << MERKLE_HEIGHT; // 1024

/// Set f for the 3f+1 layout that tolerates up to f failures.
/// For SLICE_COUNT = 3f + 1, f = (SLICE_COUNT - 1)/3.
pub const F             : usize = (SLICE_COUNT - 1) / 3;

/// Number of coding slices (parity) per stripe.
/// With 3f+1 layout, coding = f and data = 2f + 1.
pub const CODING_SLICES : usize = F;                            // 341 for 1024 total
pub const DATA_SLICES   : usize = SLICE_COUNT - CODING_SLICES; // 683 for 1024 total
