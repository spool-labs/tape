/// Domain separation tag for spooler-vote messages.
pub const ASSIGNMENT_VOTE_DOMAIN_TAG: &[u8; 8] = b"SPOOLERV";

/// Assignment Merkle tree height. Supports up to 65,536 groups.
pub const ASSIGNMENT_TREE_HEIGHT: usize = 16;

/// Assignment vote message format version.
pub const ASSIGNMENT_VOTE_FORMAT_VERSION: u64 = 1;

/// 8 (domain) + 8 (epoch) + 32 (nonce) + 32 (hash) + 8 (format) = 88 bytes.
pub const ASSIGNMENT_VOTE_MESSAGE_SIZE: usize = 88;
