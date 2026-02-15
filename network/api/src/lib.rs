//! Shared route constants, URL builders, and request/response types for the node API.

pub mod routes;
pub mod signed;
pub mod types;

/// Merkle tree height for blob encoding.
/// 2^5 = 32 leaves (20 used = SPOOL_GROUP_SIZE).
pub const MERKLE_HEIGHT: usize = tape_core::erasure::COMMITMENT_TREE_HEIGHT;

/// Content type for binary request/response bodies.
pub const BINARY_CONTENT: &str = "application/octet-stream";

/// Content type for JSON responses.
pub const CONTENT_TYPE_JSON: &str = "application/json";

// Re-export route constants and URL builders.
pub use routes::*;

// Re-export protocol types.
pub use signed::SignedMessage;
pub use types::*;
