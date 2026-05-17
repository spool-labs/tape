/// Domain separation tag for snapshot certification.
pub const SNAPSHOT_SIGN_DOMAIN_TAG: &[u8; 8] = b"SNAPSIGN";

/// Snapshot vote message format version.
pub const SNAPSHOT_SIGN_FORMAT_VERSION: u64 = 1;

/// Size of the snapshot certification message in bytes.
/// 8 (domain) + 8 (epoch) + 32 (snapshot hash) + 8 (format) = 56 bytes.
pub const SNAPSHOT_SIGN_MESSAGE_SIZE: usize = 56;
