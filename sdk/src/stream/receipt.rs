//! Stream receipt returned by stream write operations.

use tape_crypto::address::Address;

use tape_core::types::TrackNumber;

/// Returned by `write_bytes` and `write_stream`.
#[derive(Debug, Clone)]
pub struct StreamReceipt {
    /// The tape PDA address.
    pub tape: Address,
    /// The manifest track address for the stored stream.
    pub manifest: Address,
    /// Track number of the manifest on the tape.
    pub manifest_track_number: TrackNumber,
}
