//! Stream receipt returned by stream write operations.

use solana_sdk::pubkey::Pubkey;

use tape_core::types::TrackNumber;

/// Returned by `write_bytes` and `write_stream`.
#[derive(Debug, Clone)]
pub struct StreamReceipt {
    /// The tape PDA address.
    pub tape: Pubkey,
    /// The manifest track address for the stored stream.
    pub manifest: Pubkey,
    /// Track number of the manifest on the tape.
    pub manifest_track_number: TrackNumber,
}
