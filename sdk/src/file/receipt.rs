//! File receipt returned by write_file.

use solana_sdk::pubkey::Pubkey;

use tape_core::types::TrackNumber;

/// Returned by `write_file`. The manifest track address is the file's handle.
#[derive(Debug, Clone)]
pub struct FileReceipt {
    /// The tape PDA address.
    pub tape: Pubkey,
    /// The manifest track address — this is the file's identifier.
    pub manifest: Pubkey,
    /// Track number of the manifest on the tape.
    pub manifest_track_number: TrackNumber,
}
