//! Stream receipt returned by stream write operations.

use tape_api::program::tapedrive::track_pda;
use tape_core::track::types::CompressedTrack;
use tape_crypto::address::Address;
use tape_crypto::Hash;

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
    /// Object ETag: the inline manifest's value hash or coded manifest's commitment
    pub etag: Hash,
}

impl StreamReceipt {
    /// Build a receipt from a stream's certified manifest track.
    pub(crate) fn from_manifest_track(track: &CompressedTrack, etag: Hash) -> Self {
        Self {
            tape: track.tape,
            manifest: track_pda(track.tape, track.track_number).0,
            manifest_track_number: track.track_number,
            etag,
        }
    }
}
