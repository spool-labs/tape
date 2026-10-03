//! Writing, deleting and finding the list's unnamed inline tracks

use std::future::Future;

use tape_core::track::data::{track_key, BlobDataSlice};
use tape_core::track::types::CompressedTrack;
use tape_core::types::TrackNumber;
use tape_crypto::address::Address;
use tape_crypto::Hash;
use tape_sdk::track::write::UNNAMED_TRACK;

use crate::chain::error::ChainError;
use crate::records::rows::ListTape;

/// Where an inline write landed
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WrittenTrack {
    pub track: Address,
    pub track_number: TrackNumber,
}

/// Writes, deletes and finds unnamed inline tracks on the list's tapes
pub trait TrackWriter {
    /// Write bytes as one unnamed inline track on the rack or a blacklist tape
    fn write_inline(
        &self,
        tape: &ListTape,
        bytes: &[u8],
    ) -> impl Future<Output = Result<WrittenTrack, ChainError>> + Send;

    /// Delete a track on the tape, `ChainError::TrackNotFound` when no peer holds it
    fn delete_track(
        &self,
        tape: &ListTape,
        track: Address,
    ) -> impl Future<Output = Result<(), ChainError>> + Send;

    /// The track at a number, `None` while no peer holds one
    fn landed_track(
        &self,
        tape: Address,
        track_number: TrackNumber,
    ) -> impl Future<Output = Result<Option<CompressedTrack>, ChainError>> + Send;
}

/// The key the chain gives an unnamed inline track holding these bytes
pub fn inline_key(bytes: &[u8]) -> Hash {
    track_key(UNNAMED_TRACK, &BlobDataSlice::Inline(bytes))
}
