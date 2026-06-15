//! Object-layer projection rules shared by replay and node indexes.

use crate::track::blob::BlobEncoding;
use crate::track::types::CompressedTrack;
use tape_crypto::Hash;

/// Return the object ETag for a materialized track.
///
/// Coded tracks use the erasure commitment; inline tracks use the value hash
/// authenticated in the compressed track state.
pub fn object_etag(track: &CompressedTrack, blob: Option<&BlobEncoding>) -> Hash {
    blob.map(|blob| blob.commitment)
        .unwrap_or(track.value_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::EncodingProfile;
    use crate::erasure::GROUP_SIZE;
    use crate::spooler::GroupIndex;
    use crate::track::types::{TrackKind, TrackState};
    use crate::types::{StorageUnits, StripeCount};
    use tape_crypto::address::Address;

    fn track(value_hash: Hash) -> CompressedTrack {
        CompressedTrack {
            tape: Address::from([1u8; 32]),
            track_number: 1u64.into(),
            key: Hash::from([2u8; 32]),
            kind: TrackKind::Inline as u64,
            state: TrackState::Certified as u64,
            size: StorageUnits::from_bytes(17),
            group: GroupIndex::from(0),
            value_hash,
        }
    }

    #[test]
    fn inline_etag_uses_value_hash() {
        let value_hash = Hash::from([3u8; 32]);
        assert_eq!(object_etag(&track(value_hash), None), value_hash);
    }

    #[test]
    fn coded_etag_uses_blob_commitment() {
        let value_hash = Hash::from([3u8; 32]);
        let commitment = Hash::from([4u8; 32]);
        let blob = BlobEncoding {
            size: StorageUnits::from_bytes(17),
            commitment,
            profile: EncodingProfile::default(),
            stripe_size: StorageUnits::from_bytes(8),
            stripe_count: StripeCount(1),
            leaves: [Hash::default(); GROUP_SIZE],
        };

        assert_eq!(object_etag(&track(value_hash), Some(&blob)), commitment);
    }
}
