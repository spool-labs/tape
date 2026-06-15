//! Protocol request/response types for the node API.

use core::mem::size_of;

use tape_core::{
    bls::BlsSignature,
    erasure::SLICE_TREE_HEIGHT,
    spooler::GroupIndex,
};
pub use tape_core::system::VoteCandidate;
use tape_core::prelude::{EpochNumber, SpoolIndex, BlobData, TrackNumber};
use tape_core::track::types::{PackedTrack, PackedTrackProof};
use tape_core::types::SpoolBitmap;
use tape_crypto::prelude::{Address, Hash};
use wincode::containers::{Pod, Vec as WincodeVec};
use wincode::len::BincodeLen;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::api::ops::FindTrackVersion;

pub const SLICE_BYTES_LIMIT: usize = 10 * 1024 * 1024;
pub const SLICE_BODY_LIMIT: usize = size_of::<u64>()
    + SLICE_BYTES_LIMIT
    + Hash::LEN
    + size_of::<u64>()
    + (SLICE_TREE_HEIGHT * Hash::LEN);

type SliceBytes = WincodeVec<Pod<u8>, BincodeLen<SLICE_BYTES_LIMIT>>;

/// Response from the signature endpoint.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct BlsSignResponse {
    pub signature: BlsSignature,
    pub node: Address,
    pub epoch: EpochNumber,
}

/// Body for a pushed off-chain BLS vote.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct VoteRequest {
    pub signer: Address,
    pub candidate: VoteCandidate,
    pub group: GroupIndex,
    pub signature: BlsSignature,
}

/// Request for inconsistency attestation.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InconsistencyRequest {
    /// Signed proof from committee members that a node should trust.
    pub proof: InconsistencyProof,
}

/// Committee proof data for inconsistency reporting.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InconsistencyProof {
    /// Bitmap of spool positions inside the track's group that produced the proof signature.
    pub spool_bitmap: SpoolBitmap,
    /// Aggregated BLS signature over an invalidation message.
    pub signature: BlsSignature,
    /// Merkle root computed from re-encoded recovery material.
    pub observed_root: Hash,
}

/// Response from the inconsistency attestation endpoint.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct BlsInconsistencyResponse {
    pub signature: BlsSignature,
    pub node: Address,
    pub epoch: EpochNumber,
}

/// Request for sub-chunk extraction (bandwidth-optimal repair).
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct RepairRequest {
    pub helper_spool: SpoolIndex,
    pub stripes: Vec<StripeSubChunkRequest>,
}

/// Per-stripe sub-chunk extraction instructions.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct StripeSubChunkRequest {
    pub stripe: u32,
    pub sub_chunks: Vec<u32>,
}

/// Response from the node stats endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeStats {
    pub last_processed_slot: u64,
    pub blocks_processed: u64,
    pub epoch_transitions: u64,
    pub current_epoch: u64,
    pub owned_spools: u64,
    pub tracks_stored: u64,
    pub slice_payload_bytes: u64,
    pub store_disk_bytes: u64,
    pub free_disk_bytes: Option<u64>,
    pub reclaim_pending: bool,
    pub slices_stored: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub requests_total: u64,
    pub ingest_state: String,
    pub ingest_lag_slots: u64,
    pub ingest_tip_slot: u64,
}

/// Payload for slice upload requests.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SlicePayload {
    #[wincode(with = "SliceBytes")]
    pub data: Vec<u8>,
    pub leaf_hash: Hash,
    pub merkle_proof: Vec<Hash>,
}

impl SlicePayload {
    pub fn new(data: Vec<u8>, leaf_hash: Hash, merkle_proof: Vec<Hash>) -> Self {
        Self {
            data,
            leaf_hash,
            merkle_proof,
        }
    }
}

/// Request for slice synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSlicesRequest {
    pub spool_index: SpoolIndex,
    /// Last track address received, or empty to start from the beginning.
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

/// Response from slice synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSlicesResponse {
    pub entries: Vec<SyncSliceEntry>,
    /// Next cursor for pagination, or None if no more entries.
    pub next_cursor: Option<[u8; 32]>,
}

/// A single slice entry in a sync response.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSliceEntry {
    pub track_address: [u8; 32],
    #[wincode(with = "SliceBytes")]
    pub slice_data: Vec<u8>,
}

/// Request for track-data synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTracksRequest {
    pub spool_index: SpoolIndex,
    /// Last track address received, or empty to start from the beginning.
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

/// Response from track-data synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTracksResponse {
    pub entries: Vec<SyncTrackEntry>,
    /// Next cursor for pagination, or None if no more entries.
    pub next_cursor: Option<[u8; 32]>,
}

/// A single track-data entry in a sync response.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTrackEntry {
    pub track_address: [u8; 32],
    pub data: BlobData,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TrackResponse {
    pub track: PackedTrack,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct FindTrackRequest {
    pub key: Hash,
    pub version: FindTrackVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ListTracksByTapeRequest {
    pub cursor: Option<TrackNumber>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ListTracksByTapeResponse {
    pub tracks: Vec<PackedTrack>,
    pub next_cursor: Option<TrackNumber>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TrackDataResponse {
    pub data: BlobData,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct TrackProofResponse {
    pub proof: PackedTrackProof,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::encoding::EncodingProfile;
    use tape_core::erasure::GROUP_SIZE;
    use tape_core::system::VoteKind;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;

    fn address(byte: u8) -> Address {
        let mut bytes = [0u8; 32];
        bytes[0] = byte;
        Address::new(bytes)
    }

    #[test]
    fn payload_roundtrip() {
        let data = vec![0xAB; 1024];
        let leaf_hash = Hash::from([0x11; 32]);
        let proof = vec![Hash::from([0x22; 32]); SLICE_TREE_HEIGHT];

        let payload = SlicePayload::new(data.clone(), leaf_hash, proof.clone());
        let bytes = wincode::serialize(&payload).unwrap();
        let recovered: SlicePayload = wincode::deserialize(&bytes).unwrap();

        assert_eq!(recovered.data, data);
        assert_eq!(recovered.leaf_hash, leaf_hash);
        assert_eq!(recovered.merkle_proof, proof);
    }

    #[test]
    fn payload_truncated() {
        let result: Result<SlicePayload, _> = wincode::deserialize(&[0u8; 10]);
        assert!(result.is_err());
    }

    // Validates that slice payloads larger than the default wincode vector cap still roundtrip.
    #[test]
    fn payload_large() {
        let payload = SlicePayload::new(
            vec![0xAB; (4 * 1024 * 1024) + 1],
            Hash::from([0x11; 32]),
            vec![Hash::from([0x22; 32]); SLICE_TREE_HEIGHT],
        );

        let bytes = wincode::serialize(&payload).unwrap();
        let decoded: SlicePayload = wincode::deserialize(&bytes).unwrap();

        assert_eq!(decoded, payload);
    }

    // Validates that slice payloads above the configured cap are rejected on decode.
    #[test]
    fn payload_limit() {
        let payload = SlicePayload::new(
            vec![0xAB; SLICE_BYTES_LIMIT + 1],
            Hash::from([0x11; 32]),
            vec![Hash::from([0x22; 32]); SLICE_TREE_HEIGHT],
        );

        let bytes = wincode::serialize(&payload).unwrap();
        let result: Result<SlicePayload, _> = wincode::deserialize(&bytes);

        assert!(result.is_err());
    }

    // Validates that the declared body limit matches the wire encoding.
    #[test]
    fn payload_size() {
        let payload = SlicePayload::new(
            vec![0xAB; SLICE_BYTES_LIMIT],
            Hash::from([0x11; 32]),
            vec![Hash::from([0x22; 32]); SLICE_TREE_HEIGHT],
        );

        let bytes = wincode::serialize(&payload).unwrap();

        assert_eq!(bytes.len(), SLICE_BODY_LIMIT);
    }

    #[test]
    fn sign_response() {
        let resp = BlsSignResponse {
            signature: BlsSignature(G1CompressedPoint([0xAA; 32])),
            node: address(42),
            epoch: EpochNumber(100),
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: BlsSignResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn vote_request_roundtrip() {
        let req = VoteRequest {
            signer: address(7),
            candidate: VoteCandidate {
                kind: VoteKind::Snapshot,
                voting_epoch: EpochNumber(11),
                target_epoch: EpochNumber(10),
                hash: Hash::from([0x11; 32]),
            },
            group: GroupIndex(4),
            signature: BlsSignature(G1CompressedPoint([0xAB; 32])),
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: VoteRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn inconsistency() {
        let req = InconsistencyRequest {
            proof: InconsistencyProof {
                spool_bitmap: SpoolBitmap::from_indices(&[0, 3, 7, 19]),
                signature: BlsSignature(G1CompressedPoint([0xAA; 32])),
                observed_root: Hash::from([0xBB; 32]),
            },
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: InconsistencyRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);

        let resp = BlsInconsistencyResponse {
            signature: BlsSignature(G1CompressedPoint([0xCC; 32])),
            node: address(1),
            epoch: EpochNumber(50),
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: BlsInconsistencyResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn repair() {
        let req = RepairRequest {
            helper_spool: SpoolIndex(42),
            stripes: vec![
                StripeSubChunkRequest {
                    stripe: 0,
                    sub_chunks: vec![1, 2, 3],
                },
                StripeSubChunkRequest {
                    stripe: 1,
                    sub_chunks: vec![4, 5],
                },
            ],
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: RepairRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn sync_slices_request() {
        let req = SyncSlicesRequest {
            spool_index: SpoolIndex(42),
            cursor: Some([0xAA; 32]),
            limit: 100,
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: SyncSlicesRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn sync_slices_response() {
        let resp = SyncSlicesResponse {
            entries: vec![
                SyncSliceEntry {
                    track_address: [0x11; 32],
                    slice_data: vec![1, 2, 3],
                },
                SyncSliceEntry {
                    track_address: [0x22; 32],
                    slice_data: vec![4, 5, 6],
                },
            ],
            next_cursor: Some([0x22; 32]),
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSlicesResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn sync_slices_empty() {
        let resp = SyncSlicesResponse {
            entries: vec![],
            next_cursor: None,
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSlicesResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn sync_tracks_response() {
        let resp = SyncTracksResponse {
            entries: vec![SyncTrackEntry {
                track_address: [0x11; 32],
                data: BlobData::Inline(vec![1, 2, 3]),
            }],
            next_cursor: Some([0x11; 32]),
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncTracksResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn track_data_response_blob_roundtrip() {
        let resp = TrackDataResponse {
            data: BlobData::Coded(BlobEncoding {
                size: StorageUnits::from_bytes(2048),
                commitment: Hash::from([0x55; 32]),
                profile: EncodingProfile::basic_default(),
                stripe_size: StorageUnits::from_bytes(256),
                stripe_count: StripeCount(8),
                leaves: [Hash::from([0x66; 32]); GROUP_SIZE],
            }),
        };

        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: TrackDataResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

}
