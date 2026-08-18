//! Protocol request/response types for the node API.

use core::mem::size_of;

use tape_core::{
    bls::BlsSignature,
    erasure::{SLICE_TREE_HEIGHT, SUB_LEAF_BYTES},
    spooler::GroupIndex,
    challenge::ProofOfAccess,
    challenge::proof::SampleProof,
    track::blob::SubLeafProof,
};
pub use tape_core::system::VoteCandidate;
use tape_core::prelude::{BlobData, EpochNumber, SpoolIndex, TrackNumber};
use tape_core::types::RoundNumber;
use tape_core::track::types::{PackedTrack, PackedTrackProof};
use tape_core::types::{ContentType, SlotNumber, SpoolBitmap, StorageUnits};
use tape_api::instruction::TRACK_WRITE_MAX_BYTES;
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

/// Fixed-size leaf bound prevents challenge-response payload inflation.
type SampleLeafBytes = WincodeVec<Pod<u8>, BincodeLen<SUB_LEAF_BYTES>>;

/// Matches the maximum payload accepted by the on-chain write instruction.
type InlinePayloadBytes = WincodeVec<Pod<u8>, BincodeLen<TRACK_WRITE_MAX_BYTES>>;

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

/// Disk usage for one physical storage volume.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VolumeStats {
    pub name: String,
    pub store_disk_bytes: u64,
    pub free_disk_bytes: Option<u64>,
}

/// Response from the node stats endpoint.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeStats {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub last_processed_slot: u64,
    #[serde(default)]
    pub blocks_processed: u64,
    #[serde(default)]
    pub epoch_transitions: u64,
    #[serde(default)]
    pub current_epoch: u64,
    #[serde(default)]
    pub owned_spools: u64,
    #[serde(default)]
    pub tracks_stored: u64,
    #[serde(default)]
    pub slice_payload_bytes: u64,
    #[serde(default)]
    pub store_disk_bytes: u64,
    #[serde(default)]
    pub free_disk_bytes: Option<u64>,
    #[serde(default)]
    pub disk_volumes: Vec<VolumeStats>,
    #[serde(default)]
    pub reclaim_pending: bool,
    #[serde(default)]
    pub slices_stored: u64,
    #[serde(default)]
    pub bytes_uploaded: u64,
    #[serde(default)]
    pub bytes_downloaded: u64,
    #[serde(default)]
    pub sync_bytes_fetched: u64,
    #[serde(default)]
    pub repair_bytes_fetched: u64,
    #[serde(default)]
    pub recover_bytes_fetched: u64,
    #[serde(default)]
    pub requests_total: u64,
    #[serde(default)]
    pub ingest_state: String,
    #[serde(default)]
    pub ingest_lag_slots: u64,
    #[serde(default)]
    pub ingest_tip_slot: u64,
    #[serde(default)]
    pub ingest_fetch_slot: u64,
    #[serde(default)]
    pub ingest_queue_len: u64,
    #[serde(default)]
    pub bootstrap_done: bool,
    #[serde(default)]
    pub bootstrap_phase: String,
    #[serde(default)]
    pub bootstrap_current_slot: u64,
    #[serde(default)]
    pub bootstrap_target_slot: u64,
    #[serde(default)]
    pub fee_payer_lamports: Option<u64>,
}

/// Project the wire stats onto the dashboard's per-node stats.
impl From<&NodeStats> for tape_observe_api::NodeStats {
    fn from(s: &NodeStats) -> Self {
        Self {
            version: s.version.clone(),
            owned_spools: s.owned_spools,
            tracks_stored: s.tracks_stored,
            slices_stored: s.slices_stored,
            slice_payload_bytes: s.slice_payload_bytes,
            store_disk_bytes: s.store_disk_bytes,
            free_disk_bytes: s.free_disk_bytes.unwrap_or(0),
            current_epoch: s.current_epoch,
            ingest_state: s.ingest_state.clone(),
            ingest_lag_slots: s.ingest_lag_slots,
            reclaim_pending: s.reclaim_pending,
            blocks_processed: s.blocks_processed,
            bootstrap_ready: s.bootstrap_done,
            bootstrap_behind_slots: if s.bootstrap_done {
                0
            } else {
                s.bootstrap_target_slot.saturating_sub(s.bootstrap_current_slot)
            },
            fee_payer_lamports: s.fee_payer_lamports,
            sync_bytes: s.sync_bytes_fetched,
            repair_bytes: s.repair_bytes_fetched,
            recover_bytes: s.recover_bytes_fetched,
            upload_bytes: s.bytes_uploaded,
        }
    }
}

/// Wire representation of a storage-challenge proof.
///
/// Coded proofs include leaf bytes to prove access to the data rather than a
/// cached hash. Inline proofs include the registered payload.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum SampleProofPayload {
    Coded {
        sub_leaf: u64,
        #[wincode(with = "SampleLeafBytes")]
        leaf: Vec<u8>,
        sub_proof: Vec<Hash>,
    },
    Inline {
        #[wincode(with = "InlinePayloadBytes")]
        payload: Vec<u8>,
    },
}

impl From<SampleProof> for SampleProofPayload {
    fn from(proof: SampleProof) -> Self {
        match proof {
            SampleProof::Coded { sub_leaf, proof } => Self::Coded {
                sub_leaf,
                leaf: proof.sub_leaf,
                sub_proof: proof.sub_proof,
            },
            SampleProof::Inline { payload } => Self::Inline { payload },
        }
    }
}

impl From<SampleProofPayload> for SampleProof {
    fn from(payload: SampleProofPayload) -> Self {
        match payload {
            SampleProofPayload::Coded {
                sub_leaf,
                leaf,
                sub_proof,
            } => Self::Coded {
                sub_leaf,
                proof: SubLeafProof {
                    sub_leaf: leaf,
                    sub_proof,
                },
            },
            SampleProofPayload::Inline { payload } => Self::Inline { payload },
        }
    }
}

/// Wire representation of a challenged owner's answer for one round.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ProofOfAccessPayload {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
    pub track: Address,
    pub proof: SampleProofPayload,
    pub signature: BlsSignature,
}

impl From<ProofOfAccess> for ProofOfAccessPayload {
    fn from(answer: ProofOfAccess) -> Self {
        Self {
            epoch: answer.epoch,
            group: answer.group,
            round: answer.round,
            spool: answer.spool,
            block: answer.block,
            track: answer.track,
            proof: answer.proof.into(),
            signature: answer.signature,
        }
    }
}

impl From<ProofOfAccessPayload> for ProofOfAccess {
    fn from(payload: ProofOfAccessPayload) -> Self {
        Self {
            epoch: payload.epoch,
            group: payload.group,
            round: payload.round,
            spool: payload.spool,
            block: payload.block,
            track: payload.track,
            proof: payload.proof.into(),
            signature: payload.signature,
        }
    }
}

/// Wire representation of an observer's attestation for a round.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct AttestationPayload {
    pub epoch: EpochNumber,
    pub group: GroupIndex,
    pub round: RoundNumber,
    pub spool: SpoolIndex,
    pub block: Hash,
    pub signer: Address,
    pub signature: BlsSignature,
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
    /// Where the last page left off, or nothing to start from the beginning.
    ///
    /// Opaque: the server mints it and the client only ever hands it back. A
    /// mark the server did not mint, or one from a different opening of its
    /// volume, restarts the scan rather than resuming into a layout that is not
    /// there.
    pub cursor: Option<Vec<u8>>,
    pub limit: u32,
}

/// Response from slice synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSlicesResponse {
    pub entries: Vec<SyncSliceEntry>,
    /// Where to resume, or nothing when the scan is done.
    ///
    /// A page boundary rather than a row: a client resuming from it may be
    /// handed slices it already holds, which is safe because a slice it already
    /// has is skipped rather than rewritten.
    pub next_cursor: Option<Vec<u8>>,
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
    /// Where the last page left off, or nothing to start from the beginning.
    ///
    /// Opaque: the server mints it and the client only ever hands it back. A
    /// mark the server did not mint, or one from a different opening of its
    /// volume, restarts the scan rather than resuming into a layout that is not
    /// there.
    pub cursor: Option<Vec<u8>>,
    pub limit: u32,
}

/// Response from track-data synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTracksResponse {
    pub entries: Vec<SyncTrackEntry>,
    /// Where to resume, or nothing when the scan is done.
    ///
    /// A page boundary rather than a row: a client resuming from it can be
    /// handed rows it already has, which is safe because taking a track twice
    /// is taking it once.
    pub next_cursor: Option<Vec<u8>>,
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
pub struct ListObjectsRequest {
    pub prefix: Vec<u8>,
    pub delimiter: Option<Vec<u8>>,
    pub cursor: Option<Vec<u8>>,
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ObjectListItem {
    pub name: Vec<u8>,
    pub size: StorageUnits,
    pub etag: Hash,
    pub block_time: Option<i64>,
    pub slot: SlotNumber,
    pub data_tape: Address,
    pub track_number: TrackNumber,
    pub kind: u64,
    pub content_type: ContentType,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ListObjectsResponse {
    pub objects: Vec<ObjectListItem>,
    pub common_prefixes: Vec<Vec<u8>>,
    pub next_cursor: Option<Vec<u8>>,
    pub is_truncated: bool,
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
    use tape_core::erasure::{GROUP_SIZE, SUB_TREE_HEIGHT};
    use tape_core::system::VoteKind;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;

    /// A distinct non-zero hash per level. Zero is the seed the empty-subtree
    /// roots derive from, so it is the one 32-byte value with a meaning of its
    /// own and a poor stand-in for a path element.
    fn path() -> Vec<Hash> {
        (0..SUB_TREE_HEIGHT)
            .map(|level| Hash::from([level as u8 + 1; 32]))
            .collect()
    }

    /// Raise a declared length wherever the encoding put it.
    fn inflate(encoded: &mut [u8], declared: u64) {
        let claim = declared.to_le_bytes();
        let at = encoded
            .windows(claim.len())
            .position(|window| window == claim)
            .expect("a length field");
        encoded[at..at + claim.len()].copy_from_slice(&(declared + 1).to_le_bytes());
    }

    // a proof of either shape comes back off the wire as what went on it
    #[test]
    fn proof_round_trip() {
        let coded = SampleProof::Coded {
            sub_leaf: 3,
            proof: SubLeafProof {
                sub_leaf: (0..SUB_LEAF_BYTES).map(|byte| byte as u8 ^ 0x5A).collect(),
                sub_proof: path(),
            },
        };
        let encoded = wincode::serialize(&SampleProofPayload::from(coded.clone())).unwrap();
        let decoded: SampleProofPayload = wincode::deserialize(&encoded).unwrap();
        assert_eq!(SampleProof::from(decoded), coded);

        let inline = SampleProof::Inline {
            payload: b"a small object".to_vec(),
        };
        let encoded = wincode::serialize(&SampleProofPayload::from(inline.clone())).unwrap();
        let decoded: SampleProofPayload = wincode::deserialize(&encoded).unwrap();
        assert_eq!(SampleProof::from(decoded), inline);
    }

    // the oversize claim comes from a peer, so a leaf longer than one chunk is
    // refused on decode, before anything allocates
    #[test]
    fn oversize_leaf() {
        let mut encoded = wincode::serialize(&SampleProofPayload::Coded {
            sub_leaf: 0,
            leaf: (0..SUB_LEAF_BYTES).map(|byte| byte as u8 ^ 0x5A).collect(),
            sub_proof: path(),
        })
        .unwrap();
        inflate(&mut encoded, SUB_LEAF_BYTES as u64);

        assert!(wincode::deserialize::<SampleProofPayload>(&encoded).is_err());
    }

    // an inline payload is bounded by what the chain accepts, not by what the
    // SDK chooses to write
    #[test]
    fn oversize_inline() {
        let mut encoded = wincode::serialize(&SampleProofPayload::Inline {
            payload: vec![0x5A; TRACK_WRITE_MAX_BYTES],
        })
        .unwrap();
        inflate(&mut encoded, TRACK_WRITE_MAX_BYTES as u64);

        assert!(wincode::deserialize::<SampleProofPayload>(&encoded).is_err());

        // At the cap it still decodes.
        let ok = wincode::serialize(&SampleProofPayload::Inline {
            payload: vec![0x5A; TRACK_WRITE_MAX_BYTES],
        })
        .unwrap();
        assert!(wincode::deserialize::<SampleProofPayload>(&ok).is_ok());
    }

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

    // each transfer counter reaches the dashboard's field of the same meaning
    #[test]
    fn transfer_counters_projected() {
        let wire = NodeStats {
            sync_bytes_fetched: 11,
            repair_bytes_fetched: 22,
            recover_bytes_fetched: 33,
            bytes_uploaded: 44,
            ..NodeStats::default()
        };

        let stats = tape_observe_api::NodeStats::from(&wire);

        assert_eq!(stats.sync_bytes, 11);
        assert_eq!(stats.repair_bytes, 22);
        assert_eq!(stats.recover_bytes, 33);
        assert_eq!(stats.upload_bytes, 44);
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
            cursor: Some(vec![0xAA; 32]),
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
            next_cursor: Some(vec![0x22; 32]),
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
            next_cursor: Some(vec![0x11; 32]),
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
