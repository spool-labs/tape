//! Protocol request/response types for the node API.

use core::mem::size_of;

use tape_core::{
    bls::BlsSignature,
    erasure::SLICE_TREE_HEIGHT,
    spooler::GroupIndex,
};
pub use tape_core::system::VoteCandidate;
use tape_core::prelude::{BlobData, EpochNumber, SpoolIndex, TrackNumber};
use tape_core::track::types::{PackedTrack, PackedTrackProof};
use tape_core::types::{ContentType, RoundNumber, SlotNumber, SpoolBitmap, StorageUnits};
use bytes::Bytes;
use tape_crypto::prelude::{Address, Hash};
use wincode::containers::{Pod, Vec as WincodeVec};
use wincode::len::BincodeLen;
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::api::ops::FindTrackVersion;

/// Most tracks one sync request asks for
pub const SYNC_MAX_TRACKS: usize = 1_000;

/// Most slice bytes one sync response carries, past its first slice
pub const SYNC_MAX_BYTES: StorageUnits = StorageUnits(8 * 1024 * 1024);

/// Bytes the wire body puts ahead of the slice bytes
pub const SLICE_BODY_HEADROOM: usize = size_of::<u64>();

/// Bytes the wire body puts after the slice bytes
pub const SLICE_BODY_TAILROOM: usize = Hash::LEN + size_of::<u64>() + SLICE_TREE_HEIGHT * Hash::LEN;

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
    pub proof: InconsistencyProof,
}

/// Committee proof data for inconsistency reporting.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InconsistencyProof {
    pub spool_bitmap: SpoolBitmap,
    pub signature: BlsSignature,
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
    pub store_engine: String,
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
    pub store_data_bytes: u64,
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
    #[serde(default)]
    pub restarts: u64,
}

/// Project the wire stats onto the dashboard's per-node stats.
impl From<&NodeStats> for tape_observe_api::NodeStats {
    fn from(s: &NodeStats) -> Self {
        Self {
            version: s.version.clone(),
            store_engine: s.store_engine.clone(),
            owned_spools: s.owned_spools,
            tracks_stored: s.tracks_stored,
            slices_stored: s.slices_stored,
            slice_payload_bytes: s.slice_payload_bytes,
            store_disk_bytes: s.store_disk_bytes,
            store_data_bytes: s.store_data_bytes,
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
            restarts: s.restarts,
        }
    }
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


impl SlicePayload {
    /// Write the wire form around slice bytes held after a headroom gap, in place
    pub fn frame(
        mut body: Vec<u8>,
        leaf_hash: Hash,
        merkle_proof: &[Hash],
    ) -> Result<Vec<u8>, String> {
        let data_len = body
            .len()
            .checked_sub(SLICE_BODY_HEADROOM)
            .ok_or_else(|| "slice body has no room for its length".to_string())?;
        if data_len > SLICE_BYTES_LIMIT {
            return Err(format!("slice of {data_len} bytes is over the limit"));
        }
        body[..SLICE_BODY_HEADROOM].copy_from_slice(&(data_len as u64).to_le_bytes());
        body.reserve_exact(Hash::LEN + size_of::<u64>() + merkle_proof.len() * Hash::LEN);
        body.extend_from_slice(leaf_hash.as_ref());
        body.extend_from_slice(&(merkle_proof.len() as u64).to_le_bytes());
        for hash in merkle_proof {
            body.extend_from_slice(hash.as_ref());
        }
        Ok(body)
    }
}

/// A slice push body read in place; its slice bytes point into the request body
#[derive(Clone, Debug)]
pub struct SliceBody {
    pub data: Bytes,
    pub leaf_hash: Hash,
    pub merkle_proof: Vec<Hash>,
}

impl SliceBody {
    /// Read the wire form of a slice payload without copying its slice bytes
    pub fn parse(body: &Bytes) -> Result<Self, String> {
        let mut at = 0;
        let data_len = read_len(body, &mut at)?;
        if data_len > SLICE_BYTES_LIMIT {
            return Err(format!("slice of {data_len} bytes is over the limit"));
        }
        let data = take(body, &mut at, data_len)?;
        let leaf_hash = read_hash(body, &mut at)?;
        let proof_len = read_len(body, &mut at)?;
        if proof_len > SLICE_TREE_HEIGHT {
            return Err(format!("proof of {proof_len} hashes is over the limit"));
        }
        let mut merkle_proof = Vec::with_capacity(proof_len);
        for _ in 0..proof_len {
            merkle_proof.push(read_hash(body, &mut at)?);
        }
        if at != body.len() {
            return Err("slice body has trailing bytes".into());
        }
        Ok(Self {
            data,
            leaf_hash,
            merkle_proof,
        })
    }
}

fn take(body: &Bytes, at: &mut usize, len: usize) -> Result<Bytes, String> {
    let end = at
        .checked_add(len)
        .filter(|end| *end <= body.len())
        .ok_or_else(|| "slice body ends early".to_string())?;
    let taken = body.slice(*at..end);
    *at = end;
    Ok(taken)
}

fn read_len(body: &Bytes, at: &mut usize) -> Result<usize, String> {
    let bytes = take(body, at, size_of::<u64>())?;
    let mut raw = [0u8; size_of::<u64>()];
    raw.copy_from_slice(&bytes);
    usize::try_from(u64::from_le_bytes(raw)).map_err(|error| error.to_string())
}

fn read_hash(body: &Bytes, at: &mut usize) -> Result<Hash, String> {
    let bytes = take(body, at, Hash::LEN)?;
    let mut raw = [0u8; Hash::LEN];
    raw.copy_from_slice(&bytes);
    Ok(Hash::from(raw))
}


/// Request for slice synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSlicesRequest {
    pub spool_index: SpoolIndex,
    pub live_groups: u64,
    pub tracks: Vec<Address>,
    pub max_bytes: StorageUnits,
}

/// Response from slice synchronization.
///
/// Asked-for tracks in neither list were left out to stay within the byte
/// budget, and the caller asks for them again.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSlicesResponse {
    pub entries: Vec<SyncSliceEntry>,
    pub missing: Vec<Address>,
}

/// A single slice entry in a sync response.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSliceEntry {
    pub track_address: [u8; 32],
    #[wincode(with = "SliceBytes")]
    pub slice_data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTrackDataRequest {
    pub tracks: Vec<Address>,
    pub max_bytes: StorageUnits,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTrackDataResponse {
    pub entries: Vec<SyncTrackDataEntry>,
    pub missing: Vec<Address>,
}

#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncTrackDataEntry {
    pub track: Address,
    pub payload: Vec<u8>,
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
    pub track: Address,
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

/// Body of a challenge round's proofs from one holder to one group peer.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeRequest {
    pub epoch: EpochNumber,
    pub round: RoundNumber,
    pub proofs: Vec<ChallengeProof>,
}

/// One spool's proof. `spool` is the holder's live spool.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeProof {
    pub spool: SpoolIndex,
    pub track: Address,
    pub data: ChallengeData,
}

/// A leaf of a coded track's slice, or the whole payload of an inline track.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum ChallengeData {
    Coded(ChallengeLeaf),
    Inline(Vec<u8>),
}

/// A sampled leaf and its path to the slice root.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeLeaf {
    pub sub_leaf: Vec<u8>,
    pub sub_proof: Vec<Hash>,
}

/// Response from the challenge board endpoint: the node's rounds after the
/// slot asked for, oldest first.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeBoard {
    pub is_enabled: bool,
    pub rounds: Vec<ChallengeRound>,
}

/// One round as a node saw it. A round the node sat out has no entries.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeRound {
    pub slot: SlotNumber,
    pub is_observed: bool,
    pub entries: Vec<ChallengeEntry>,
}

/// One peer spool in a round. `seen` is empty while nothing has arrived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeEntry {
    pub spool: SpoolIndex,
    pub seen: Option<ChallengeSeen>,
}

/// A proof's outcome and the milliseconds from the round opening to its arrival.
#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct ChallengeSeen {
    pub outcome: ChallengeOutcome,
    pub ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub enum ChallengeOutcome {
    Ok,
    Invalid,
    WrongTrack,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::erasure::{MAX_GROUP_SIZE, SUB_TREE_HEIGHT};
    use tape_core::system::VoteKind;
    use tape_core::track::blob::BlobEncoding;
    use tape_core::types::{StorageUnits, StripeCount};
    use tape_crypto::bls12254::min_sig::G1CompressedPoint;

    fn encoded_slice(len: usize) -> (SlicePayload, Bytes) {
        let data: Vec<u8> = (0..len).map(|byte| (byte % 251) as u8).collect();
        let payload = SlicePayload::new(data, Hash::from([3u8; 32]), path()[..SLICE_TREE_HEIGHT].to_vec());
        let body = Bytes::from(wincode::serialize(&payload).expect("encode"));
        (payload, body)
    }

    // reading a body in place gives the same fields as the wire decoder
    #[test]
    fn slice_body() {
        let (payload, body) = encoded_slice(70_000);

        let parsed = SliceBody::parse(&body).expect("parse");

        assert_eq!(parsed.data.as_ref(), payload.data.as_slice());
        assert_eq!(parsed.leaf_hash, payload.leaf_hash);
        assert_eq!(parsed.merkle_proof, payload.merkle_proof);
    }

    // framing in place writes the bytes the wire encoder writes
    #[test]
    fn framed_body() {
        for len in [0, 1, 70_000] {
            let (payload, body) = encoded_slice(len);
            let mut held = vec![0u8; SLICE_BODY_HEADROOM];
            held.extend_from_slice(&payload.data);

            let framed = SlicePayload::frame(held, payload.leaf_hash, &payload.merkle_proof)
                .expect("frame");

            assert_eq!(framed.as_slice(), body.as_ref());
        }
        let over = vec![0u8; SLICE_BODY_HEADROOM + SLICE_BYTES_LIMIT + 1];
        assert!(SlicePayload::frame(over, Hash::default(), &[]).is_err());
    }

    // a cut or padded body is refused
    #[test]
    fn slice_body_bounds() {
        let (_, body) = encoded_slice(1000);

        assert!(SliceBody::parse(&body.slice(..body.len() - 1)).is_err());
        let mut padded = body.to_vec();
        padded.push(0);
        assert!(SliceBody::parse(&Bytes::from(padded)).is_err());
        let mut inflated = body.to_vec();
        inflated[..8].copy_from_slice(&(SLICE_BYTES_LIMIT as u64 + 1).to_le_bytes());
        assert!(SliceBody::parse(&Bytes::from(inflated)).is_err());
    }

    /// A distinct non-zero hash per level. Zero is the seed the empty-subtree
    /// roots derive from, so it is the one 32-byte value with a meaning of its
    /// own and a poor stand-in for a path element.
    fn path() -> Vec<Hash> {
        (0..SUB_TREE_HEIGHT)
            .map(|level| Hash::from([level as u8 + 1; 32]))
            .collect()
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

    // a coded leaf and an inline payload both survive the wire
    #[test]
    fn challenge_request_roundtrip() {
        let leaf = ChallengeLeaf {
            sub_leaf: vec![0x5A; 1024],
            sub_proof: vec![Hash::from([0x22; 32]); 16],
        };
        let req = ChallengeRequest {
            epoch: EpochNumber(12),
            round: RoundNumber(40),
            proofs: vec![
                ChallengeProof {
                    spool: SpoolIndex(64),
                    track: address(7),
                    data: ChallengeData::Coded(leaf),
                },
                ChallengeProof {
                    spool: SpoolIndex(84),
                    track: address(8),
                    data: ChallengeData::Inline(vec![1, 2, 3]),
                },
            ],
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: ChallengeRequest = wincode::deserialize(&bytes).unwrap();
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
            live_groups: 4,
            tracks: vec![Address::new([0xAA; 32]), Address::new([0xBB; 32])],
            max_bytes: SYNC_MAX_BYTES,
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
            missing: vec![Address::new([0x33; 32])],
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSlicesResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn sync_track_data_roundtrip() {
        let req = SyncTrackDataRequest {
            tracks: vec![Address::new([0x44; 32])],
            max_bytes: SYNC_MAX_BYTES,
        };
        let bytes = wincode::serialize(&req).unwrap();
        assert_eq!(req, wincode::deserialize::<SyncTrackDataRequest>(&bytes).unwrap());

        let resp = SyncTrackDataResponse {
            entries: vec![SyncTrackDataEntry {
                track: Address::new([0x44; 32]),
                payload: vec![7; 1024],
            }],
            missing: vec![Address::new([0x55; 32])],
        };
        let bytes = wincode::serialize(&resp).unwrap();
        assert_eq!(resp, wincode::deserialize::<SyncTrackDataResponse>(&bytes).unwrap());
    }

    #[test]
    fn sync_slices_empty() {
        let resp = SyncSlicesResponse {
            entries: vec![],
            missing: vec![],
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSlicesResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn track_data_response_blob_roundtrip() {
        let resp = TrackDataResponse {
            data: BlobData::Coded(BlobEncoding {
                size: StorageUnits::from_bytes(2048),
                commitment: Hash::from([0x55; 32]),
                stripe_size: StorageUnits::from_bytes(256),
                stripe_count: StripeCount(8),
                leaves: [Hash::from([0x66; 32]); MAX_GROUP_SIZE],
            }),
        };

        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: TrackDataResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

}
