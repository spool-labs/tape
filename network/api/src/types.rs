//! Protocol request/response types for the node API.

use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Response from the signature endpoint.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct BlsSignResponse {
    pub signature: [u8; 32],
    pub node_id: u64,
    pub epoch: u64,
}

/// Request for inconsistency attestation.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct InconsistencyRequest {
    pub computed_root: Hash,
}

/// Response from the inconsistency attestation endpoint.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct BlsInconsistencyResponse {
    pub signature: [u8; 32],
    pub node_id: u64,
    pub epoch: u64,
}

/// Request for sub-chunk extraction (bandwidth-optimal repair).
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct RepairRequest {
    pub lost_slice: u16,
    pub helper_spool: u16,
    pub stripes: Vec<StripeSubChunkRequest>,
}

/// Per-stripe sub-chunk extraction instructions.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct StripeSubChunkRequest {
    pub stripe: u32,
    pub sub_chunks: Vec<u32>,
}

/// Response from the stats endpoint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeStats {
    pub last_processed_slot: u64,
    pub blocks_processed: u64,
    pub epoch_transitions: u64,
    pub current_epoch: u64,
    pub owned_spools: u64,
    pub tracks_stored: u64,
    pub storage_bytes_used: u64,
    pub slices_stored: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub requests_total: u64,
}

/// Payload for slice upload requests.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SlicePayload {
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

/// Request for spool synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSpoolRequest {
    pub spool_index: u16,
    /// Last track address received, or empty to start from the beginning.
    pub cursor: Option<[u8; 32]>,
    pub limit: u32,
}

/// Response from spool synchronization.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSpoolResponse {
    pub entries: Vec<SyncSpoolEntry>,
    /// Next cursor for pagination, or None if no more entries.
    pub next_cursor: Option<[u8; 32]>,
}

/// A single slice entry in a sync response.
#[derive(Debug, Clone, PartialEq, Eq, SchemaRead, SchemaWrite)]
pub struct SyncSpoolEntry {
    pub track_address: [u8; 32],
    pub slice_data: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_roundtrip() {
        let data = vec![0xAB; 1024];
        let leaf_hash = Hash::from([0x11; 32]);
        let proof = vec![Hash::from([0x22; 32]); MERKLE_HEIGHT];

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

    #[test]
    fn sign_response() {
        let resp = BlsSignResponse {
            signature: [0xAA; 32],
            node_id: 42,
            epoch: 100,
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: BlsSignResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn inconsistency() {
        let req = InconsistencyRequest {
            computed_root: Hash::from([0xBB; 32]),
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: InconsistencyRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);

        let resp = BlsInconsistencyResponse {
            signature: [0xCC; 32],
            node_id: 1,
            epoch: 50,
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: BlsInconsistencyResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn repair() {
        let req = RepairRequest {
            lost_slice: 3,
            helper_spool: 42,
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
    fn sync_spool_request() {
        let req = SyncSpoolRequest {
            spool_index: 42,
            cursor: Some([0xAA; 32]),
            limit: 100,
        };
        let bytes = wincode::serialize(&req).unwrap();
        let decoded: SyncSpoolRequest = wincode::deserialize(&bytes).unwrap();
        assert_eq!(req, decoded);
    }

    #[test]
    fn sync_spool_response() {
        let resp = SyncSpoolResponse {
            entries: vec![
                SyncSpoolEntry {
                    track_address: [0x11; 32],
                    slice_data: vec![1, 2, 3],
                },
                SyncSpoolEntry {
                    track_address: [0x22; 32],
                    slice_data: vec![4, 5, 6],
                },
            ],
            next_cursor: Some([0x22; 32]),
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSpoolResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }

    #[test]
    fn sync_spool_empty() {
        let resp = SyncSpoolResponse {
            entries: vec![],
            next_cursor: None,
        };
        let bytes = wincode::serialize(&resp).unwrap();
        let decoded: SyncSpoolResponse = wincode::deserialize(&bytes).unwrap();
        assert_eq!(resp, decoded);
    }
}
