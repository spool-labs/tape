//! Types for spool synchronization protocol.

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_node_api::MERKLE_HEIGHT;

/// Track identifier (Pubkey serialized as base58 string for JSON compatibility).
pub type TrackId = String;

/// Convert a Pubkey to TrackId for serialization.
pub fn track_id_from_pubkey(pubkey: &Pubkey) -> TrackId {
    pubkey.to_string()
}

/// Parse a TrackId back to Pubkey.
pub fn track_id_to_pubkey(track_id: &TrackId) -> Result<Pubkey, &'static str> {
    track_id.parse().map_err(|_| "invalid track id")
}

/// Sync request message (versioned for future compatibility).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncSpoolRequest {
    V1(SyncSpoolRequestV1),
}

/// Version 1 of sync spool request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncSpoolRequestV1 {
    /// The spool index to sync.
    pub spool_index: SpoolIndex,
    /// Starting track ID for pagination.
    pub starting_track_id: TrackId,
    /// Maximum number of slices to return per batch.
    pub batch_size: usize,
    /// The epoch this request is for.
    pub epoch: EpochNumber,
}

impl SyncSpoolRequest {
    /// Create a new V1 request.
    pub fn new_v1(
        spool_index: SpoolIndex,
        starting_track_id: TrackId,
        batch_size: usize,
        epoch: EpochNumber,
    ) -> Self {
        Self::V1(SyncSpoolRequestV1 {
            spool_index,
            starting_track_id,
            batch_size,
            epoch,
        })
    }
}

/// A single slice in a sync response.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SyncSlice {
    /// Track identifier (base58 pubkey).
    pub track_id: TrackId,
    /// Slice/spool index (0 to SLICE_COUNT-1).
    pub slice_index: SpoolIndex,
    /// Raw slice data.
    pub data: Vec<u8>,
    /// Merkle leaf hash of this slice (Blake3 hash of data).
    pub leaf_hash: Hash,
    /// Merkle proof path (MERKLE_HEIGHT sibling hashes).
    pub merkle_proof: [Hash; MERKLE_HEIGHT],
}

/// Sync response message (versioned).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncSpoolResponse {
    /// V1 response: list of slices.
    V1(Vec<SyncSlice>),
}

impl SyncSpoolResponse {
    /// Create a new V1 response.
    pub fn new_v1(slices: Vec<SyncSlice>) -> Self {
        Self::V1(slices)
    }

    /// Check if response is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::V1(slices) => slices.is_empty(),
        }
    }

    /// Get the slices (for V1).
    pub fn slices(&self) -> &[SyncSlice] {
        match self {
            Self::V1(slices) => slices,
        }
    }
}

/// Signed sync request for node-to-node communication.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedSyncRequest {
    /// The underlying request.
    pub request: SyncSpoolRequest,
    /// Ed25519 signature over the serialized request.
    pub signature: Vec<u8>,
    /// Public key of the signer.
    pub signer_pubkey: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::EpochNumber;

    #[test]
    fn test_sync_request_creation() {
        let request = SyncSpoolRequest::new_v1(
            42,
            "track_0".to_string(),
            1000,
            EpochNumber(5),
        );

        match request {
            SyncSpoolRequest::V1(v1) => {
                assert_eq!(v1.spool_index, 42);
                assert_eq!(v1.starting_track_id, "track_0");
                assert_eq!(v1.batch_size, 1000);
                assert_eq!(v1.epoch, EpochNumber(5));
            }
        }
    }

    #[test]
    fn test_sync_response_empty() {
        let empty = SyncSpoolResponse::new_v1(vec![]);
        assert!(empty.is_empty());

        let non_empty = SyncSpoolResponse::new_v1(vec![SyncSlice {
            track_id: "track_1".to_string(),
            slice_index: 0,
            data: vec![1, 2, 3],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        }]);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_track_id_conversion() {
        let pubkey = Pubkey::new_unique();
        let track_id = track_id_from_pubkey(&pubkey);
        let parsed = track_id_to_pubkey(&track_id).unwrap();
        assert_eq!(pubkey, parsed);
    }
}
