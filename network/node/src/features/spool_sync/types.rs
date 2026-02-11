//! Types for spool synchronization protocol.

use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_node_api::MERKLE_HEIGHT;
use tape_store::types::Pubkey;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Sync request message (versioned for future compatibility).
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub enum SyncSpoolRequest {
    V1(SyncSpoolRequestV1),
}

/// Version 1 of sync spool request.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct SyncSpoolRequestV1 {
    /// The spool index to sync.
    pub spool_index: SpoolIndex,
    /// Starting track address for pagination (Pubkey::default() = from beginning).
    pub starting_track: Pubkey,
    /// Maximum number of slices to return per batch.
    pub batch_size: u32,
    /// The epoch this request is for.
    pub epoch: EpochNumber,
}

impl SyncSpoolRequest {
    /// Create a new V1 request.
    pub fn new_v1(
        spool_index: SpoolIndex,
        starting_track: Pubkey,
        batch_size: u32,
        epoch: EpochNumber,
    ) -> Self {
        Self::V1(SyncSpoolRequestV1 {
            spool_index,
            starting_track,
            batch_size,
            epoch,
        })
    }
}

/// A single slice in a sync response.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct SyncSlice {
    /// Track address (Pubkey).
    pub track_address: Pubkey,
    /// Slice/spool index (0 to SPOOL_COUNT-1).
    pub slice_index: SpoolIndex,
    /// Raw slice data.
    pub data: Vec<u8>,
    /// Merkle leaf hash of this slice (Blake3 hash of data).
    pub leaf_hash: Hash,
    /// Merkle proof path (MERKLE_HEIGHT sibling hashes).
    pub merkle_proof: [Hash; MERKLE_HEIGHT],
}

/// Sync response message (versioned).
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::EpochNumber;

    #[test]
    fn test_sync_request_creation() {
        let request = SyncSpoolRequest::new_v1(
            42,
            Pubkey::default(),
            1000,
            EpochNumber(5),
        );

        match request {
            SyncSpoolRequest::V1(v1) => {
                assert_eq!(v1.spool_index, 42);
                assert_eq!(v1.starting_track, Pubkey::default());
                assert_eq!(v1.batch_size, 1000);
                assert_eq!(v1.epoch, EpochNumber(5));
            }
        }
    }

    #[test]
    fn test_sync_request_wincode_roundtrip() {
        let request = SyncSpoolRequest::new_v1(
            42,
            Pubkey::new_unique(),
            500,
            EpochNumber(10),
        );

        let bytes = wincode::serialize(&request).unwrap();
        let decoded: SyncSpoolRequest = wincode::deserialize(&bytes).unwrap();

        match (&request, &decoded) {
            (SyncSpoolRequest::V1(a), SyncSpoolRequest::V1(b)) => {
                assert_eq!(a.spool_index, b.spool_index);
                assert_eq!(a.starting_track, b.starting_track);
                assert_eq!(a.batch_size, b.batch_size);
                assert_eq!(a.epoch, b.epoch);
            }
        }
    }

    #[test]
    fn test_sync_response_empty() {
        let empty = SyncSpoolResponse::new_v1(vec![]);
        assert!(empty.is_empty());

        let non_empty = SyncSpoolResponse::new_v1(vec![SyncSlice {
            track_address: Pubkey::new_unique(),
            slice_index: 0,
            data: vec![1, 2, 3],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        }]);
        assert!(!non_empty.is_empty());
    }

    #[test]
    fn test_sync_response_wincode_roundtrip() {
        let response = SyncSpoolResponse::new_v1(vec![SyncSlice {
            track_address: Pubkey::new_unique(),
            slice_index: 7,
            data: vec![1, 2, 3, 4],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        }]);

        let bytes = wincode::serialize(&response).unwrap();
        let decoded: SyncSpoolResponse = wincode::deserialize(&bytes).unwrap();

        assert_eq!(response.slices().len(), decoded.slices().len());
        assert_eq!(
            response.slices()[0].track_address,
            decoded.slices()[0].track_address
        );
        assert_eq!(response.slices()[0].data, decoded.slices()[0].data);
    }
}
