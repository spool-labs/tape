//! Types for spool synchronization protocol.

use serde::{Deserialize, Serialize};

/// Spool index (0-1023).
pub type SpoolIndex = u16;

/// Epoch number.
pub type EpochNumber = u64;

/// Track identifier (as string for now, will be proper type later).
pub type TrackId = String;

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
    /// Maximum number of slices to return.
    pub slice_count: u64,
    /// The epoch this request is for.
    pub epoch: EpochNumber,
}

impl SyncSpoolRequest {
    /// Create a new V1 request.
    pub fn new_v1(
        spool_index: SpoolIndex,
        starting_track_id: TrackId,
        slice_count: u64,
        epoch: EpochNumber,
    ) -> Self {
        Self::V1(SyncSpoolRequestV1 {
            spool_index,
            starting_track_id,
            slice_count,
            epoch,
        })
    }
}

/// Sync response message (versioned).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SyncSpoolResponse {
    /// V1 response: list of (track_id, slice_index, data).
    V1(Vec<(TrackId, u16, Vec<u8>)>),
}

impl SyncSpoolResponse {
    /// Create a new V1 response.
    pub fn new_v1(slices: Vec<(TrackId, u16, Vec<u8>)>) -> Self {
        Self::V1(slices)
    }

    /// Check if response is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::V1(slices) => slices.is_empty(),
        }
    }

    /// Get the slices (for V1).
    pub fn slices(&self) -> &[(TrackId, u16, Vec<u8>)] {
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

    #[test]
    fn test_sync_request_creation() {
        let request = SyncSpoolRequest::new_v1(42, "track_0".to_string(), 1000, 5);

        match request {
            SyncSpoolRequest::V1(v1) => {
                assert_eq!(v1.spool_index, 42);
                assert_eq!(v1.starting_track_id, "track_0");
                assert_eq!(v1.slice_count, 1000);
                assert_eq!(v1.epoch, 5);
            }
        }
    }

    #[test]
    fn test_sync_response_empty() {
        let empty = SyncSpoolResponse::new_v1(vec![]);
        assert!(empty.is_empty());

        let non_empty = SyncSpoolResponse::new_v1(vec![
            ("track_1".to_string(), 0, vec![1, 2, 3]),
        ]);
        assert!(!non_empty.is_empty());
    }
}
