//! Types for spool synchronization protocol.

use tape_core::spooler::SpoolIndex;
use tape_core::types::EpochNumber;
use tape_crypto::Hash;
use tape_node_api::MERKLE_HEIGHT;
use tape_store::types::Pubkey;
use wincode_derive::{SchemaRead, SchemaWrite};

/// Sync spool request.
#[derive(Clone, Debug, SchemaRead, SchemaWrite)]
pub struct SyncSpoolRequest {
    /// The spool index to sync.
    pub spool_index: SpoolIndex,
    /// Starting track address for pagination (Pubkey::default() = from beginning).
    pub starting_track: Pubkey,
    /// Maximum number of slices to return per batch.
    pub batch_size: u32,
    /// The epoch this request is for.
    pub epoch: EpochNumber,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tape_core::types::EpochNumber;

    #[test]
    fn test_sync_request_creation() {
        let request = SyncSpoolRequest {
            spool_index: 42,
            starting_track: Pubkey::default(),
            batch_size: 1000,
            epoch: EpochNumber(5),
        };

        assert_eq!(request.spool_index, 42);
        assert_eq!(request.starting_track, Pubkey::default());
        assert_eq!(request.batch_size, 1000);
        assert_eq!(request.epoch, EpochNumber(5));
    }

    #[test]
    fn test_sync_request_wincode_roundtrip() {
        let request = SyncSpoolRequest {
            spool_index: 42,
            starting_track: Pubkey::new_unique(),
            batch_size: 500,
            epoch: EpochNumber(10),
        };

        let bytes = wincode::serialize(&request).unwrap();
        let decoded: SyncSpoolRequest = wincode::deserialize(&bytes).unwrap();

        assert_eq!(request.spool_index, decoded.spool_index);
        assert_eq!(request.starting_track, decoded.starting_track);
        assert_eq!(request.batch_size, decoded.batch_size);
        assert_eq!(request.epoch, decoded.epoch);
    }

    #[test]
    fn test_sync_response_wincode_roundtrip() {
        let response = vec![SyncSlice {
            track_address: Pubkey::new_unique(),
            slice_index: 7,
            data: vec![1, 2, 3, 4],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        }];

        let bytes = wincode::serialize(&response).unwrap();
        let decoded: Vec<SyncSlice> = wincode::deserialize(&bytes).unwrap();

        assert_eq!(response.len(), decoded.len());
        assert_eq!(response[0].track_address, decoded[0].track_address);
        assert_eq!(response[0].data, decoded[0].data);
    }
}
