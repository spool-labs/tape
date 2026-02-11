//! Shared API routes and constants for tapedrive storage nodes.
//!
//! This crate provides the canonical definitions for REST API endpoints
//! used by both `tape-node` (server) and `tape-node-client` (client).

use serde::{Deserialize, Serialize};
use tape_crypto::Hash;
use wincode_derive::{SchemaRead, SchemaWrite};

// =============================================================================
// Signature Response Type
// =============================================================================

/// Response from the signature endpoint.
///
/// Returned by GET /v1/tracks/{track_id}/sign
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignResponse {
    /// BLS signature as raw bytes (32 bytes, compressed G1).
    pub signature: [u8; 32],
    /// NodeId of the signing node.
    pub node_id: u64,
    /// Committee member index for bitmap construction.
    pub member_index: u8,
    /// Epoch number that was signed (for message reconstruction).
    pub epoch: u64,
}

/// API version prefix.
pub const API_V1: &str = "/v1";

/// Merkle tree height for blob encoding.
/// 2^5 = 32 leaves (20 used = SPOOL_GROUP_SIZE).
/// Matches `tape_core::erasure::COMMITMENT_TREE_HEIGHT`.
pub const MERKLE_HEIGHT: usize = 5;

// =============================================================================
// Payload Types
// =============================================================================

/// Payload for slice upload requests.
///
/// Sent via PUT /v1/tracks/{track_id}/slices/{slice_index}
/// Serialized using wincode (Content-Type: application/x-wincode)
///
/// The payload includes the slice data along with the merkle proof,
/// allowing storage nodes to verify the slice belongs to the claimed blob.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct SlicePayload {
    /// The raw slice data.
    pub data: Vec<u8>,
    /// Merkle leaf hash of this slice (hash of data).
    pub leaf_hash: Hash,
    /// Merkle proof (MERKLE_HEIGHT sibling hashes).
    pub merkle_proof: [Hash; MERKLE_HEIGHT],
}

impl SlicePayload {
    /// Create a new slice payload.
    pub fn new(data: Vec<u8>, leaf_hash: Hash, merkle_proof: [Hash; MERKLE_HEIGHT]) -> Self {
        Self {
            data,
            leaf_hash,
            merkle_proof,
        }
    }

    /// Serialize to wincode bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        wincode::serialize(self).expect("SlicePayload serialization should never fail")
    }

    /// Deserialize from wincode bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, wincode::ReadError> {
        wincode::deserialize(bytes)
    }
}

// =============================================================================
// Slice Operations
// =============================================================================

/// GET/PUT endpoint for individual slice operations.
///
/// Path parameters:
/// - `track_id`: The track identifier
/// - `slice_index`: The slice index (0 to SPOOL_GROUP_SIZE-1)
pub const SLICE_PATH: &str = "/v1/tracks/{track_id}/slices/{slice_index}";

/// GET endpoint for slice existence check.
pub const SLICE_STATUS_PATH: &str = "/v1/tracks/{track_id}/slices/{slice_index}/status";

// =============================================================================
// Track Operations
// =============================================================================

/// GET/PUT endpoint for track metadata.
///
/// Path parameters:
/// - `track_id`: The track identifier
pub const METADATA_PATH: &str = "/v1/tracks/{track_id}/metadata";

/// GET endpoint for metadata existence check.
pub const METADATA_STATUS_PATH: &str = "/v1/tracks/{track_id}/metadata/status";

/// GET endpoint for track lifecycle status.
///
/// Path parameters:
/// - `track_id`: The track identifier
pub const TRACK_STATUS_PATH: &str = "/v1/tracks/{track_id}/status";

/// POST endpoint for bandwidth-optimal repair (sub-chunk extraction).
pub const REPAIR_PATH: &str = "/v1/tracks/{track_id}/repair";

/// POST endpoint for inconsistency attestation.
pub const INCONSISTENCY_PATH: &str = "/v1/tracks/{track_id}/inconsistency";

// =============================================================================
// Node Operations
// =============================================================================

/// GET endpoint for health checks.
pub const HEALTH_PATH: &str = "/v1/health";

/// GET endpoint for node information.
pub const INFO_PATH: &str = "/v1/info";

/// GET endpoint for node statistics (block processor metrics).
pub const STATS_PATH: &str = "/v1/stats";

/// Response from the stats endpoint.
///
/// Returned by GET /v1/stats
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeStats {
    /// Last Solana slot processed by the block processor.
    pub last_processed_slot: u64,
    /// Total blocks (with tapedrive instructions) processed.
    pub blocks_processed: u64,
    /// Total epoch transitions seen.
    pub epoch_transitions: u64,
    /// Current epoch number.
    pub current_epoch: u64,
    /// Number of spools owned by this node.
    pub owned_spools: u64,
    /// Number of tracks stored.
    pub tracks_stored: u64,
    /// Storage bytes used.
    pub storage_bytes_used: u64,
    /// Total slices stored.
    pub slices_stored: u64,
    /// Cumulative bytes uploaded (received by this node).
    pub bytes_uploaded: u64,
    /// Cumulative bytes downloaded (served by this node).
    pub bytes_downloaded: u64,
    /// Cumulative total requests handled.
    pub requests_total: u64,
}

// =============================================================================
// Certification Operations
// =============================================================================

/// GET endpoint for track signature (BLS certification).
///
/// Path parameters:
/// - `track_id`: The track identifier (base58 pubkey)
///
/// Returns: SignResponse with BLS signature, node_id, and member_index
pub const SIGN_PATH: &str = "/v1/tracks/{track_id}/sign";

// =============================================================================
// Node-to-Node Operations
// =============================================================================

/// POST endpoint for spool synchronization during epoch transitions.
pub const SYNC_SPOOL_PATH: &str = "/v1/migrate/sync_spool";

// =============================================================================
// Snapshot Operations
// =============================================================================

/// GET endpoint for snapshot chunk BLS signature.
///
/// Path parameters:
/// - `epoch`: Epoch number
/// - `chunk_index`: Chunk index (0..SPOOL_GROUP_COUNT-1)
///
/// Returns: SignResponse with BLS signature, node_id, and member_index
pub const SNAPSHOT_SIGN_PATH: &str = "/v1/snapshots/{epoch}/sign/{chunk_index}";

/// Build a snapshot sign endpoint URL.
pub fn snapshot_sign_url(epoch: u64, chunk_index: u64) -> String {
    format!("/v1/snapshots/{}/sign/{}", epoch, chunk_index)
}

// =============================================================================
// Repair Types
// =============================================================================

/// Request for inconsistency attestation.
///
/// Sent by the detecting node to spool group peers. Serialized with wincode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct InconsistencyRequest {
    /// Merkle root computed by the detecting node after full recovery
    /// (decode + re-encode). Differs from on-chain commitment.
    pub computed_root: Hash,
}

/// Response from the inconsistency attestation endpoint.
///
/// Returned by POST /v1/tracks/{track_id}/inconsistency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InconsistencyResponse {
    /// BLS signature over the InvalidateMessage (32 bytes, compressed G1).
    pub signature: [u8; 32],
    /// NodeId of the attesting node.
    pub node_id: u64,
    /// Committee member index for bitmap construction.
    pub member_index: u8,
    /// Epoch number that was signed.
    pub epoch: u64,
}

/// Request for sub-chunk extraction (bandwidth-optimal repair).
///
/// Sent by the repairing node to each helper. Serialized with wincode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct RepairRequest {
    /// The slice index being repaired (lost slice).
    pub lost_slice: u16,
    /// The spool index of this helper's slice (so the helper knows which slice to read).
    pub helper_spool: u16,
    /// Per-stripe sub-chunk extraction plan for this helper.
    pub stripes: Vec<StripeSubChunkRequest>,
}

/// Per-stripe sub-chunk extraction instructions.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, SchemaRead, SchemaWrite)]
pub struct StripeSubChunkRequest {
    /// Stripe index.
    pub stripe: u32,
    /// Sub-chunk indices to extract from this helper's chunk.
    pub sub_chunks: Vec<u32>,
}

// =============================================================================
// Content Types
// =============================================================================

/// Content type for wincode-encoded request/response bodies.
pub const CONTENT_TYPE_WINCODE: &str = "application/x-wincode";

/// Content type for raw binary data (slices).
pub const CONTENT_TYPE_OCTET_STREAM: &str = "application/octet-stream";

/// Content type for JSON responses.
pub const CONTENT_TYPE_JSON: &str = "application/json";

// =============================================================================
// Helper Functions
// =============================================================================

/// Build a slice endpoint URL for a specific track and slice.
///
/// # Example
/// ```
/// use tape_node_api::slice_url;
/// let url = slice_url("track123", 42);
/// assert_eq!(url, "/v1/tracks/track123/slices/42");
/// ```
pub fn slice_url(track_id: &str, slice_index: u16) -> String {
    format!("/v1/tracks/{}/slices/{}", track_id, slice_index)
}

/// Build a metadata endpoint URL for a specific track.
pub fn metadata_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/metadata", track_id)
}

/// Build a status endpoint URL for a specific track.
pub fn status_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/status", track_id)
}

/// Build a sign endpoint URL for a specific track.
pub fn sign_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/sign", track_id)
}

/// Build a slice status endpoint URL.
pub fn slice_status_url(track_id: &str, slice_index: u16) -> String {
    format!("/v1/tracks/{}/slices/{}/status", track_id, slice_index)
}

/// Build a metadata status endpoint URL.
pub fn metadata_status_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/metadata/status", track_id)
}

/// Build a repair endpoint URL for a specific track.
pub fn repair_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/repair", track_id)
}

/// Build an inconsistency endpoint URL for a specific track.
pub fn inconsistency_url(track_id: &str) -> String {
    format!("/v1/tracks/{}/inconsistency", track_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slice_url() {
        assert_eq!(slice_url("abc", 0), "/v1/tracks/abc/slices/0");
        assert_eq!(slice_url("track_123", 1023), "/v1/tracks/track_123/slices/1023");
    }

    #[test]
    fn test_metadata_url() {
        assert_eq!(metadata_url("abc"), "/v1/tracks/abc/metadata");
    }

    #[test]
    fn test_status_url() {
        assert_eq!(status_url("abc"), "/v1/tracks/abc/status");
    }

    #[test]
    fn test_sign_url() {
        assert_eq!(sign_url("abc"), "/v1/tracks/abc/sign");
        assert_eq!(sign_url("track_123"), "/v1/tracks/track_123/sign");
    }

    #[test]
    fn test_paths_consistent() {
        // Ensure template paths match helper functions
        let track = "test_track";
        let slice = 42u16;

        let built = slice_url(track, slice);
        let expected = SLICE_PATH
            .replace("{track_id}", track)
            .replace("{slice_index}", &slice.to_string());
        assert_eq!(built, expected);
    }

    #[test]
    fn test_slice_payload_roundtrip() {
        let payload = SlicePayload {
            data: vec![0xAB; 1000],
            leaf_hash: Hash::default(),
            merkle_proof: [Hash::default(); MERKLE_HEIGHT],
        };

        let bytes = payload.to_bytes();
        let decoded = SlicePayload::from_bytes(&bytes).unwrap();

        assert_eq!(payload, decoded);
    }

    #[test]
    fn test_slice_payload_new() {
        let data = vec![1, 2, 3, 4];
        let leaf_hash = Hash::default();
        let proof = [Hash::default(); MERKLE_HEIGHT];

        let payload = SlicePayload::new(data.clone(), leaf_hash, proof);

        assert_eq!(payload.data, data);
        assert_eq!(payload.leaf_hash, leaf_hash);
        assert_eq!(payload.merkle_proof, proof);
    }
}
