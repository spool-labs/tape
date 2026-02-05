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
/// 2^5 = 32 leaves (20 used = SLICE_COUNT).
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
/// - `slice_index`: The slice index (0 to SLICE_COUNT-1)
pub const SLICE_PATH: &str = "/v1/tracks/{track_id}/slices/{slice_index}";

// =============================================================================
// Track Operations
// =============================================================================

/// GET/PUT endpoint for track metadata.
///
/// Path parameters:
/// - `track_id`: The track identifier
pub const METADATA_PATH: &str = "/v1/tracks/{track_id}/metadata";

/// GET endpoint for track status information.
///
/// Path parameters:
/// - `track_id`: The track identifier
pub const STATUS_PATH: &str = "/v1/tracks/{track_id}/status";

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
