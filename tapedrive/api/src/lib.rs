//! Shared API routes and constants for tapedrive storage nodes.
//!
//! This crate provides the canonical definitions for REST API endpoints
//! used by both `tape-node` (server) and `tape-node-client` (client).

/// API version prefix.
pub const API_V1: &str = "/v1";

// =============================================================================
// Slice Operations
// =============================================================================

/// GET/PUT endpoint for individual slice operations.
///
/// Path parameters:
/// - `track_id`: The track identifier
/// - `slice_index`: The slice index (0-1023)
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
}
