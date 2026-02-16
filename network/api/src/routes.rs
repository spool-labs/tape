//! Route constants and URL builders for the node API.

/// API version prefix.
pub const API_V1: &str = "/v1";

/// GET/PUT endpoint for individual slice operations.
pub const SLICE_PATH: &str = "/v1/tracks/:track_id/slices/:slice_index";

/// Internal authenticated PUT endpoint for individual slice ingest.
pub const INTERNAL_SLICE_PATH: &str = "/v1/internal/tracks/:track_id/slices/:slice_index";

/// GET endpoint for slice existence check.
pub const SLICE_STATUS_PATH: &str = "/v1/tracks/:track_id/slices/:slice_index/status";

/// GET/PUT endpoint for track metadata.
pub const METADATA_PATH: &str = "/v1/tracks/:track_id/metadata";

/// Internal authenticated PUT endpoint for track metadata ingest.
pub const INTERNAL_METADATA_PATH: &str = "/v1/internal/tracks/:track_id/metadata";

/// GET endpoint for metadata existence check.
pub const METADATA_STATUS_PATH: &str = "/v1/tracks/:track_id/metadata/status";

/// GET endpoint for track lifecycle status.
pub const TRACK_STATUS_PATH: &str = "/v1/tracks/:track_id/status";

/// GET endpoint for track signature (BLS certification).
pub const SIGN_PATH: &str = "/v1/tracks/:track_id/sign";

/// GET endpoint for snapshot chunk BLS signature.
pub const SNAPSHOT_SIGN_PATH: &str = "/v1/snapshots/:epoch/:chunk_index/sign";

/// POST endpoint for bandwidth-optimal repair (sub-chunk extraction).
pub const REPAIR_PATH: &str = "/v1/tracks/:track_id/repair";

/// POST endpoint for inconsistency attestation.
pub const INCONSISTENCY_PATH: &str = "/v1/tracks/:track_id/inconsistency";

/// POST endpoint for spool synchronization during epoch transitions.
pub const SYNC_SPOOL_PATH: &str = "/v1/sync/spool";

/// GET endpoint for health checks.
pub const HEALTH_PATH: &str = "/v1/health";

/// GET endpoint for node information.
pub const INFO_PATH: &str = "/v1/info";

/// GET endpoint for node statistics.
pub const STATS_PATH: &str = "/v1/stats";

/// Build a slice endpoint URL for a specific track and slice.
pub fn slice_url(track_id: &str, slice_index: u16) -> String {
    format!("/v1/tracks/{track_id}/slices/{slice_index}")
}

/// Build an internal slice endpoint URL.
pub fn internal_slice_url(track_id: &str, slice_index: u16) -> String {
    format!("/v1/internal/tracks/{track_id}/slices/{slice_index}")
}

/// Build a slice status endpoint URL.
pub fn slice_status_url(track_id: &str, slice_index: u16) -> String {
    format!("/v1/tracks/{track_id}/slices/{slice_index}/status")
}

/// Build a metadata endpoint URL for a specific track.
pub fn metadata_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/metadata")
}

/// Build an internal metadata endpoint URL.
pub fn internal_metadata_url(track_id: &str) -> String {
    format!("/v1/internal/tracks/{track_id}/metadata")
}

/// Build a metadata status endpoint URL.
pub fn metadata_status_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/metadata/status")
}

/// Build a status endpoint URL for a specific track.
pub fn status_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/status")
}

/// Build a sign endpoint URL for a specific track.
pub fn sign_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/sign")
}

/// Build a repair endpoint URL for a specific track.
pub fn repair_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/repair")
}

/// Build an inconsistency endpoint URL for a specific track.
pub fn inconsistency_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/inconsistency")
}

/// Build a snapshot sign endpoint URL.
pub fn snapshot_sign_url(epoch: u64, chunk_index: u64) -> String {
    format!("/v1/snapshots/{epoch}/{chunk_index}/sign")
}

/// Build a snapshot commitments endpoint URL.
pub fn snapshot_commitments_url(epoch: u64) -> String {
    format!("/v1/snapshots/{epoch}/commitments")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_builders() {
        assert_eq!(slice_url("abc", 5), "/v1/tracks/abc/slices/5");
        assert_eq!(metadata_url("abc"), "/v1/tracks/abc/metadata");
        assert_eq!(status_url("abc"), "/v1/tracks/abc/status");
        assert_eq!(sign_url("abc"), "/v1/tracks/abc/sign");
        assert_eq!(repair_url("abc"), "/v1/tracks/abc/repair");
        assert_eq!(inconsistency_url("abc"), "/v1/tracks/abc/inconsistency");
        assert_eq!(snapshot_sign_url(10, 3), "/v1/snapshots/10/3/sign");
        assert_eq!(
            slice_status_url("abc", 5),
            "/v1/tracks/abc/slices/5/status"
        );
        assert_eq!(
            metadata_status_url("abc"),
            "/v1/tracks/abc/metadata/status"
        );
    }
}
