//! Route constants and URL builders for the node API.

pub const API_V1: &str = "/v1";

// Routes 

pub const HEALTH_PATH:             &str = "/v1/health";
pub const INCONSISTENCY_PATH:      &str = "/v1/tracks/:track_id/inconsistency";
pub const INFO_PATH:               &str = "/v1/info";
pub const METADATA_PATH:           &str = "/v1/tracks/:track_id/metadata";
pub const REPAIR_PATH:             &str = "/v1/tracks/:track_id/repair";
pub const SIGN_PATH:               &str = "/v1/tracks/:track_id/sign";
pub const SLICE_PATH:              &str = "/v1/tracks/:track_id/slices/:spool_id";
pub const SNAPSHOT_SIG_PATH:       &str = "/v1/snapshots/:epoch/:chunk_index/sig";
pub const STATS_PATH:              &str = "/v1/stats";
pub const SYNC_SPOOL_PATH:         &str = "/v1/sync/spool";
pub const TRACK_STATUS_PATH:       &str = "/v1/tracks/:track_id/status";

// Route Builders 

pub fn slice_url(track_id: &str, spool_id: u16) -> String {
    format!("/v1/tracks/{track_id}/slices/{spool_id}")
}

pub fn metadata_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/metadata")
}

pub fn status_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/status")
}

pub fn sign_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/sign")
}

pub fn repair_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/repair")
}

pub fn inconsistency_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/inconsistency")
}

pub fn snapshot_signature_url(epoch: u64, chunk_index: u64) -> String {
    format!("/v1/snapshots/{epoch}/{chunk_index}/sig")
}

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
        assert_eq!(
            snapshot_signature_url(10, 3),
            "/v1/snapshots/10/3/sig"
        );
    }
}
