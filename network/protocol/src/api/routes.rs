//! Route constants and URL builders for the node API.

use tape_core::types::{SpoolIndex, TrackNumber};

pub const API_V1: &str = "/v1";

// Routes

pub const NODE_HEALTH_PATH: &str = "/v1/health";
pub const NODE_INFO_PATH: &str = "/v1/info";
pub const NODE_METRICS_PATH: &str = "/v1/metrics";
pub const NODE_STATS_PATH: &str = "/v1/stats";

pub const VOTE_PATH: &str = "/v1/votes";

pub const SYNC_SLICES_PATH: &str = "/v1/sync/slices";
pub const SYNC_TRACKS_PATH: &str = "/v1/sync/tracks";

pub const TAPE_TRACK_PATH: &str = "/v1/tapes/{tape_id}/tracks/{track_number}";
pub const TAPE_TRACK_FIND_PATH: &str = "/v1/tapes/{tape_id}/tracks/find";
pub const TAPE_TRACK_LIST_PATH: &str = "/v1/tapes/{tape_id}/tracks/list";
pub const TAPE_OBJECT_LIST_PATH: &str = "/v1/tapes/{tape_id}/objects/list";

pub const TRACK_PATH: &str = "/v1/tracks/{track_id}";
pub const TRACK_DATA_PATH: &str = "/v1/tracks/{track_id}/data";
pub const TRACK_INCONSISTENCY_PATH: &str = "/v1/tracks/{track_id}/inconsistency";
pub const TRACK_PROOF_PATH: &str = "/v1/tracks/{track_id}/proof";
pub const TRACK_REPAIR_PATH: &str = "/v1/tracks/{track_id}/repair";
pub const TRACK_SIGN_PATH: &str = "/v1/tracks/{track_id}/sign";
pub const TRACK_SLICE_PATH: &str = "/v1/tracks/{track_id}/slices/{spool_id}";
pub const TRACK_SLICE_STATUS_PATH: &str = "/v1/tracks/{track_id}/slices/{spool_id}/status";
pub const TRACK_STATUS_PATH: &str = "/v1/tracks/{track_id}/status";

// Route Builders

pub fn slice_url(track_id: &str, spool_id: SpoolIndex) -> String {
    format!("/v1/tracks/{track_id}/slices/{}", spool_id.0)
}

pub fn status_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/status")
}

pub fn track_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}")
}

pub fn track_data_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/data")
}

pub fn track_proof_url(track_id: &str) -> String {
    format!("/v1/tracks/{track_id}/proof")
}

pub fn tape_track_url(tape_id: &str, track_number: TrackNumber) -> String {
    format!("/v1/tapes/{tape_id}/tracks/{}", track_number.0)
}

pub fn find_track_url(tape_id: &str) -> String {
    format!("/v1/tapes/{tape_id}/tracks/find")
}

pub fn list_tracks_by_tape_url(tape_id: &str) -> String {
    format!("/v1/tapes/{tape_id}/tracks/list")
}

pub fn list_objects_url(tape_id: &str) -> String {
    format!("/v1/tapes/{tape_id}/objects/list")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_builders() {
        assert_eq!(slice_url("abc", SpoolIndex(5)), "/v1/tracks/abc/slices/5");
        assert_eq!(track_url("abc"), "/v1/tracks/abc");
        assert_eq!(track_data_url("abc"), "/v1/tracks/abc/data");
        assert_eq!(track_proof_url("abc"), "/v1/tracks/abc/proof");
        assert_eq!(tape_track_url("def", TrackNumber(7)), "/v1/tapes/def/tracks/7");
        assert_eq!(find_track_url("def"), "/v1/tapes/def/tracks/find");
        assert_eq!(list_tracks_by_tape_url("def"), "/v1/tapes/def/tracks/list");
        assert_eq!(list_objects_url("def"), "/v1/tapes/def/objects/list");
        assert_eq!(status_url("abc"), "/v1/tracks/abc/status");
        assert_eq!(sign_url("abc"), "/v1/tracks/abc/sign");
        assert_eq!(repair_url("abc"), "/v1/tracks/abc/repair");
        assert_eq!(inconsistency_url("abc"), "/v1/tracks/abc/inconsistency");
    }
}
