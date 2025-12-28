//! API endpoint constants.

/// Base path for API version 1.
pub const API_V1: &str = "/v1";

/// Slice endpoint path template.
pub const SLICE_PATH: &str = "/v1/tracks/{track_id}/slices/{slice_index}";

/// Metadata endpoint path template.
pub const METADATA_PATH: &str = "/v1/tracks/{track_id}/metadata";

/// Status endpoint path template.
pub const STATUS_PATH: &str = "/v1/tracks/{track_id}/status";

/// Health check endpoint.
pub const HEALTH_PATH: &str = "/v1/health";

/// Node info endpoint.
pub const INFO_PATH: &str = "/v1/info";

/// Shard sync endpoint (node-to-node).
pub const SYNC_SHARD_PATH: &str = "/v1/migrate/sync_shard";

/// Content type for wincode-encoded bodies.
pub const CONTENT_TYPE_WINCODE: &str = "application/x-wincode";
