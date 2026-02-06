//! API endpoint constants.
//!
//! Re-exports from `tape-node-api`.

pub use tape_node_api::{
    API_V1,
    CONTENT_TYPE_WINCODE,
    HEALTH_PATH,
    INFO_PATH,
    METADATA_PATH,
    METADATA_STATUS_PATH,
    SLICE_PATH,
    SLICE_STATUS_PATH,
    TRACK_STATUS_PATH,
    REPAIR_PATH,
    INCONSISTENCY_PATH,
    SYNC_SPOOL_PATH,
    // Helper functions
    metadata_url,
    slice_url,
    status_url,
    repair_url,
};
