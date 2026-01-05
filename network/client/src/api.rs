//! API endpoint constants.
//!
//! Re-exports from `tape-node-api` for backwards compatibility.

pub use tape_node_api::{
    API_V1,
    CONTENT_TYPE_WINCODE,
    HEALTH_PATH,
    INFO_PATH,
    METADATA_PATH,
    SLICE_PATH,
    STATUS_PATH,
    SYNC_SPOOL_PATH,
    // Helper functions
    metadata_url,
    slice_url,
    status_url,
};
