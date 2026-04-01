//! Stream-level API for multi-track storage.
//!
//! Large byte streams are split into chunk tracks, each stored separately on a
//! single tape. A manifest track is written last to record chunk layout for
//! reassembly during reads.

pub mod error;
pub mod manifest;
pub mod read;
pub mod receipt;
pub mod write;
