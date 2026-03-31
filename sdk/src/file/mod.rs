//! File-level API for multi-track storage.
//!
//! Files that exceed the single-track size limit are split into chunks,
//! each stored as a separate track. A manifest track is written last,
//! recording the chunk layout for reassembly on read.
//!
//! The file API always writes a manifest, even for single-chunk files.
//! This keeps `read_file` unambiguous — it always reads a manifest.

pub mod error;
pub mod manifest;
pub mod read;
pub mod receipt;
pub mod write;

pub use error::FileError;
