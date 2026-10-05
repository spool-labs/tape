//! Client-level knobs for the write paths.

use tape_core::erasure::MAX_GROUP_SIZE;
use tape_core::track::types::TrackFlags;

/// Write pacing knobs; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// In-flight slice uploads per track write. A track has at most
    /// `MAX_GROUP_SIZE` slices, so the default sends all n at once on every
    /// network.
    pub slice_concurrency: usize,
    /// Stream chunks uploading concurrently; also bounds peak memory.
    pub store_depth: usize,
    /// Every track this client writes is registered with these flags.
    pub flags: u64,
    /// Stream writes and the one-call write list the tape first, and finish or
    /// reuse the tracks that hold their content. Off by default: a write appends.
    pub resume: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: MAX_GROUP_SIZE,
            store_depth: 4,
            flags: TrackFlags::NONE,
            resume: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let options = WriteOptions::default();
        assert_eq!(options.slice_concurrency, MAX_GROUP_SIZE);
        assert_eq!(options.store_depth, 4);
        assert_eq!(options.flags, TrackFlags::NONE);
        assert!(!options.resume);
    }
}
