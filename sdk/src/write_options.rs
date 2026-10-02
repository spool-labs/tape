//! Client-level knobs for the write paths.

use tape_core::erasure::GROUP_SIZE;
use tape_core::track::types::TrackVisibility;

/// Write pacing knobs; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// In-flight slice uploads per track write.
    pub slice_concurrency: usize,
    /// Stream chunks uploading concurrently; also bounds peak memory.
    pub store_depth: usize,
    /// Every track this client writes is registered with this visibility.
    pub visibility: TrackVisibility,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: GROUP_SIZE,
            store_depth: 4,
            visibility: TrackVisibility::Public,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let options = WriteOptions::default();
        assert_eq!(options.slice_concurrency, GROUP_SIZE);
        assert_eq!(options.store_depth, 4);
        assert_eq!(options.visibility, TrackVisibility::Public);
    }
}
