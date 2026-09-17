//! Client-level concurrency knobs for the write paths.

use tape_core::erasure::GROUP_SIZE;

/// Write pacing knobs; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// In-flight slice uploads per track write.
    pub slice_concurrency: usize,
    /// Stream chunks uploading concurrently; also bounds peak memory.
    pub store_depth: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: GROUP_SIZE,
            store_depth: 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // defaults match the tuned write path
    #[test]
    fn defaults() {
        let options = WriteOptions::default();
        assert_eq!(options.slice_concurrency, GROUP_SIZE);
        assert_eq!(options.store_depth, 4);
    }
}
