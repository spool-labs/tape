//! Client-level concurrency knobs for the read paths.

/// Read pacing knobs; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// In-flight slice downloads per track read. Anything above `k` is a hedge
    /// against a slow node; below `k` splits one wave into several.
    pub slice_concurrency: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: 8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        assert_eq!(ReadOptions::default().slice_concurrency, 8);
    }
}
