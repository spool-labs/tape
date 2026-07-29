//! Client-level concurrency knobs for the read paths.

/// Read pacing knobs; defaults suit a single client on an ordinary uplink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// In-flight slice downloads per track read.
    ///
    /// A read needs only `k` slices of the group, so anything above `k` is a
    /// hedge: extra racers mean one slow node no longer sets the pace. It is
    /// not free, because a slice that loses the race has already pulled bytes
    /// before it is dropped, so raising this trades ingress for tail latency.
    /// Below `k` it does the opposite and splits one wave into several.
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

    // the default hedges one slice past the default profile's k of 7
    #[test]
    fn defaults() {
        assert_eq!(ReadOptions::default().slice_concurrency, 8);
    }
}
