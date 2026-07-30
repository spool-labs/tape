//! Client-level concurrency knobs for the read paths.

/// Peers asked at once before a metadata query widens.
///
/// A committee can hold up to MEMBER_COUNT nodes, and asking every one of them
/// for every lookup spends requests on peers that were never going to answer
/// first. Three covers the common case where the fastest peer replies well
/// inside the hedge delay, and the ladder widens when it does not.
pub const DEFAULT_QUERY_FAN_OUT: usize = 3;

/// Read pacing knobs; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// In-flight slice downloads per track read. Anything above `k` is a hedge
    /// against a slow node; below `k` splits one wave into several.
    pub slice_concurrency: usize,

    /// Peers a metadata query asks before widening. Tooling that wants the old
    /// full race can raise this past the committee size.
    pub query_fan_out: usize,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: 8,
            query_fan_out: DEFAULT_QUERY_FAN_OUT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let options = ReadOptions::default();
        assert_eq!(options.slice_concurrency, 8);
        assert_eq!(options.query_fan_out, DEFAULT_QUERY_FAN_OUT);
    }
}
