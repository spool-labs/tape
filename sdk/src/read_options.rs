//! Client-level knobs for the read paths.

use tape_core::share::ShareToken;

/// Peers asked at once before a metadata query widens.
pub const DEFAULT_QUERY_FAN_OUT: usize = 3;

/// Read pacing knobs and the share token for hidden tracks; defaults are the tuned fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// In-flight slice downloads per track read. Anything above `k` is a hedge
    /// against a slow node; below `k` splits one wave into several.
    pub slice_concurrency: usize,

    /// Peers a metadata query asks before widening. Tooling that wants the old
    /// full race can raise this past the committee size.
    pub query_fan_out: usize,

    /// Share token presented to nodes on slice and track-data fetches.
    pub share_token: Option<ShareToken>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: 8,
            query_fan_out: DEFAULT_QUERY_FAN_OUT,
            share_token: None,
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
        assert!(options.share_token.is_none());
    }
}
