//! Client-level knobs for the read paths.

use tape_core::grant::ReadGrant;

/// Peers asked at once before a metadata query widens.
pub const DEFAULT_QUERY_FAN_OUT: usize = 3;

/// Read pacing and restricted-read knobs; defaults are the tuned public fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    /// In-flight slice downloads per track read. Anything above `k` is a hedge
    /// against a slow node; below `k` splits one wave into several.
    pub slice_concurrency: usize,
    /// Peers a metadata query asks before widening. Tooling that wants the old
    /// full race can raise this past the committee size.
    pub query_fan_out: usize,
    /// Grant presented to nodes on slice and track-data fetches.
    pub grant: Option<ReadGrant>,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            slice_concurrency: 8,
            query_fan_out: DEFAULT_QUERY_FAN_OUT,
            grant: None,
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
        assert!(options.grant.is_none());
    }
}
