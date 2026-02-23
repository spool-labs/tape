//! Shared helpers for runtime task implementations.

use std::path::PathBuf;

use tape_core::erasure::slice_for_spool;
use tape_core::types::EpochNumber;
use tape_store::types::TrackInfo;

use crate::state::ChainStateHandle;
use crate::TaskOutcome;

/// Expand `~` and environment variables in a path.
pub fn expand_path(path: &str) -> PathBuf {
    shellexpand::full(path)
        .map(|s| PathBuf::from(s.as_ref()))
        .unwrap_or_else(|_| PathBuf::from(path))
}

/// Validate an untrusted slice before local persistence.
pub fn validate_slice_entry(
    spool: u16,
    track_info: &TrackInfo,
    data: &[u8],
) -> Result<(), String> {
    let slice_index = slice_for_spool(track_info.spool_group, spool)
        .ok_or_else(|| "track not mapped to this spool group".to_string())?;

    if track_info.original_size > 0 && data.is_empty() {
        return Err("empty slice for non-empty track".to_string());
    }

    let expected_max = track_info
        .stripe_size
        .checked_mul(track_info.stripe_count)
        .ok_or_else(|| "invalid stripe dimensions".to_string())?;
    if expected_max > 0 && data.len() as u64 > expected_max {
        return Err("slice exceeds expected decoded size".to_string());
    }

    if !track_info.verify_slice(slice_index, data) {
        return Err("slice does not match commitment".to_string());
    }

    Ok(())
}

/// Load the current chain epoch or return a retryable outcome.
pub fn require_epoch(chain_state: &ChainStateHandle) -> Result<EpochNumber, TaskOutcome> {
    let cs = chain_state.load();
    if cs.has_epoch() {
        Ok(cs.epoch)
    } else {
        Err(TaskOutcome::Retryable("no current epoch".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_path_no_expansion() {
        let path = "/absolute/path/to/file";
        assert_eq!(expand_path(path), PathBuf::from(path));
    }

    #[test]
    fn test_expand_path_with_tilde() {
        let expanded = expand_path("~/test");
        // Should not start with ~ after expansion
        assert!(!expanded.to_string_lossy().starts_with('~'));
    }
}
