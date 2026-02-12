//! Utility functions for the storage node.
//!
//! Common helper functions used across multiple modules.

use std::path::PathBuf;

/// Default node config file path (~/.tape/node.yaml).
pub fn default_config_path() -> PathBuf {
    dirs::home_dir()
        .map(|h| h.join(".tape").join("node.yaml"))
        .unwrap_or_else(|| PathBuf::from(".tape/node.yaml"))
}

/// Expand ~ and environment variables in a path.
pub fn expand_path(path: &str) -> PathBuf {
    shellexpand::full(path)
        .map(|s| PathBuf::from(s.as_ref()))
        .unwrap_or_else(|_| PathBuf::from(path))
}

/// Get the current Unix timestamp in seconds.
pub fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
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

    #[test]
    fn test_default_config_path() {
        let path = default_config_path();
        assert!(path.to_string_lossy().contains("node.yaml"));
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        // Should be a reasonable Unix timestamp (after year 2020)
        assert!(ts > 1577836800); // Jan 1, 2020
    }
}
