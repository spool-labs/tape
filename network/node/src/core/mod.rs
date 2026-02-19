//! Core module — shared utilities for the storage node runtime.
//!
//! This module centralizes code that is used across multiple components:
//! - `backoff`: Shared retry infrastructure with exponential backoff
//! - `cleanup_map`: Time-bounded map with background eviction
//! - `gauge_guard`: RAII metric guards for active-count gauges
//! - `retry`: Task retry policy helpers
//! - `utils`: Common helper functions (path expansion, timestamps)

pub mod guard;
pub mod map;
pub mod retry;
pub mod utils;

pub use retry::{Backoff, BackoffConfig, retry_with_backoff, compute_delay};
pub use map::CleanupMap;
pub use guard::GaugeGuard;
pub use utils::expand_path;
