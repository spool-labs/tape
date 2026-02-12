//! Core module - shared constants and utilities for the storage node.
//!
//! This module centralizes code that is used across multiple features:
//! - `backoff`: Shared retry infrastructure with exponential backoff
//! - `cleanup_map`: Time-bounded map with background eviction
//! - `config`: Node configuration loading and parsing
//! - `constants`: All magic numbers and configuration defaults
//! - `context`: Central shared state for the storage node
//! - `gauge_guard`: RAII metric guards for active-count gauges
//! - `managed_task`: Exclusive background task lifecycle management
//! - `utils`: Common helper functions (path expansion, keypair loading, etc.)

pub mod backoff;
pub mod cleanup_map;
pub mod config;
pub mod constants;
pub mod context;
pub mod gauge_guard;
pub mod managed_task;
pub mod utils;

// Re-export commonly used items at the module level
pub use backoff::{Backoff, BackoffConfig, retry_with_backoff};
pub use cleanup_map::CleanupMap;
pub use config::{default_config_content, ConfigError, NodeConfig, TlsConfig};
pub use constants::*;
pub use context::{ContextError, NodeContext};
pub use gauge_guard::GaugeGuard;
pub use managed_task::ManagedTask;
pub use utils::{
    current_timestamp, default_config_path, expand_path, load_bls_keypair, load_keypair,
    KeypairError,
};
