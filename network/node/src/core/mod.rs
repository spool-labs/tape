//! Core module — shared utilities for the storage node runtime.
//!
//! This module centralizes code that is used across multiple components:
//! - `backoff`: Shared retry infrastructure with exponential backoff
//! - `cleanup_map`: Time-bounded map with background eviction
//! - `config`: Node configuration loading and parsing
//! - `context`: Central shared state for the storage node
//! - `gauge_guard`: RAII metric guards for active-count gauges
//! - `managed_task`: Exclusive background task lifecycle management
//! - `utils`: Common helper functions (path expansion, timestamps)

pub mod backoff;
pub mod cleanup_map;
pub mod config;
pub mod context;
pub mod gauge_guard;
pub mod managed_task;
pub mod utils;

pub use backoff::{Backoff, BackoffConfig, retry_with_backoff};
pub use cleanup_map::CleanupMap;
pub use config::{
    ConfigError, IngressLimitsConfig, NodeApiConfig, NodeConfig, TlsConfig,
    TransportSecurityConfig, default_config_content,
};
pub use context::{ContextError, NodeContext};
pub use gauge_guard::GaugeGuard;
pub use managed_task::ManagedTask;
pub use utils::{current_timestamp, default_config_path, expand_path};
