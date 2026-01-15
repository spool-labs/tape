//! Core module - shared constants and utilities for the storage node.
//!
//! This module centralizes code that is used across multiple features:
//! - `config`: Node configuration loading and parsing
//! - `constants`: All magic numbers and configuration defaults
//! - `context`: Central shared state for the storage node
//! - `utils`: Common helper functions (path expansion, keypair loading, etc.)

pub mod config;
pub mod constants;
pub mod context;
pub mod utils;

// Re-export commonly used items at the module level
pub use config::{default_config_content, ConfigError, NodeConfig, TlsConfig};
pub use constants::*;
pub use context::{ContextError, NodeContext};
pub use utils::{
    current_timestamp, default_config_path, expand_path, load_bls_keypair, load_keypair,
    KeypairError,
};
