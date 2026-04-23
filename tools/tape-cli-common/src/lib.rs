//! Shared plumbing for tape CLI tools — cluster URL resolution, config
//! loading, global arg flattening, output format, typed error extraction.

pub mod args;
pub mod cluster;
pub mod config;
pub mod output;
pub mod tape_error;

pub use args::GlobalArgs;
pub use config::{CliConfig, ConfigError};
pub use output::{CliOutput, KeyValue, OkMessage, OutputFormat, emit};
pub use tape_error::{as_tape_error, is_already_initialized_runtime};
